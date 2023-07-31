<?php

namespace Core\Messaging\Console;

use Core\EventSourcing\Contracts\EventDispatcher;
use Core\EventSourcing\DomainEvent;
use Core\Messaging\Contracts\Consumer;
use DateTimeImmutable;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Contracts\Config\Repository;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;

class ConsumeMessages extends Command
{
    /**
     * The name and signature of the console command.
     * @var string
     */
    protected $signature = 'mq:server
                            {--timeout=3600 : The number of seconds a child process can run}';

    /**
     * The console command description.
     * @var string
     */
    protected $description = 'Listen to a queue.';

    protected Consumer $consumer;

    protected EventDispatcher $dispatcher;

    protected Repository $config;

    /**
     * Create a new command instance.
     * @param Consumer $consumer
     * @param EventDispatcher $dispatcher
     * @param Repository $config
     */
    public function __construct(Consumer $consumer, EventDispatcher $dispatcher, Repository $config)
    {
        parent::__construct();
        $this->consumer = $consumer;
        $this->dispatcher = $dispatcher;
        $this->config = $config;
    }

    /**
     * Execute the console command.
     * @return mixed
     * @throws Exception
     */
    public function handle()
    {
        $process_id = getmypid();

        $this->info('starting consumer');

        $this->handleSignals($process_id);

        try {
            $this->consumer->consume(function (AMQPMessage $message) {
                $event = $this->mapMessageToEvent($message);
                $this->dispatcher->dispatch($event);
            });

            if (!extension_loaded('pcntl')) $this->shutdown();

        } catch (AMQPRuntimeException $e) {
            $this->error('AMQP exception: ' . $e->getMessage());
            $this->shutdown();
            return 1;
        } catch (\RuntimeException $e) {
            $this->error('runtime exception: ' . $e->getMessage());
            $this->shutdown();
            return 1;
        } catch (\Exception $e) {
            $this->error('error exception: ' . $e->getMessage());
            $this->shutdown();
            return 1;
        }
        $this->info("consumer stopped.");

        return 0; // exit gracefully
    }

    /**
     * @param AMQPMessage $message
     * @return DomainEvent
     */
    protected function mapMessageToEvent(AMQPMessage $message): DomainEvent
    {
        $payload = json_decode($message->body, true);
        return new DomainEvent($payload);
    }

    public function shutdown(): void
    {
        $this->info('gracefully stopping consumer process.');
        try {
            $this->consumer->close();
        } catch (Exception $e) {
            $this->warn('cannot close consumer: ' . $e->getMessage());
            exit(1); // exit with error
        }
    }

    protected function timestamp(): string
    {
        $tz = new \DateTimeZone('UTC');
        $now = new DateTimeImmutable('now', $tz);
        return $now->format('Y-m-d H:i:s.u');
    }

    /**
     * @param int $process_id
     */
    protected function handleSignals(int $process_id): void
    {
        $time_limit = intval($this->option('timeout'));

        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, [$this, 'shutdown']);
            pcntl_signal(SIGTERM, [$this, 'shutdown']);
            pcntl_signal(SIGINT, [$this, 'shutdown']);
        }

        if ($time_limit > 0 && !extension_loaded('pcntl')) {
            $this->warn("time limit cannot set, pcntl extension not installed.");
        }

        if ($time_limit > 0 && extension_loaded('pcntl')) {
            $this->info("execution limit of {$time_limit}s was set.");
            pcntl_signal(SIGALRM, function () use ($time_limit, $process_id) {
                $this->info("consumer stopped due to time limit of {$time_limit}s.");
                $this->shutdown();
                if (extension_loaded('posix')) {
                    posix_kill($process_id, SIGKILL);
                }
                exit(0); // exit gracefully
            });
            pcntl_alarm($time_limit);
        }
    }


    protected function appendMessageMeta($string): string
    {
        $host = gethostname();
        $process_id = getmypid();
        $service_id = $this->config->get('mq.service_id');
        return sprintf(
            '%s - %s@%s - pid%s | %s',
            $this->timestamp(), $service_id, $host, $process_id, $string
        );
    }

    /**
     * Write a string as information output.
     *
     * @param string $string
     * @param int|string|null $verbosity
     * @return void
     */
    public function info($string, $verbosity = null)
    {
        parent::info($this->appendMessageMeta($string), $verbosity);
    }

    /**
     * Write a string as warning output.
     *
     * @param string $string
     * @param int|string|null $verbosity
     * @return void
     */
    public function warn($string, $verbosity = null)
    {
        parent::warn($this->appendMessageMeta($string), $verbosity);
    }


    /**
     * Write a string as error output.
     *
     * @param string $string
     * @param int|string|null $verbosity
     * @return void
     */
    public function error($string, $verbosity = null)
    {
        parent::error($this->appendMessageMeta($string), $verbosity);
    }
}