<?php

namespace Core\Messaging\Console;

use Core\EventSourcing\Contracts\EventDispatcher;
use Core\EventSourcing\DomainEvent;
use Core\Messaging\Contracts\Consumer;
use DateTimeImmutable;
use Exception;
use Illuminate\Console\Command;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;

class ConsumeMessages extends Command
{
    /**
     * The name and signature of the console command.
     * @var string
     */
    protected $signature = 'mq:server
                            {--timeout=0 : The number of seconds a child process can run}';

    /**
     * The console command description.
     * @var string
     */
    protected $description = 'Listen to a queue.';

    /**
     * Consumer instance.
     * @var Consumer
     */
    protected $consumer;

    /**
     * Event dispatcher instance.
     * @var EventDispatcher
     */
    protected $dispatcher;

    /**
     * Create a new command instance.
     * @param Consumer $consumer
     * @param EventDispatcher $dispatcher
     */
    public function __construct(Consumer $consumer, EventDispatcher $dispatcher)
    {
        parent::__construct();
        $this->consumer = $consumer;
        $this->dispatcher = $dispatcher;
    }

    /**
     * Execute the console command.
     * @return mixed
     * @throws Exception
     */
    public function handle()
    {
        $process_id = getmypid();

        $this->info("Starting consumer process {$process_id} at {$this->timestamp()}");

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
        } catch (\RuntimeException $e) {
            $this->error('Runtime exception: ' . $e->getMessage());
            $this->shutdown();
        } catch (\Exception $e) {
            $this->error('Error exception: ' . $e->getMessage());
            $this->shutdown();
        }
        $this->info("Consumer stopped at {$this->timestamp()}.");
    }

    /**
     * @param AMQPMessage $message
     * @return DomainEvent
     */
    protected function mapMessageToEvent(AMQPMessage $message)
    {
        $payload = json_decode($message->body, true);
        return new DomainEvent($payload);
    }

    public function shutdown()
    {
        $this->info('Gracefully stopping consumer process.');
        $this->consumer->close();
    }

    protected function timestamp()
    {
        $tz = new \DateTimeZone(env('APP_TIMEZONE', 'UTC'));
        $now = new DateTimeImmutable('now', $tz);
        return $now->format('Y-m-d H:i:s.u');
    }

    /**
     * @param int $process_id
     */
    protected function handleSignals($process_id): void
    {
        $time_limit = intval($this->option('timeout'));

        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGINT, [$this, 'shutdown']);
            pcntl_signal(SIGTERM, [$this, 'shutdown']);
        }

        if ($time_limit > 0 && !extension_loaded('pcntl')) {
            $this->warn("Time limit cannot set. Extension pcntl not installed.");
        }

        if ($time_limit > 0 && extension_loaded('pcntl')) {
            $this->info("Time limit of {$time_limit}s was set to process $process_id.");
            pcntl_signal(SIGALRM, function () use ($time_limit, $process_id) {
                $this->info("Consumer process $process_id stopped due to time limit of {$time_limit}s exceeded at {$this->timestamp()}");
                $this->shutdown();
                if (extension_loaded('posix')) posix_kill($process_id, SIGKILL);
                exit(1);
            });
            pcntl_alarm($time_limit);
        }
    }
}