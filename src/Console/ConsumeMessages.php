<?php 

namespace Core\Messaging\Console;

use DateTimeImmutable;
use Illuminate\Console\Command;
use Core\Messaging\Contracts\Consumer;
use Core\EventSourcing\DomainEvent;
use Core\EventSourcing\Contracts\EventDispatcher;
use Exception;
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
        $timestamp = (new DateTimeImmutable('now', env('APP_TIMEZONE', 'UTC')))->format('Y-m-d H:i:s.u');
        $time_limit = intval($this->option('timeout'));

        $this->info("Starting consumer process $process_id at $timestamp");

        try {
            $this->consumer->consume(function (AMQPMessage $message) {
                $event = $this->mapMessageToEvent($message);
                $this->dispatcher->dispatch($event);
            });
        } catch (AMQPRuntimeException $e) {
            $this->error('AMQP exception: ' . $e->getMessage());
            echo $e->getMessage() . PHP_EOL;
            $this->consumer->close();
        } catch (\RuntimeException $e) {
            $this->error('Runtime exception: ' . $e->getMessage());
            $this->consumer->close();
        } catch (\Exception $e) {
            $this->error('Error exception: ' . $e->getMessage());
            $this->consumer->close();
        }

        if ($time_limit > 0 && !extension_loaded('pcntl')) {
            $this->info("Time limit cannot set. Extension pcntl not installed.");
        }
        if ($time_limit > 0 && extension_loaded('pcntl')) {
            $this->info("Time limit of {$time_limit} was set to process $process_id.");
            pcntl_signal(SIGALRM, function () use ($time_limit, $process_id) {
                $timestamp = (new DateTimeImmutable('now', env('APP_TIMEZONE', 'UTC')))->format('Y-m-d H:i:s.u');
                $this->info("Consumer process $process_id stopped due to time limit of {$time_limit}s exceeded at $timestamp");
                if (extension_loaded('posix')) posix_kill($process_id, SIGKILL);
                exit(1);
            });
            pcntl_alarm($time_limit);
        }
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
}