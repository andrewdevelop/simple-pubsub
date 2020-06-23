<?php 

namespace Core\Messaging\Console;

use DateTimeImmutable;
use Illuminate\Console\Command;
use Core\Messaging\Contracts\Consumer;
use Core\EventSourcing\DomainEvent;
use Core\EventSourcing\Contracts\EventDispatcher;
use Exception;
use PhpAmqpLib\Message\AMQPMessage;

class ConsumeMessages extends Command
{
    /**
     * The name and signature of the console command.
     * @var string
     */
    protected $signature = 'mq:server';

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
        $this->info("Starting consumer process $process_id at $timestamp");
        $this->consumer->consume(function(AMQPMessage $message) {
            $event = $this->mapMessageToEvent($message);
            $this->dispatcher->dispatch($event);
        });
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