<?php

namespace Core\Messaging\Console;

use Core\EventSourcing\Contracts\EventDispatcher;
use Core\EventSourcing\DomainEvent;
use Core\Messaging\Contracts\Consumer;
use Exception;
use Illuminate\Console\Command;
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

    /**
     * Create a new command instance.
     */
    public function __construct(
        protected readonly EventDispatcher $dispatcher,
        protected readonly Consumer        $consumer,
    )
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     * @return void
     * @throws Exception
     */
    public function handle()
    {
        $this->consumer->consume(function (AMQPMessage $message) {
            $this->dispatcher->dispatch($this->mapMessageToEvent($message));
        });
    }

    /**
     * @param AMQPMessage $message
     * @return DomainEvent
     */
    protected function mapMessageToEvent(AMQPMessage $message): DomainEvent
    {
        $payload = json_decode($message->getBody(), true);
        if (!is_array($payload)) {
            throw new \InvalidArgumentException('Invalid payload received, expected JSON object or array.');
        }
        return new DomainEvent($payload);
    }
}