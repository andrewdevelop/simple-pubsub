<?php

namespace Core\Messaging\Console;

use Core\EventSourcing\Contracts\EventDispatcher;
use Core\EventSourcing\DomainEvent;
use Core\Messaging\Consumer;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Contracts\Config\Repository;
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

    protected EventDispatcher $dispatcher;

    protected Repository $config;

    /**
     * Create a new command instance.
     * @param EventDispatcher $dispatcher
     * @param Repository $config
     */
    public function __construct(EventDispatcher $dispatcher, Repository $config)
    {
        parent::__construct();
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
        $consumer = new Consumer($this->config->get('mq'), $this->option('timeout'));
        $consumer->consume(
            callback: function (AMQPMessage $message) {
                $event = $this->mapMessageToEvent($message);
                $this->dispatcher->dispatch($event);
            }
        );
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
}