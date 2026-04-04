<?php

namespace Core\Messaging;

use Core\Contracts\Event;
use Core\EventSourcing\Reactor;
use Core\Messaging\Contracts\Publisher;

/**
 * Publishes selected events via message bus.
 * @package Core\Messaging
 */
class EventPublisher extends Reactor
{
    public function __construct(
        protected readonly Publisher $publisher,
    )
    {
    }

    /**
     * @param string $event_name
     * @param Event $event
     * @return mixed
     */
    public function handle($event_name, Event $event)
    {
        $payload = method_exists($event, 'toArray') ? $event->toArray() : (array) $event;
        $message = json_encode($payload);
        return $this->publisher->publish(is_string($message) ? $message : '');
    }
}