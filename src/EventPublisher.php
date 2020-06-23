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
	protected $publisher;

	public function __construct(Publisher $publisher)
	{
		$this->publisher = $publisher;
	}

	public function handle($event_name, Event $event)
	{
		$message = (string) $event;
		return $this->publisher->publish($message);
	}
}