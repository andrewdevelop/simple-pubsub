<?php 

namespace Core\Messaging;

use Core\Messaging\Contracts\Publisher as Contract;
use Exception;
use PhpAmqpLib\Message\AMQPMessage;
use Ramsey\Uuid\Uuid;

class Publisher extends InteractsWithQueue implements Contract
{
    /**
     * @param string $json
     * @return AMQPMessage
     * @throws Exception
     */
	public function publish(string $json)
	{
		$this->connect();
        $message = $this->makeMessage($json);
        $this->publishMessage($message);
		$this->close();

		return $message;
	}

    /**
     * @param AMQPMessage $msg
     * @param void
     */
	protected function publishMessage(AMQPMessage $msg)
    {
        $exchange = 'events';
        $routing_key = $this->service_id;
        $mandatory = false;
        $immediate = false;
        $ticket = null;
        $this->channel->basic_publish($msg, $exchange, $routing_key, $mandatory, $immediate, $ticket);
    }

    /**
     * @param $message
     * @return AMQPMessage
     * @throws Exception
     */
    protected function makeMessage($message): AMQPMessage
    {
        $correlation_id = (string) Uuid::uuid4();
        $content_type = 'application/json';
        return new AMQPMessage($message, compact('correlation_id', 'content_type'));
    }
}