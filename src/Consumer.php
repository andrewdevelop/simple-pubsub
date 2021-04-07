<?php 

namespace Core\Messaging;

use Core\Messaging\Contracts\Consumer as Contract;
use ErrorException;
use Exception;

class Consumer extends InteractsWithQueue implements Contract
{
    /**
     * @param callable $callback
     * @throws ErrorException
     * @throws Exception
     */
	public function consume(callable $callback)
	{
		$this->connect();

        $this->makeConsumer($callback);
        while ($this->channel->is_consuming()) {
		    $this->channel->wait();
		}
	}

    /**
     * @param $callback
     * @return mixed|string
     */
    protected function makeConsumer($callback)
    {
        $queue = 'evt_' . $this->service_id;
        $consumer_tag = $this->service_id;
        $no_local = false;
        $no_ack = true;
        $exclusive = false;
        $nowait = false;
        $ticket = null;
        $arguments = [];
        return $this->channel->basic_consume($queue, $consumer_tag, $no_local, $no_ack, $exclusive, $nowait, $callback, $ticket, $arguments);
    }

}