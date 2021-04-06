<?php

namespace Core\Messaging;

use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

abstract class InteractsWithQueue
{
    /**
     * Connection instance.
     * @var AMQPStreamConnection
     */
    protected $connection;

    /**
     * Channel instance.
     * @var AMQPChannel
     */
    protected $channel;

    /** @var string */
    protected $host;

    /** @var string */
    protected $port;

    /** @var string */
    protected $login;

    /** @var string */
    protected $password;

    /** @var string */
    protected $vhost;

    /** @var string */
    protected $service_id;

    /** @var string */
    protected $exchange = '';

    /** @var string */
    protected $queue = '';


    /**
     * Create new instance.
     * @param string $host
     * @param string $port
     * @param string $login
     * @param string $password
     * @param string $vhost
     * @param string $service_id
     */
    public function __construct(string $host, string $port, string $login, string $password, string $vhost, string $service_id)
    {
        $this->host = $host;
        $this->port = $port;
        $this->login = $login;
        $this->password = $password;
        $this->vhost = $vhost;
        $this->service_id = $service_id;

        $this->connect();
    }

    /**
     * We create a connection to the server.
     * @return $this
     */
    public function connect()
    {
        $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->login, $this->password, $this->vhost);
        $this->channel = $this->connection->channel();
        return $this;
    }

    /**
     * We close the channel and the connection;
     * @return void
     * @throws Exception
     */
    protected function close()
    {
        $this->channel->close();
        try {
            if ($this->connection !== null) $this->connection->close();
        } catch (\ErrorException $e) {
            // Connection might already be closed. Ignoring exceptions.
        }
    }
}