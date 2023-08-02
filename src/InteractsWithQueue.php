<?php

namespace Core\Messaging;

use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * @deprecated Use Consumer or Publisher instead.
 */
abstract class InteractsWithQueue
{
    protected AMQPStreamConnection $connection;

    protected AMQPChannel $channel;

    protected string $host;

    protected string $port;

    protected string $login;

    protected string $password;

    protected string $vhost;

    protected string $service_id;

    protected string $exchange = '';

    protected string $queue = '';


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
    }

    /**
     * We create a connection to the server.
     * @return $this
     */
    public function connect(): static
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
    public function close(): void
    {
        $error = null;
        try {
            if (isset($this->channel)) $this->channel->close();
        } catch (\ErrorException $e) {
            // Connection might already be closed. Ignoring exceptions.
            $error = $e;
        }
        try {
            if (isset($this->connection)) $this->connection->close();
        } catch (\ErrorException $e) {
            // Connection might already be closed. Ignoring exceptions.
            $error = $e;
        }
        if ($error) throw $error;
    }
}