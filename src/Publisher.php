<?php

namespace Core\Messaging;

use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Ramsey\Uuid\Uuid;
use Core\Messaging\Contracts\Publisher as Contract;

class Publisher implements Contract
{
    /**
     * Connection timeout in seconds. Needs to be at least 2x heartbeat.
     * @var int
     */
    protected int $connection_timeout = 60;

    /**
     * Heartbeat timeout in seconds.
     * @var int
     */
    protected int $heartbeat_timeout = 30;

    protected AMQPStreamConnection|null $connection;

    protected AMQPChannel|null $channel;

    protected array $config;

    protected string $service_id;

    public function __construct(array $config)
    {
        $this->config = $config;
        $this->service_id = $config['service_id'];
    }

    protected function connect(): void
    {
        AbstractConnection::$LIBRARY_PROPERTIES['connection_name'] = [
            'S', ''.$this->service_id . '@' . gethostname() . '-p-' . date('Ymd-His')
        ];
        $this->connection = new AMQPStreamConnection(
            host: $this->config['host'] ?? "localhost",
            port: $this->config['port'] ?? 5672,
            user: $this->config['login'] ?? "guest",
            password: $this->config['password'] ?? "guest",
            vhost: $this->config['vhost'] ?? "/",
            insist: false,
            login_method: 'AMQPLAIN',
            login_response: null,
            locale: 'en_US',
            connection_timeout: $this->connection_timeout,
            read_write_timeout: $this->connection_timeout,
            context: null,
            keepalive: true,
            heartbeat: $this->heartbeat_timeout,
        );

        $this->channel = $this->connection->channel();
    }

    /**
     * @param string $json
     * @return AMQPMessage
     * @throws Exception
     */
    public function publish(string $json)
    {
        $this->connect();
        $message = $this->makeMessage($json);
        $this->publishMessage($this->channel, $message);
        $this->disconnect();
        return $message;
    }

    /**
     * @param AMQPMessage $msg
     * @param void
     */
    protected function publishMessage(AMQPChannel $channel, AMQPMessage $msg)
    {
        $channel->basic_publish(
            msg: $msg,
            exchange: 'events',
            routing_key: $this->service_id,
            mandatory: false,
            immediate: false,
            ticket: null
        );
    }

    /**
     * @param $message
     * @return AMQPMessage
     * @throws Exception
     */
    protected function makeMessage($message): AMQPMessage
    {
        $correlation_id = $message['uuid'] ?? (string)Uuid::uuid4();
        $content_type = 'application/json';
        return new AMQPMessage($message, compact('correlation_id', 'content_type'));
    }

    protected function disconnect(): void
    {
        try {
            if ($this->channel && $this->channel->is_open()) $this->channel->close();
        } catch (\Throwable $e) {
            // Channel might already be closed. Ignoring exceptions.
        }

        try {
            if ($this->connection && $this->connection->isConnected()) $this->connection->close();
        } catch (\Throwable $e) {
            // Connection might already be closed. Ignoring exceptions.
        }
    }

    /**
     * @return mixed
     */
    public function close()
    {
        $this->disconnect();
    }
}