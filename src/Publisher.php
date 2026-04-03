<?php

declare(strict_types=1);

namespace Core\Messaging;

use Core\Messaging\Contracts\Publisher as Contract;
use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;

class Publisher implements Contract
{
    protected AMQPStreamConnection|null $connection;

    protected AMQPChannel|null $channel;

    protected LoggerInterface|null $logger = null;

    /**
     * @param InternalConfig $config
     */
    public function __construct(
        protected readonly InternalConfig $config,
    )
    {
    }

    public function withLogger(LoggerInterface $logger): static
    {
        $this->logger = $logger;
        return $this;
    }

    /**
     * @throws Exception
     */
    protected function connect(): void
    {
        $this->connection = ConnectionFactory::create($this->config, 'p');

        $this->channel = $this->connection->channel();

        // Auto-declare the exchange as fanout to ensure it exists
        Topology::declareExchange($this->channel, $this->config->exchange_name);
    }

    /**
     * @param string $str_message
     * @return AMQPMessage
     * @throws Exception
     */
    public function publish(string $str_message): AMQPMessage
    {
        $this->connect();
        $message = $this->makeMessage($str_message);
        if ($this->channel !== null) {
            $this->publishMessage($this->channel, $message);
        }
        $this->disconnect();
        return $message;
    }

    /**
     * @param AMQPChannel $channel
     * @param AMQPMessage $msg
     * @return void
     */
    protected function publishMessage(AMQPChannel $channel, AMQPMessage $msg): void
    {
        $channel->basic_publish(
            msg: $msg,
            exchange: $this->config->exchange_name,
            routing_key: '', // Fanout exchange ignores routing key
            mandatory: false,
            immediate: false,
            ticket: null
        );
        $this->log("publishing message: {$msg->getBody()}", 'debug');
    }

    /**
     * @param string $message
     * @return AMQPMessage
     * @throws Exception
     */
    protected function makeMessage(string $message): AMQPMessage
    {
        $decoded = json_decode($message, true);
        $uuid = (is_array($decoded) && isset($decoded['uuid']) && is_scalar($decoded['uuid'])) ? (string)$decoded['uuid'] : (string)Uuid::uuid4();
        $correlation_id = $uuid;
        $content_type = 'application/json';
        $app_id = $this->config->service_id;

        return new AMQPMessage($message, compact('correlation_id', 'content_type', 'app_id'));
    }

    protected function disconnect(): void
    {
        try {
            if ($this->channel && $this->channel->is_open()) $this->channel->close();
        } catch (\Throwable $e) {
            // Channel might already be closed. Ignoring exceptions.
            $this->log('Failed to close channel: ' . $e->getMessage(), 'warning');
        }

        try {
            if ($this->connection && $this->connection->isConnected()) $this->connection->close();
        } catch (\Throwable $e) {
            // Connection might already be closed. Ignoring exceptions.
            $this->log('Failed to close connection: ' . $e->getMessage(), 'warning');
        }
    }

    /**
     * @return mixed
     */
    public function close()
    {
        $this->disconnect();
    }

    /**
     * @param string $message
     * @param string $level
     * @return void
     */
    protected function log(string $message, string $level = 'info'): void
    {
        if ($this->logger === null) return;

        $context = [
            'type' => 'publisher',
            'service_id' => $this->config->service_id,
            'hostname' => gethostname(),
        ];
        $this->logger->log($level, $message, $context);
    }
}