<?php

declare(strict_types=1);

namespace Core\Messaging;

use Core\Messaging\Contracts\Consumer as Contract;
use DateTimeImmutable;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

class Consumer implements Contract
{
    /**
     * Connection timeout in seconds. Needs to be at least 2x heartbeat.
     * @var int
     */
    protected int $connection_timeout;

    /**
     * Heartbeat timeout in seconds.
     * @var int
     */
    protected int $heartbeat_timeout;

    protected int $restart_timeout = 10;

    protected AMQPStreamConnection|null $connection = null;

    protected AMQPChannel|null $channel = null;

    protected InternalConfig $config;

    protected string $service_id;

    protected int $timeout;

    protected bool $strict;

    private bool $force_shutdown = false;

    private bool $auto_ack = true;

    private bool $with_dlq = false;

    protected LoggerInterface|null $logger = null;

    /**
     * @param InternalConfig $config
     * @param int|null $timeout
     * @param bool $strict
     * @param bool $auto_ack
     */
    public function __construct(InternalConfig $config, int|null $timeout = null, bool $strict = false, bool $auto_ack = true)
    {
        $this->config = $config;
        $this->service_id = $config->service_id;
        $this->with_dlq = $config->with_failover;
        $this->connection_timeout = $config->connection_timeout;
        $this->heartbeat_timeout = $config->heartbeat_timeout;
        $this->timeout = $timeout ?? 3600;
        $this->strict = $strict;
        $this->auto_ack = $auto_ack;

        $this->registerSignalHandlers();
    }

    public function withLogger(LoggerInterface $logger): static
    {
        $this->logger = $logger;
        return $this;
    }

    protected function registerSignalHandlers(): void
    {
        # define('AMQP_DEBUG', true);
        define('AMQP_WITHOUT_SIGNALS', false);
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, [$this, 'handleSignals']); // 15 : supervisor default stop
        pcntl_signal(SIGINT, [$this, 'handleSignals']);  // 2  : ctrl+c
        pcntl_signal(SIGQUIT, [$this, 'handleSignals']); // 3  : kill -s QUIT
        pcntl_signal(SIGHUP, [$this, 'handleSignals']);  // 1  : kill -s HUP

        pcntl_signal(SIGALRM, function () {
            $this->log("consumer stopping due to time limit of {$this->timeout}s.", 'info');
            $this->force_shutdown = true;
        });
        pcntl_alarm($this->timeout);
    }

    /**
     * @param int $signal
     * @return void
     */
    public function handleSignals(int $signal): void
    {
        $this->log("signal $signal received");
        switch ($signal) {
            case SIGTERM:
            case SIGINT:
            case SIGQUIT:
                $this->shutdown();
                pcntl_signal($signal, SIG_DFL); // restore handler
                $this->kill($signal, true); // kill self with signal, see https://www.cons.org/cracauer/sigint.html
                break;
            case SIGHUP:
                $this->shutdown();
                pcntl_signal($signal, SIG_DFL); // restore handler
                $this->kill($signal, true);
                break;
            default:
                $this->log("unknown signal $signal received", 'error');
        }
    }

    public function kill(int $signal = SIGKILL, bool $immediately = false, ?int $code = 1): void
    {
        if (!$immediately) {
            $this->log("waiting {$this->restart_timeout}s before restart or close");
            sleep($this->restart_timeout);
        }
        if (extension_loaded('posix')) {
            posix_kill((int)getmypid(), $signal);
        }
        exit($code);
    }

    protected function connect(): void
    {
        try {
            $this->connection = ConnectionFactory::create($this->config, 'c');
            $this->log("connected", 'info');
        } catch (\Throwable $e) {
            $this->log("connection error: {$e->getMessage()}", 'error');
            $this->kill(SIGKILL);
            return;
        }

        if (!$this->connection) {
            $this->log("connection error: connection is null", 'error');
            $this->kill(SIGKILL);
            return;
        }

        $this->channel = $this->connection->channel();
        $channel_id = (string)$this->channel->getChannelId();
        $this->log("connected to channel $channel_id", 'info');
    }

    public function consume(callable $callback)
    {
        $handler = function (AMQPMessage $message) use ($callback) {
            // Block any incoming exit signals to make sure the current message can be processed.
            pcntl_sigprocmask(SIG_BLOCK, [SIGTERM, SIGINT]);

            $this->log("message received", 'debug');

            if ($message->getBody() === 'quit') {
                $this->log('quit message received, stopping consumer', 'warning');
                $channel = $message->getChannel();
                $channel?->basic_cancel((string)$message->getConsumerTag());
                $this->force_shutdown = true;
            } else {
                // Ignore messages generated by our own service to prevent loopback
                if ($message->has('app_id') && $message->get('app_id') === $this->service_id) {
                    $this->log("ignoring loopback message from self ({$this->service_id})", 'debug');
                    $this->ackMessage($message);
                    return;
                }

                try {
                    $callback($message);
                    $this->ackMessage($message);
                } catch (\Throwable $e) {
                    if ($this->strict) {
                        $this->log("unprocessable entity: {$e->getMessage()}", 'error');
                        $this->disconnect();
                        $this->kill(1);
                    } else {
                        if ($this->with_dlq) {
                            $queueName = $this->config->queue_prefix . '_' . $this->service_id;
                            $deathCount = $this->getDeathCount($message, $queueName);
                            if ($deathCount >= $this->config->max_retries) {
                                $this->log("message failed {$deathCount} times, sending to DLQ. Error: {$e->getMessage()}", 'error');
                                $channel = $message->getChannel();
                                $channel?->basic_publish($message, '', $queueName . '_dlq');
                                $this->ackMessage($message);
                            } else {
                                $this->log("message failed, nacking to retry (attempt " . ($deathCount + 1) . "). Error: {$e->getMessage()}", 'warning');
                                $this->nackMessage($message);
                            }
                        } else {
                            $this->log('unprocessable entity, ignoring message, ' . $e->getMessage(), 'error');
                            $this->ackMessage($message);
                        }
                    }
                }
            }

            // Unblock any incoming exit signals, message has been processed at this point.
            pcntl_sigprocmask(SIG_UNBLOCK, [SIGTERM, SIGINT]);
            // Dispatch the exit signals that might've come in.
            pcntl_signal_dispatch();
        };

        $this->log('starting consumer' . ($this->timeout ? " (timeout {$this->timeout}s)" : ''), 'info');

        $this->connect();

        if (!$this->channel) {
            $this->log("channel is not initialized", 'error');
            $this->kill(SIGKILL);
            return;
        }

        $this->bootConsumer($this->channel, $this->service_id, $handler);

        try {
            $this->log('waiting for messages', 'info');
            while ($this->channel && $this->channel->is_open() && !$this->force_shutdown) {
                $this->channel->wait();
                // Dispatch incoming exit signals.
                pcntl_signal_dispatch();
            }

        } catch (AMQPRuntimeException $e) {
            $this->log("AMQP runtime exception: {$e->getMessage()}", 'error');
            $this->disconnect();
            $this->kill(SIGKILL);
        } catch (\RuntimeException $e) {
            $this->log("runtime exception: {$e->getMessage()}", 'error');
            $this->disconnect();
            $this->kill(SIGKILL);
        } catch (\Throwable $e) {
            $this->log("exception: {$e->getMessage()}", 'error');
            $this->disconnect();
            $this->kill(1);
        }

        $this->disconnect();
        $this->log('consumer stopped', 'info');
        exit(0);
    }

    private function shutdown(): void
    {
        $this->force_shutdown = true;
    }

    protected function bootConsumer(AMQPChannel $channel, string $tag, callable $callback): void
    {
        $queue = Topology::declareQueue(
            $channel,
            $tag,
            $this->with_dlq,
            $this->config->exchange_name,
            $this->config->queue_prefix,
            $this->config->retry_delay_sec
        );

        $channel->basic_consume(
            queue: $queue,
            consumer_tag: $tag,
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            callback: $callback,
            ticket: null,
            arguments: []
        );
    }

    public function close()
    {
        $this->disconnect();
    }

    public function disconnect(): void
    {
        try {
            // Channel might already be closed. Ignoring exceptions.
            if ($this->channel && $this->channel->is_open()) $this->channel->close();
        } catch (\Throwable $e) {
            $this->log("cannot close channel: {$e->getMessage()}", 'error');
        }

        try {
            // Connection might already be closed. Ignoring exceptions.
            if ($this->connection && $this->connection->isConnected()) $this->connection->close();
        } catch (\Throwable $e) {
            $this->log("cannot close connection: {$e->getMessage()}", 'error');
        }
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
            'type' => 'consumer',
            'service_id' => $this->config->service_id,
            'hostname' => gethostname(),
        ];
        $this->logger->log($level, $message, $context);
    }

    /**
     * @param AMQPMessage $message
     * @param string $queueName
     * @return int
     */
    protected function getDeathCount(AMQPMessage $message, string $queueName): int
    {
        if (!$message->has('application_headers')) {
            return 0;
        }

        /** @var \PhpAmqpLib\Wire\AMQPTable $headersTable */
        $headersTable = $message->get('application_headers');
        $headers = $headersTable->getNativeData();

        if (!isset($headers['x-death']) || !is_array($headers['x-death'])) {
            return 0;
        }

        foreach ($headers['x-death'] as $death) {
            if (is_array($death) &&
                isset($death['queue'], $death['reason'], $death['count']) &&
                $death['queue'] === $queueName &&
                $death['reason'] === 'rejected'
            ) {
                return is_numeric($death['count']) ? (int)$death['count'] : 0;
            }
        }

        return 0;
    }

    protected function ackMessage(AMQPMessage $message): void
    {
        if (!$this->auto_ack) return;

        $channel = $message->getChannel();
        if ($channel && $channel->is_open()) {
            try {
                $message->ack();
            } catch (\Throwable $e) {
                $this->log("cannot ack message: {$e->getMessage()}", 'error');
            }
        } else {
            $this->log("cannot ack message: channel is closed", 'error');
        }
    }

    protected function nackMessage(AMQPMessage $message): void
    {
        if (!$this->auto_ack) return;

        $channel = $message->getChannel();
        if ($channel && $channel->is_open()) {
            try {
                // Reject without requeue triggers DLX
                $message->nack(false);
            } catch (\Throwable $e) {
                $this->log("cannot nack message: {$e->getMessage()}", 'error');
            }
        } else {
            $this->log("cannot nack message: channel is closed", 'error');
        }
    }
}
