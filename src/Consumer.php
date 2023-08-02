<?php

namespace Core\Messaging;

use Core\Messaging\Contracts\Consumer as Contract;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;

class Consumer implements Contract
{
    /**
     * Connection timeout in seconds. Needs to be at least 2x heartbeat.
     * @var int
     */
    protected int $connection_timeout = 120;

    /**
     * Heartbeat timeout in seconds.
     * @var int
     */
    protected int $heartbeat_timeout = 60;

    protected int $restart_timeout = 30;

    protected AMQPStreamConnection|null $connection;

    protected AMQPChannel|null $channel;

    protected array $config;

    protected string $service_id;

    protected int $timeout;

    protected bool $strict;

    private bool $force_shutdown = false;

    public function __construct(array $config, int|null $timeout = null, bool $strict = false)
    {
        $this->config = $config;
        $this->service_id = $config['service_id'];
        $this->timeout = $timeout ?? 3600;
        $this->strict = $strict;

        $this->registerSignalHandlers();
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

    public function handleSignals(int $signal)
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
            posix_kill(getmypid(), $signal);
        }
        exit($code);
    }

    protected function connect(): void
    {
        try {
            AbstractConnection::$LIBRARY_PROPERTIES['connection_name'] = [
                'S', $this->service_id . '@' . gethostname() . '-c-' . date('Ymd-His')
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
            $this->log("connected", 'info');
        } catch (\Throwable $e) {
            $this->log("connection error: {$e->getMessage()}", 'error');
            $this->kill(SIGKILL);
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

            if ($message->body === 'quit') {
                $this->log('quit message received, stopping consumer', 'warning');
                $message->getChannel()->basic_cancel($message->getConsumerTag());
                $this->force_shutdown = true;
            } else {
                try {
                    $callback($message);
                } catch (\Throwable $e) {
                    if ($this->strict) {
                        $this->log("unprocessable entity: {$e->getMessage()}", 'error');
                        $this->disconnect();
                        $this->kill(1);
                    } else {
                        $this->log('unprocessable entity, ignoring message, ' . $e->getMessage(), 'error');
                    }
                }
            }
            // Acknowledge the message!
            $message->ack();

            // Unblock any incoming exit signals, message has been processed at this point.
            pcntl_sigprocmask(SIG_UNBLOCK, [SIGTERM, SIGINT]);
            // Dispatch the exit signals that might've come in.
            pcntl_signal_dispatch();
        };

        $this->log('starting consumer' . ($this->timeout ? " (timeout {$this->timeout}s)" : ''), 'info');

        $this->connect();

        $this->bootConsumer($this->channel, $this->service_id, $handler);

        try {
            $this->log('waiting for messages', 'info');
            while ($this->channel->is_open() && !$this->force_shutdown) {
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

    private function shutdown()
    {
        $this->force_shutdown = true;
    }

    protected function bootConsumer(AMQPChannel $channel, string $tag, callable $callback): void
    {
        $channel->basic_consume(
            queue: 'evt_' . $tag,
            consumer_tag: $tag,
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            callback: $callback,
            ticket: null,
            arguments: [
                // 'x-cancel-on-ha-failover' => ['t', false] // fail over to another node,
            ]
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

    public function log(string $message, $level = 'info'): void
    {
        $now = new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
        echo $now->format('Y-m-d H:i:s') . ' '
            . $this->service_id . '@' . gethostname()
            . ' ' . $level . ': ' . $message . "\n";
    }
}