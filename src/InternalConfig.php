<?php

namespace Core\Messaging;

final class InternalConfig
{
    public function __construct(
        public readonly string $host,
        public readonly int    $port,
        public readonly string $login,
        public readonly string $password,
        public readonly string $vhost,
        public readonly string $service_id,
        public readonly string $exchange_name = 'events',
        public readonly string $queue_prefix = 'evt',
        public readonly bool   $with_dlq = false,
        public readonly int    $max_retries = 5,
        public readonly int    $retry_delay_sec = 10,
        public readonly int    $dlq_message_ttl_sec = 0, // 0 means messages won't expire in DLQ
        public readonly int    $connection_timeout = 120,
        public readonly int    $heartbeat_timeout = 60,
    )
    {
    }

    /**
     * @param array<string, mixed> $config
     * @return self
     */
    public static function fromArray(array $config): self
    {
        /** @var string $host */
        $host = $config['host'] ?? 'localhost';
        /** @var int $port */
        $port = $config['port'] ?? 5672;
        /** @var string $login */
        $login = $config['login'] ?? 'guest';
        /** @var string $password */
        $password = $config['password'] ?? 'guest';
        /** @var string $vhost */
        $vhost = $config['vhost'] ?? '/';
        /** @var string $service_id */
        $service_id = $config['service_id'] ?? '';
        /** @var string $exchange_name */
        $exchange_name = $config['exchange_name'] ?? 'events';
        /** @var string $queue_prefix */
        $queue_prefix = $config['queue_prefix'] ?? 'evt';
        /** @var bool $with_dlq */
        $with_dlq = filter_var($config['with_dlq'] ?? false, FILTER_VALIDATE_BOOLEAN);
        /** @var int $max_retries */
        $max_retries = $config['max_retries'] ?? 5;
        /** @var int $retry_delay_sec */
        $retry_delay_sec = $config['retry_delay_sec'] ?? 10;
        /** @var int $dlq_message_ttl_sec */
        $dlq_message_ttl_sec = $config['dlq_message_ttl_sec'] ?? 86400;
        /** @var int $connection_timeout */
        $connection_timeout = $config['connection_timeout'] ?? 120;
        /** @var int $heartbeat_timeout */
        $heartbeat_timeout = $config['heartbeat_timeout'] ?? 60;

        return new self(
            host: $host,
            port: $port,
            login: $login,
            password: $password,
            vhost: $vhost,
            service_id: $service_id,
            exchange_name: $exchange_name,
            queue_prefix: $queue_prefix,
            with_dlq: $with_dlq,
            max_retries: $max_retries,
            retry_delay_sec: $retry_delay_sec,
            dlq_message_ttl_sec: $dlq_message_ttl_sec,
            connection_timeout: $connection_timeout,
            heartbeat_timeout: $heartbeat_timeout,
        );
    }
}