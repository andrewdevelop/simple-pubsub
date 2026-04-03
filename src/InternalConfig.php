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
        public readonly bool   $with_failover = false,
        public readonly int    $max_retries = 5,
        public readonly int    $retry_delay_sec = 10,
        public readonly int    $connection_timeout = 120,
        public readonly int    $heartbeat_timeout = 60,
    )
    {
    }

    public static function fromArray(array $config): self
    {
        return new self(
            host: $config['host'] ?? 'localhost',
            port: $config['port'] ?? 5672,
            login: $config['login'] ?? 'guest',
            password: $config['password'] ?? 'guest',
            vhost: $config['vhost'] ?? '/',
            service_id: $config['service_id'] ?? null,
            exchange_name: $config['exchange_name'] ?? 'events',
            queue_prefix: $config['queue_prefix'] ?? 'evt',
            with_failover: $config['with_failover'] ?? false,
            max_retries: $config['max_retries'] ?? 5,
            retry_delay_sec: $config['retry_delay_sec'] ?? 10,
            connection_timeout: $config['connection_timeout'] ?? 120,
            heartbeat_timeout: $config['heartbeat_timeout'] ?? 60,
        );
    }
}