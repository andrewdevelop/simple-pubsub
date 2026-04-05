<?php

return [
    /** ID */
    'service_id' => env('SERVICE_ID', null),

    /** RabbitMQ config */
    'dsn' => env('RABBITMQ_DSN', null),
    'host' => env('RABBITMQ_HOST', '127.0.0.1'),
    'port' => env('RABBITMQ_PORT', 5672),
    'login' => env('RABBITMQ_LOGIN', 'guest'),
    'password' => env('RABBITMQ_PASSWORD', 'guest'),
    'vhost' => env('RABBITMQ_VHOST', '/'),
    'connection_timeout' => env('RABBITMQ_CONNECTION_TIMEOUT', 120),
    'heartbeat_timeout' => env('RABBITMQ_HEARTBEAT_TIMEOUT', 60),
    
    /** Topology config */
    'exchange_name' => env('RABBITMQ_EXCHANGE_NAME', 'events'),
    'queue_prefix' => env('RABBITMQ_QUEUE_PREFIX', 'evt'),

    'with_failover' => env('RABBITMQ_WITH_FAILOVER', false),
    'max_retries' => env('RABBITMQ_MAX_RETRIES', 5),
    'retry_delay_sec' => env('RABBITMQ_RETRY_DELAY_SEC', 10),
    'dlq_message_ttl_sec' => env('RABBITMQ_DLQ_MESSAGE_TTL_SEC', 432000), // 5 days
    'ssl_params' => [
        'ssl_on' => env('RABBITMQ_SSL', false),
        'cafile' => env('RABBITMQ_SSL_CAFILE', null),
        'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
        'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
        'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
        'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
    ],
];