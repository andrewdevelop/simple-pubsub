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
    'ssl_params' => [
        'ssl_on' => env('RABBITMQ_SSL', false),
        'cafile' => env('RABBITMQ_SSL_CAFILE', null),
        'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
        'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
        'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
        'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
    ],
];