<?php

namespace Core\Messaging;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

class Topology
{
    /**
     * @param AMQPChannel $channel
     * @param string $exchange_name
     * @return void
     */
    public static function declareExchange(AMQPChannel $channel, string $exchange_name = 'events'): void
    {
        $channel->exchange_declare($exchange_name, 'fanout', false, true, false);
    }

    /**
     * @param AMQPChannel $channel
     * @param string $service_id
     * @param bool $with_dlq
     * @param string $exchange_name
     * @param string $queue_prefix
     * @param int $retry_delay_sec
     * @param int $dlq_message_ttl_sec
     * @return string
     */
    public static function declareQueue(
        AMQPChannel $channel,
        string      $service_id,
        bool        $with_dlq = false,
        string      $exchange_name = 'events',
        string      $queue_prefix = 'evt',
        int         $retry_delay_sec = 5,
        int         $dlq_message_ttl_sec = 86400
    ): string {
        $queue = $queue_prefix . '_' . $service_id;
        
        // 1. Declare fanout exchange
        self::declareExchange($channel, $exchange_name);

        $arguments = [];
        if ($with_dlq) {
            $retry_exchange = 'retry_dx_' . $service_id;
            $retry_queue = $queue . '_retry_'. $retry_delay_sec;
            $dlq = $queue . '_dlq';

            // Declare Retry Exchange (Direct)
            $channel->exchange_declare($retry_exchange, 'direct', false, true, false);

            // Declare Retry Queue (TTL = 5s, DLX points back to main queue)
            $retry_args = new AMQPTable([
                'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => $queue,
                'x-message-ttl' => $retry_delay_sec,
            ]);
            $channel->queue_declare($retry_queue, false, true, false, false, false, $retry_args);
            $channel->queue_bind($retry_queue, $retry_exchange, $queue);

            // Declare DLQ Queue
            $dlq_args = new AMQPTable([
                'x-message-ttl' => $dlq_message_ttl_sec,
            ]);
            $channel->queue_declare($dlq, false, true, false, false, false, $dlq_args);

            // Set DLX for Main Queue to route to Retry Exchange on reject
            $arguments['x-dead-letter-exchange'] = $retry_exchange;
            $arguments['x-dead-letter-routing-key'] = $queue;
        }

        $table = empty($arguments) ? [] : new AMQPTable($arguments);

        // 2. Declare durable queue
        $channel->queue_declare($queue, false, true, false, false, false, $table);

        // 3. Bind the queue to the exchange
        $channel->queue_bind($queue, $exchange_name);

        return $queue;
    }
}

