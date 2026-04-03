<?php

namespace Core\Messaging;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;

class Topology
{
    /**
     * @param AMQPChannel $channel
     * @param string $exchangeName
     * @return void
     */
    public static function declareExchange(AMQPChannel $channel, string $exchangeName = 'events'): void
    {
        $channel->exchange_declare($exchangeName, 'fanout', false, true, false);
    }

    /**
     * @param AMQPChannel $channel
     * @param string $serviceId
     * @param bool $withFailover
     * @param string $exchangeName
     * @param string $queuePrefix
     * @param int $retry_delay_sec
     * @return string
     */
    public static function declareQueue(
        AMQPChannel $channel,
        string $serviceId,
        bool $withFailover = false,
        string $exchangeName = 'events',
        string $queuePrefix = 'evt',
        int $retry_delay_sec = 5
    ): string {
        $queue = $queuePrefix . '_' . $serviceId;
        
        // 1. Declare fanout exchange
        self::declareExchange($channel, $exchangeName);

        $arguments = [];
        if ($withFailover) {
            $retryExchange = 'retry_dx_' . $serviceId;
            $retryQueue = $queue . '_retry_'. $retry_delay_sec;
            $dlqQueue = $queue . '_dlq';

            // Declare Retry Exchange (Direct)
            $channel->exchange_declare($retryExchange, 'direct', false, true, false);

            // Declare Retry Queue (TTL = 5s, DLX points back to main queue)
            $retryArgs = new AMQPTable([
                'x-dead-letter-exchange' => '',
                'x-dead-letter-routing-key' => $queue,
                'x-message-ttl' => $retry_delay_sec,
            ]);
            $channel->queue_declare($retryQueue, false, true, false, false, false, $retryArgs);
            $channel->queue_bind($retryQueue, $retryExchange, $queue);

            // Declare DLQ Queue
            $channel->queue_declare($dlqQueue, false, true, false, false, false, new AMQPTable());

            // Set DLX for Main Queue to route to Retry Exchange on reject
            $arguments['x-dead-letter-exchange'] = $retryExchange;
            $arguments['x-dead-letter-routing-key'] = $queue;
        }

        $table = empty($arguments) ? [] : new AMQPTable($arguments);

        // 2. Declare durable queue
        $channel->queue_declare($queue, false, true, false, false, false, $table);

        // 3. Bind the queue to the exchange
        $channel->queue_bind($queue, $exchangeName);

        return $queue;
    }
}

