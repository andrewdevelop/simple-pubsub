<?php

namespace Tests;

use Core\Messaging\Topology;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use Mockery;

class TopologyTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
    }

    public function testDeclareExchange()
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('exchange_declare')
            ->once()
            ->with('my_exchange', 'fanout', false, true, false);

        Topology::declareExchange($channel, 'my_exchange');
        
        $this->assertTrue(true); // Asserting that the mock expectations were met
    }

    public function testDeclareQueueWithoutDlq()
    {
        $channel = Mockery::mock(AMQPChannel::class);
        
        // From self::declareExchange
        $channel->shouldReceive('exchange_declare')
            ->once()
            ->with('events', 'fanout', false, true, false);

        // From declareQueue
        $channel->shouldReceive('queue_declare')
            ->once()
            ->with('evt_test_service', false, true, false, false, false, []);

        $channel->shouldReceive('queue_bind')
            ->once()
            ->with('evt_test_service', 'events');

        $queueName = Topology::declareQueue($channel, 'test_service', false, 'events', 'evt');
        
        $this->assertEquals('evt_test_service', $queueName);
    }

    public function testDeclareQueueWithDlq()
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $serviceId = 'failover_service';
        $queue = 'myqueue_' . $serviceId;
        $retryExchange = 'retry_dx_' . $serviceId;
        $retryQueue = $queue . '_retry_10';
        $dlqQueue = $queue . '_dlq';

        // From self::declareExchange
        $channel->shouldReceive('exchange_declare')
            ->once()
            ->with('myevents', 'fanout', false, true, false);

        // Retry Exchange
        $channel->shouldReceive('exchange_declare')
            ->once()
            ->with($retryExchange, 'direct', false, true, false);

        // Retry Queue
        $channel->shouldReceive('queue_declare')
            ->once()
            ->with($retryQueue, false, true, false, false, false, Mockery::on(function($arg) use ($queue) {
                return $arg instanceof AMQPTable && 
                       $arg->getNativeData()['x-dead-letter-exchange'] === '' &&
                       $arg->getNativeData()['x-dead-letter-routing-key'] === $queue &&
                       $arg->getNativeData()['x-message-ttl'] === 10;
            }));

        $channel->shouldReceive('queue_bind')
            ->once()
            ->with($retryQueue, $retryExchange, $queue);

        // DLQ Queue
        $channel->shouldReceive('queue_declare')
            ->once()
            ->with($dlqQueue, false, true, false, false, false, Mockery::on(function($arg) {
                return $arg instanceof AMQPTable && 
                       $arg->getNativeData()['x-message-ttl'] === 86400000;
            }));

        // Main Queue with DLX arguments
        $channel->shouldReceive('queue_declare')
            ->once()
            ->with($queue, false, true, false, false, false, Mockery::on(function($arg) use ($retryExchange, $queue) {
                return $arg instanceof AMQPTable && 
                       $arg->getNativeData()['x-dead-letter-exchange'] === $retryExchange &&
                       $arg->getNativeData()['x-dead-letter-routing-key'] === $queue;
            }));

        $channel->shouldReceive('queue_bind')
            ->once()
            ->with($queue, 'myevents');

        $queueName = Topology::declareQueue($channel, $serviceId, true, 'myevents', 'myqueue', 10);
        
        $this->assertEquals($queue, $queueName);
    }
}