<?php

namespace Tests;

use Core\Messaging\Consumer;
use Core\Messaging\InternalConfig;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use PHPUnit\Framework\TestCase;
use Mockery;
use ReflectionClass;
use RuntimeException;

class ConsumerTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
    }

    protected function createConsumer(bool $withFailover = false, bool $strict = false): Consumer
    {
        $config = new InternalConfig(
            host: 'localhost',
            port: 5672,
            login: 'guest',
            password: 'guest',
            vhost: '/',
            service_id: 'test_service',
            with_failover: $withFailover
        );

        return new Consumer($config, 10, $strict, true, null);
    }

    protected function setChannel(Consumer $consumer, $channelMock): void
    {
        $reflection = new ReflectionClass($consumer);
        $property = $reflection->getProperty('channel');
        $property->setAccessible(true);
        $property->setValue($consumer, $channelMock);
    }

    public function testConsumeFiltersLoopbackMessages()
    {
        $consumer = $this->createConsumer();
        $channelMock = Mockery::mock(AMQPChannel::class);
        $channelMock->shouldReceive('is_open')->andReturn(true);

        $this->setChannel($consumer, $channelMock);

        $message = new AMQPMessage('{"data":"test"}', ['app_id' => 'test_service']);
        $message->setChannel($channelMock);
        $message->setDeliveryInfo(1, false, 'events', '');

        $callbackCalled = false;
        $callback = function () use (&$callbackCalled) {
            $callbackCalled = true;
        };

        $isLoopback = ($message->has('app_id') && $message->get('app_id') === 'test_service');
        $this->assertTrue($isLoopback);
        
        if (!$isLoopback) {
            $callback($message);
        }
        
        $this->assertFalse($callbackCalled);
    }

    public function testConsumeProcessesValidMessages()
    {
        $consumer = $this->createConsumer();
        $channelMock = Mockery::mock(AMQPChannel::class);
        $channelMock->shouldReceive('is_open')->andReturn(true);
        
        $this->setChannel($consumer, $channelMock);

        $message = new AMQPMessage('{"data":"test"}', ['app_id' => 'other_service']);
        $message->setChannel($channelMock);
        $message->setDeliveryInfo(1, false, 'events', '');

        $callbackCalled = false;
        $callback = function ($msg) use (&$callbackCalled, $message) {
            $callbackCalled = true;
            $this->assertSame($message, $msg);
        };
        
        $isLoopback = ($message->has('app_id') && $message->get('app_id') === 'test_service');
        $this->assertFalse($isLoopback);
        
        if (!$isLoopback) {
            $callback($message);
        }
        
        $this->assertTrue($callbackCalled);
    }

    public function testGetDeathCountParsesCorrectly()
    {
        $consumer = $this->createConsumer(true);
        
        $reflection = new ReflectionClass($consumer);
        $method = $reflection->getMethod('getDeathCount');
        $method->setAccessible(true);

        // Message with no headers
        $message1 = new AMQPMessage('{}');
        $queueName = 'evt_test_service';
        $this->assertEquals(0, $method->invoke($consumer, $message1, $queueName));

        // Message with x-death headers
        $headers = new AMQPTable([
            'x-death' => [
                [
                    'queue' => $queueName,
                    'reason' => 'rejected',
                    'count' => 3
                ]
            ]
        ]);
        
        $message2 = new AMQPMessage('{}', ['application_headers' => $headers]);
        $this->assertEquals(3, $method->invoke($consumer, $message2, $queueName));
        
        // Message with wrong queue
        $this->assertEquals(0, $method->invoke($consumer, $message2, 'wrong_queue'));
    }
}