<?php

namespace Tests;

use Core\Messaging\InternalConfig;
use Core\Messaging\Publisher;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Mockery;
use ReflectionClass;

class PublisherTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
    }

    public function testPublishMessage()
    {
        $config = new InternalConfig(
            host: 'localhost',
            port: 5672,
            login: 'guest',
            password: 'guest',
            vhost: '/',
            service_id: 'test_service',
            with_dlq: false
        );

        $publisher = Mockery::mock(Publisher::class, [$config])->makePartial()->shouldAllowMockingProtectedMethods();
        
        $channelMock = Mockery::mock(AMQPChannel::class);
        $connectionMock = Mockery::mock(AMQPStreamConnection::class);
        $connectionMock->shouldReceive('channel')->andReturn($channelMock);
        
        $publisher->shouldReceive('connect')->once()->andReturnUsing(function() use ($publisher, $channelMock) {
            $reflection = new ReflectionClass($publisher);
            $property = $reflection->getProperty('channel');
            $property->setAccessible(true);
            $property->setValue($publisher, $channelMock);
        });
        $publisher->shouldReceive('disconnect')->once();

        // The channel should receive the publish command
        $channelMock->shouldReceive('basic_publish')
            ->once()
            ->with(
                Mockery::type(AMQPMessage::class),
                'events',
                '',
                false,
                false,
                null
            );

        $jsonPayload = '{"event": "test_event", "data": "value"}';
        $message = $publisher->publish($jsonPayload);

        $this->assertInstanceOf(AMQPMessage::class, $message);
        
        // Assert loopback prevention is working via app_id
        $this->assertEquals('test_service', $message->get('app_id'));
        $this->assertEquals('application/json', $message->get('content_type'));
    }
}