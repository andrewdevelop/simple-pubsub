<?php

declare(strict_types=1);

namespace Core\Messaging;

use Core\Messaging\Contracts\Consumer as ConsumerContract;
use Core\Messaging\Contracts\Publisher as PublisherContract;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Support\DeferrableProvider;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

class MessagingServiceProvider extends ServiceProvider implements DeferrableProvider
{
    /**
     * Configure and register bindings in the container.
     * @return void
     */
    public function register()
    {
        $this->configure();

        $this->app->singleton(InternalConfig::class, function (\Illuminate\Contracts\Container\Container $app) {
            /** @var Repository $cfg */
            $cfg = $app->make('config');
            /** @var string $sid */
            $sid = $cfg->get('mq.service_id');
            if (empty($sid)) {
                throw new InvalidArgumentException('Message queue not configured. Please declare a unique ID for your service in /config/mq.php file with key "service_id" or in your .env file with key "SERVICE_ID".');
            }

            $host = $cfg->get('mq.host', 'localhost');
            $port = intval($cfg->get('mq.port', 5672));
            $login = $cfg->get('mq.login', 'guest');
            $password = $cfg->get('mq.password', 'guest');
            $vhost = $cfg->get('mq.vhost', '/');
            $exchange_name = $cfg->get('mq.exchange_name', 'events');
            $queue_prefix = $cfg->get('mq.queue_prefix', 'evt');
            $with_dlq = filter_var($cfg->get('mq.with_dlq', false), FILTER_VALIDATE_BOOLEAN);
            $max_retries = $cfg->get('mq.max_retries', 5);
            $retry_delay_sec = $cfg->get('mq.retry_delay_sec', 10);
            $dlq_message_ttl_sec = $cfg->get('mq.dlq_message_ttl_sec', 0);
            $connection_timeout = $cfg->get('mq.connection_timeout', 120);
            $heartbeat_timeout = $cfg->get('mq.heartbeat_timeout', 60);

            return new InternalConfig(
                host: $host,
                port: $port,
                login: $login,
                password: $password,
                vhost: $vhost,
                service_id: $sid,
                exchange_name: $exchange_name,
                queue_prefix: $queue_prefix,
                with_dlq: $with_dlq,
                max_retries: $max_retries,
                retry_delay_sec: $retry_delay_sec,
                dlq_message_ttl_sec: $dlq_message_ttl_sec,
                connection_timeout: $connection_timeout,
                heartbeat_timeout: $heartbeat_timeout,
            );
        });
        $this->app->singleton(PublisherContract::class, function (\Illuminate\Contracts\Container\Container $app) {
            /** @var InternalConfig $config */
            $config = $app->make(InternalConfig::class);
            $instance = new Publisher($config);
            if ($app->bound('log')) {
                /** @var LoggerInterface $logger */
                $logger = $app->make('log');
                $instance->withLogger($logger);
            }
            return $instance;
        });
        $this->app->singleton(ConsumerContract::class, function (\Illuminate\Contracts\Container\Container $app) {
            /** @var InternalConfig $config */
            $config = $app->make(InternalConfig::class);
            $instance = new Consumer($config);
            if ($app->bound('log')) {
                /** @var LoggerInterface $logger */
                $logger = $app->make('log');
                $instance->withLogger($logger);
            }
            return $instance;
        });
    }

    /**
     * Perform post-registration booting of services.
     * @return void
     */
    public function boot()
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                Console\ConsumeMessages::class,
            ]);
        }
    }

    /**
     * Get the services provided by the provider.
     * @return array<int, string>
     */
    public function provides()
    {
        return [
            InternalConfig::class,
            PublisherContract::class,
            ConsumerContract::class,
        ];
    }

    private function configure(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/mq.php', 'mq');
        if (method_exists($this->app, 'configure')) {
            $this->app->configure('mq');
        }
    }
}