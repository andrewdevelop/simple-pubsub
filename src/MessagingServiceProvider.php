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
            $sid = $cfg->get('mq.service_id');
            if (empty($sid)) {
                throw new InvalidArgumentException('Message queue not configured. Please declare a unique ID for your service in /config/mq.php file with key "service_id" or in your .env file with key "SERVICE_ID".');
            }

            $host = $cfg->get('mq.host', 'localhost');
            $port = $cfg->get('mq.port', 5672);
            $login = $cfg->get('mq.login', 'guest');
            $password = $cfg->get('mq.password', 'guest');
            $vhost = $cfg->get('mq.vhost', '/');
            $exchange_name = $cfg->get('mq.exchange_name', 'events');
            $queue_prefix = $cfg->get('mq.queue_prefix', 'evt');
            $with_failover = $cfg->get('mq.with_failover', false);
            $max_retries = $cfg->get('mq.max_retries', 5);
            $retry_delay_sec = $cfg->get('mq.retry_delay_sec', 10);
            $connection_timeout = $cfg->get('mq.connection_timeout', 120);
            $heartbeat_timeout = $cfg->get('mq.heartbeat_timeout', 60);

            return new InternalConfig(
                host: is_scalar($host) ? (string)$host : 'localhost',
                port: is_numeric($port) ? (int)$port : 5672,
                login: is_scalar($login) ? (string)$login : 'guest',
                password: is_scalar($password) ? (string)$password : 'guest',
                vhost: is_scalar($vhost) ? (string)$vhost : '/',
                service_id: is_scalar($sid) ? (string)$sid : '',
                exchange_name: is_scalar($exchange_name) ? (string)$exchange_name : 'events',
                queue_prefix: is_scalar($queue_prefix) ? (string)$queue_prefix : 'evt',
                with_failover: filter_var($with_failover, FILTER_VALIDATE_BOOLEAN),
                max_retries: is_numeric($max_retries) ? (int)$max_retries : 5,
                retry_delay_sec: is_numeric($retry_delay_sec) ? (int)$retry_delay_sec : 10,
                connection_timeout: is_numeric($connection_timeout) ? (int)$connection_timeout : 120,
                heartbeat_timeout: is_numeric($heartbeat_timeout) ? (int)$heartbeat_timeout : 60,
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