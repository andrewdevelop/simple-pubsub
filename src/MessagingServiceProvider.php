<?php 

namespace Core\Messaging;

use Illuminate\Support\ServiceProvider;
use Core\Messaging\Contracts\Publisher as PublisherContract;
use Core\Messaging\Contracts\Consumer as ConsumerContract;
use Core\Messaging\Publisher;
use Core\Messaging\Consumer;
use InvalidArgumentException;

class MessagingServiceProvider extends ServiceProvider
{
	/**
	 * Configure and register bindings in the container.
	 * @return void
	 */
	public function register()
	{
	    $this->mergeConfigFrom(__DIR__.'/../config/mq.php', 'mq');
	    $this->app->configure('mq');

        if (!$this->app->config->get('mq.service_id')) {
        	throw new InvalidArgumentException('Message queue not configured. Please declare a unique ID for your service in /config/mq.php file with key "service_id" or in your .env file with key "SERVICE_ID".');
        }

        $config = [
            'host'       => $this->app->config->get('mq.host'),
            'port'       => $this->app->config->get('mq.port'),
            'login'      => $this->app->config->get('mq.login'),
            'password'   => $this->app->config->get('mq.password'),
            'vhost'      => $this->app->config->get('mq.vhost'),
            'service_id' => $this->app->config->get('mq.service_id'),
        ];
        $args = array_values($config);

        $this->app->singleton(PublisherContract::class, function() use ($args) {
            return new Publisher(...$args);
        });

        $this->app->singleton(ConsumerContract::class, function() use ($args) {
            return new Consumer(...$args);
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
     * @return array
     */
    public function provides()
    {
        return [
            PublisherContract::class,
            ConsumerContract::class,
        ];
    }        
}