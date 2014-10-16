<?php namespace Plus\Queue;

use Illuminate\Support\ServiceProvider;
use Plus\Queue\Connectors\RedisConnector;

class QueueServiceProvider extends ServiceProvider {
	
	/**
	 * Indicates if loading of the provider is deferred.
	 *
	 * @var bool
	 */
	protected $defer = false;

	/**
	 * Boot the service provider.
	 *
	 * @return void
	 */
	public function boot()
	{
		$manager = $this->app['queue'];

		$this->registerConnectors($manager);
	}

	public function register()
	{
		//
	}

	/**
	 * Register the connectors on the queue manager.
	 *
	 * @param  \Illuminate\Queue\QueueManager  $manager
	 * @return void
	 */
	public function registerConnectors($manager)
	{
		foreach (array('Redis') as $connector)
		{
			$this->{"register{$connector}Connector"}($manager);
		}
	}

	/**
	 * Register the Redis queue connector.
	 *
	 * @param  \Illuminate\Queue\QueueManager  $manager
	 * @return void
	 */
	protected function registerRedisConnector($manager)
	{
		$app = $this->app;

		$manager->extend('redis', function() use ($app)
		{
			return new RedisConnector($app['redis']);
		});
	}


}
