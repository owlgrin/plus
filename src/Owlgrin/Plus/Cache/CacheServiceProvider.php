<?php namespace Owlgrin\Plus\Cache;

use Illuminate\Support\ServiceProvider;
use Illuminate\Cache\Repository;
use Illuminate\Cache\RedisStore;

class CacheServiceProvider extends ServiceProvider {

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
		$manager = $this->app['cache'];

		$this->registerDrivers($manager);
	}

	public function register()
	{
		//
	}

	/**
	 * Register the drivers
	 *
	 * @return void
	 */
	public function registerDrivers($manager)
	{
		foreach (array('Redis') as $connector)
		{
			$this->{"register{$connector}Driver"}($manager);
		}
	}

	protected function registerRedisDriver($manager)
	{
		$app = $this->app;

		$manager->extend('redis', function() use ($app)
		{
			return new Repository(new RedisStore($app['redis'], $this->getPrefix(), $this->getConnection()));
		});
	}

	protected function getPrefix()
	{
		return $this->app['config']['cache.prefix'];
	}

	protected function getConnection()
	{
		return $this->app['config']['cache.connection'];
	}

}
