<?php namespace Owlgrin\Plus\Queue;

use Illuminate\Redis\Database;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Queue\RedisQueue as BaseQueue;
use Illuminate\Queue\QueueInterface;

class RedisQueue extends BaseQueue implements QueueInterface {

	/**
	 * Create a new Redis queue instance.
	 *
	 * @param  \Illuminate\Redis\Database  $redis
	 * @param  string  $default
	 * @param  string  $connection
	 * @return void
	 */
	public function __construct(Database $redis, $default = 'default', $connection = null)
	{
		parent::__construct($redis, $default, $connection);
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string  $job
	 * @param  mixed   $data
	 * @param  string  $queue
	 * @return void
	 */
	public function push($job, $data = '', $queue = null)
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue);
	}

	/**
	 * Push a raw payload onto the queue.
	 *
	 * @param  string  $payload
	 * @param  string  $queue
	 * @param  array   $options
	 * @return mixed
	 */
	public function pushRaw($payload, $queue = null, array $options = array())
	{
		$this->connection()->rpush($this->getQueue($queue), $payload);

		return array_get(json_decode($payload, true), 'id');
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int  $delay
	 * @param  string  $job
	 * @param  mixed   $data
	 * @param  string  $queue
	 * @return void
	 */
	public function later($delay, $job, $data = '', $queue = null)
	{
		$payload = $this->createPayload($job, $data);

		$delay = $this->getSeconds($delay);

		$this->connection()->zadd($this->getQueue($queue).':delayed', $this->getTime() + $delay, $payload);

		return array_get(json_decode($payload, true), 'id');
	}

	/**
	 * Release a reserved job back onto the queue.
	 *
	 * @param  string  $queue
	 * @param  string  $payload
	 * @param  int  $delay
	 * @param  int  $attempts
	 * @return void
	 */
	public function release($queue, $payload, $delay, $attempts)
	{
		$payload = $this->setMeta($payload, 'attempts', $attempts);

		$this->connection()->zadd($this->getQueue($queue).':delayed', $this->getTime() + $delay, $payload);
	}

	/**
	 * Pop the next job off of the queue.
	 *
	 * @param  string  $queue
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = null)
	{
		$original = $queue ?: $this->default;

		$this->migrateAllExpiredJobs($queue = $this->getQueue($queue));

		$job = $this->connection()->lpop($queue);

		if ( ! is_null($job))
		{
			$this->connection()->zadd($queue.':reserved', $this->getTime() + 60, $job);

			return new RedisJob($this->container, $this, $job, $original);
		}
	}

	/**
	 * Delete a reserved job from the queue.
	 *
	 * @param  string  $queue
	 * @param  string  $job
	 * @return void
	 */
	public function deleteReserved($queue, $job)
	{
		$this->connection()->zrem($this->getQueue($queue).':reserved', $job);
	}

	/**
	 * Migrate all of the waiting jobs in the queue.
	 *
	 * @param  string  $queue
	 * @return void
	 */
	protected function migrateAllExpiredJobs($queue)
	{
		$this->migrateExpiredJobs($queue.':delayed', $queue);

		$this->migrateExpiredJobs($queue.':reserved', $queue);
	}

	/**
	 * Migrate the delayed jobs that are ready to the regular queue.
	 *
	 * @param  string  $from
	 * @param  string  $to
	 * @return void
	 */
	public function migrateExpiredJobs($from, $to)
	{
		$options = ['cas' => true, 'watch' => $from, 'retry' => 10];

		$this->connection()->transaction($options, function ($transaction) use ($from, $to)
		{
			// First we need to get all of jobs that have expired based on the current time
			// so that we can push them onto the main queue. After we get them we simply
			// remove them from this "delay" queues. All of this within a transaction.
			$jobs = $this->getExpiredJobs(
				$transaction, $from, $time = $this->getTime()
			);

			// If we actually found any jobs, we will remove them from the old queue and we
			// will insert them onto the new (ready) "queue". This means they will stand
			// ready to be processed by the queue worker whenever their turn comes up.
			if (count($jobs) > 0)
			{
				$this->removeExpiredJobs($transaction, $from, $time);

				$this->pushExpiredJobsOntoNewQueue($transaction, $to, $jobs);
			}
		});
	}

	/**
	 * Get the expired jobs from a given queue.
	 *
	 * @param  \Predis\Transaction\MultiExec  $transaction
	 * @param  string  $from
	 * @param  int  $time
	 * @return array
	 */
	protected function getExpiredJobs($transaction, $from, $time)
	{
		return $transaction->zrangebyscore($from, '-inf', $time);
	}

	/**
	 * Remove the expired jobs from a given queue.
	 *
	 * @param  \Predis\Transaction\MultiExec  $transaction
	 * @param  string  $from
	 * @param  int  $time
	 * @return void
	 */
	protected function removeExpiredJobs($transaction, $from, $time)
	{
		$transaction->multi();

		$transaction->zremrangebyscore($from, '-inf', $time);
	}

	/**
	 * Push all of the given jobs onto another queue.
	 *
	 * @param  \Predis\Transaction\MultiExec  $transaction
	 * @param  string  $to
	 * @param  array  $jobs
	 * @return void
	 */
	protected function pushExpiredJobsOntoNewQueue($transaction, $to, $jobs)
	{
		call_user_func_array([$transaction, 'rpush'], array_merge([$to], $jobs));
	}

	public function connection()
	{
		return $this->redis->connection($this->connection);
	}

	/**
	 * Get the underlying Redis instance.
	 *
	 * @return \Illuminate\Redis\Database
	 */
	public function getRedis()
	{
		return $this->redis;
	}

}
