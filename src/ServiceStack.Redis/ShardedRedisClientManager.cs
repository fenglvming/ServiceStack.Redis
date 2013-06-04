using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ServiceStack.Redis.Support;
using System.Threading;

namespace ServiceStack.Redis
{
	/// <summary>
	/// Provides sharding of redis client connections.
	/// uses consistent hashing to distribute keys across connection pools
	/// </summary>
	public class ShardedRedisClientManager
	{
		private readonly ConsistentHash<ShardedConnectionPool> _consistentHash;
        private List<KeyValuePair<ShardedConnectionPool, int>> removed = new List<KeyValuePair<ShardedConnectionPool, int>>();
		public ShardedRedisClientManager(params ShardedConnectionPool[] connectionPools)
		{
			if (connectionPools == null) throw new ArgumentNullException("connection pools can not be null.");

			List<KeyValuePair<ShardedConnectionPool, int>> pools = new List<KeyValuePair<ShardedConnectionPool, int>>();
			foreach (var connectionPool in connectionPools)
			{
				pools.Add(new KeyValuePair<ShardedConnectionPool, int>(connectionPool, connectionPool.weight));
			}
			_consistentHash = new ConsistentHash<ShardedConnectionPool>(pools);
            ValidShardingPoll(pools);
		}

		/// <summary>
		/// maps a key to a redis connection pool
		/// </summary>
		/// <param name="key">key to map</param>
		/// <returns>a redis connection pool</returns>
		public ShardedConnectionPool GetConnectionPool(string key)
		{
            lock (_consistentHash)
            {
                return _consistentHash.GetTarget(key);
            }
		}

        public void ValidShardingPoll(List<KeyValuePair<ShardedConnectionPool, int>> pools)
        {
            var newpools = pools;
            Thread t = new Thread(Func => {
                 while(true){
                         checkValid(newpools);
                         Thread.Sleep(60000);
                         checkInvalid();
                         Thread.Sleep(60000);
                 }
            });
            t.Start();
        }

        private void checkValid(List<KeyValuePair<ShardedConnectionPool, int>> pools)
        {
            foreach (var pool in pools)
            {
                var writeclient = pool.Key.GetClient();
                var readclient = pool.Key.GetReadOnlyClient();
                var nwclient = writeclient as RedisNativeClient;
                var nrclient = readclient as RedisNativeClient;
                bool needRemove = false;
                try
                {
                    if (!nwclient.Ping())
                    {
                        if (!nrclient.Ping())
                        {
                            needRemove = true;
                        }
                    }
                }
                catch (Exception e)
                {
                    needRemove = true;
                }

                if (needRemove)
                {
                    removed.Add(pool);
                    lock (_consistentHash)
                    {
                        _consistentHash.RemoveTarget(pool.Key, pool.Value);
                    }
                }
            }
        }


        public void checkInvalid() {
            List<KeyValuePair<ShardedConnectionPool, int>> addBack = new List<KeyValuePair<ShardedConnectionPool, int>>();
            foreach (var pool in removed)
            {
                var writeclient = pool.Key.GetClient();
                var readclient = pool.Key.GetReadOnlyClient();
                var nwclient = writeclient as RedisNativeClient;
                var nrclient = readclient as RedisNativeClient;
                if (nwclient.Ping())
                {
                    if (nrclient.Ping())
                    {
                        lock (_consistentHash)
                        {
                            addBack.Add(pool);
                            _consistentHash.AddTarget(pool.Key, pool.Value);
                        }
                    }
                }
            }

            foreach (var pool in addBack)
            {
                removed.Remove(pool);
            }
        }

	}
}
