
use redis::FromRedisValue;
use redis::{cmd, RedisFuture, ToRedisArgs};
use redis::aio::ConnectionLike;
use crate::types::*;

pub trait AsyncTsCommands: ConnectionLike + Send + Sized {
	
  	/// Returns information about a redis time series key.
    fn ts_info<'a, K:ToRedisArgs + Send + Sync + 'a>(&'a mut self, key:K) -> RedisFuture<TsInfo> {
        Box::pin(async move { cmd("TS.INFO").arg(key).query_async(self).await })
    }

    /// Creates a new redis time series key.
    fn ts_create<'a, K: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, key: K, options:TsOptions) -> RedisFuture<RV> {
    	Box::pin(async move { cmd("TS.CREATE").arg(key).arg(options).query_async(self).await })
    }

    /// Adds a single time series value with a timestamp to an existing redis time series.
    fn ts_add<'a, K: ToRedisArgs + Send + Sync + 'a, TS: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, key: K, ts:TS, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.ADD").arg(key).arg(ts).arg(value).query_async(self).await })
    }

    /// Adds a single time series value to an existing redis time series with redis system
    /// time as timestamp.
    fn ts_add_now<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, key: K, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.ADD").arg(key).arg("*").arg(value).query_async(self).await })
    }

}

impl<T> AsyncTsCommands for T where T: Send + ConnectionLike {}
