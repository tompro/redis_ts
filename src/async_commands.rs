
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

    /// Adds a single time series value to a redis time series. If the time series does not 
    /// yet exist it will be created with given settings.
    fn ts_add_create<'a, K: ToRedisArgs + Send + Sync + 'a, TS: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, key: K, ts: TS, value: V, options: TsOptions) -> RedisFuture<RV> {
    		Box::pin(async move { cmd("TS.ADD").arg(key).arg(ts).arg(value).arg(options).query_async(self).await })
    }

    /// Adds multiple time series values to an existing redis time series.
    fn ts_madd<'a, K: ToRedisArgs + Send + Sync + 'a, TS: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, values:&'a[(K, TS, V)]) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.MADD").arg(values).query_async(self).await })
    }

    /// Increments a time series value with redis system time.
    fn ts_incrby_now<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, key: K, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.INCRBY").arg(key).arg(value).query_async(self).await })
    }

    /// Increments a time series value with given timestamp.
    fn ts_incrby<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, TS:ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, key: K, ts:TS, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.INCRBY").arg(key).arg(value).arg("TIMESTAMP").arg(ts).query_async(self).await })
    }

    /// Increments a time series value with timestamp. Time series will be created if it 
    /// not already exists.
    fn ts_incrby_create<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, TS:ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, key: K, ts:TS, value: V, options:TsOptions) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.INCRBY").arg(key).arg(value).arg("TIMESTAMP")
            .arg(ts).arg(options).query_async(self).await })
    }

    /// Decrements a time series value with redis system time.
    fn ts_decrby_now<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, key: K, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.DECRBY").arg(key).arg(value).query_async(self).await })
    }

    /// Decrements a time series value with given timestamp.
    fn ts_decrby<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, TS:ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, key: K, ts:TS, value: V) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.DECRBY").arg(key).arg(value).arg("TIMESTAMP").arg(ts).query_async(self).await })
    }

    /// Decrements a time series value with timestamp. Time series will be created if it 
    /// not already exists.
    fn ts_decrby_create<'a, K: ToRedisArgs + Send + Sync + 'a, V: ToRedisArgs + Send + Sync + 'a, TS:ToRedisArgs + Send + Sync + 'a, 
    	RV: FromRedisValue>(&'a mut self, key: K, ts:TS, value: V, options:TsOptions) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.DECRBY").arg(key).arg(value).arg("TIMESTAMP")
            .arg(ts).arg(options).query_async(self).await })
    }

    /// Creates a new redis time series compaction rule.
    fn ts_createrule<'a, K: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, source_key: K, dest_key: K, aggregation_type:TsAggregationType) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.CREATERULE").arg(source_key).arg(dest_key).arg(aggregation_type).query_async(self).await})
    }

    /// Deletes an existing redis time series compaction rule.
    fn ts_deleterule<'a, K: ToRedisArgs + Send + Sync + 'a, RV: FromRedisValue>(
        &'a mut self, source_key: K, dest_key: K) -> RedisFuture<RV> {
        Box::pin(async move { cmd("TS.DELETERULE").arg(source_key).arg(dest_key).query_async(self).await })
    }

    /// Returns the latest (current) value in a redis time series.
    fn ts_get<'a, K:ToRedisArgs + Send + Sync + 'a, TS: FromRedisValue, V:FromRedisValue>(&'a mut self, key:K) -> RedisFuture<Option<(TS,V)>> {
        Box::pin(async move { cmd("TS.GET").arg(key).query_async(self).await.or_else(|_| { Ok(None) }) })
    }

    /// Returns the latest (current) value from multiple redis time series.
    fn ts_mget<'a, TS: std::default::Default +  FromRedisValue, V: std::default::Default + FromRedisValue>(
        &'a mut self, filter_options:TsFilterOptions) -> RedisFuture<TsMget<TS,V>> {
        Box::pin(async move { cmd("TS.MGET").arg(filter_options).query_async(self).await })
    }

    /// Executes a redis time series range query.
    fn ts_range<'a, K:ToRedisArgs + Send + Sync + 'a, FTS:ToRedisArgs + Send + Sync + 'a, TTS:ToRedisArgs + Send + Sync + 'a, 
    	C:ToRedisArgs + Send + Sync + 'a, TS: std::marker::Copy + FromRedisValue, V: std::marker::Copy + FromRedisValue>(
        &'a mut self, key:K, from_timestamp:FTS, to_timestamp:TTS, count:Option<C>,aggregation_type:Option<TsAggregationType>) -> RedisFuture<TsRange<TS,V>> {
        let mut c = cmd("TS.RANGE");
        c.arg(key).arg(from_timestamp).arg(to_timestamp);
        if let Some(ct) = count {
            c.arg("COUNT").arg(ct);
        }
        Box::pin(async move { c.arg(aggregation_type).query_async(self).await })
    }

    /// Executes multiple redis time series range queries.
    fn ts_mrange<'a, FTS:ToRedisArgs + Send + Sync + 'a, TTS:ToRedisArgs + Send + Sync + 'a, C:ToRedisArgs + Send + Sync + 'a, 
    TS: std::default::Default + FromRedisValue + Copy, V: std::default::Default + FromRedisValue + Copy>(
        &mut self, from_timestamp:FTS, to_timestamp:TTS, count:Option<C>, aggregation_type:Option<TsAggregationType>, 
        filter_options:TsFilterOptions) -> RedisFuture<TsMrange<TS,V>> {
        let mut c = cmd("TS.MRANGE");
        c.arg(from_timestamp).arg(to_timestamp);
        if let Some(ct) = count {
            c.arg("COUNT").arg(ct);
        }
        c.arg(aggregation_type).arg(filter_options);
        Box::pin(async move { c.query_async(self).await })
    }

    /// Returns a filtered list of redis time series keys.
    fn ts_queryindex<'a>(&'a mut self, filter_options:TsFilterOptions) -> RedisFuture<Vec<String>> {
        Box::pin(async move { cmd("TS.QUERYINDEX").arg(filter_options.get_filters()).query_async(self).await })
    }
}

impl<T> AsyncTsCommands for T where T: Send + ConnectionLike {}
