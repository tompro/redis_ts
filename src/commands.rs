use crate::types::*;
use redis::{cmd, ConnectionLike, FromRedisValue, RedisResult, ToRedisArgs};

/// Provides a high level synchronous API to work with redis time series data types. Uses some abstractions
/// for easier handling of time series related redis command arguments. All commands are directly
/// available on ConnectionLike types from the redis crate.
/// ```rust,no_run
/// # fn run() -> redis::RedisResult<()> {
/// use redis::Commands;
/// use redis_ts::{TsCommands, TsOptions};
///
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
///
/// let _:() = con.ts_create("my_ts", TsOptions::default())?;
/// let ts:u64 = con.ts_add_now("my_ts", 2.0)?;
/// let v:Option<(u64,f64)> = con.ts_get("my_ts")?;
/// # Ok(()) }
/// ```
///
pub trait TsCommands: ConnectionLike + Sized {
    /// Creates a new redis time series key.
    fn ts_create<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        options: TsOptions,
    ) -> RedisResult<RV> {
        cmd("TS.CREATE").arg(key).arg(options).query(self)
    }

    /// Modifies an existing redis time series configuration.
    fn ts_alter<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        options: TsOptions,
    ) -> RedisResult<RV> {
        cmd("TS.ALTER")
            .arg(key)
            .arg(options.uncompressed(false))
            .query(self)
    }

    /// Adds a single time series value with a timestamp to an existing redis time series.
    fn ts_add<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.ADD").arg(key).arg(ts).arg(value).query(self)
    }

    /// Adds a single time series value to an existing redis time series with redis system
    /// time as timestamp.
    fn ts_add_now<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.ADD").arg(key).arg("*").arg(value).query(self)
    }

    /// Adds a single time series value to a redis time series. If the time series does not
    /// yet exist it will be created with given settings.
    fn ts_add_create<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
        options: TsOptions,
    ) -> RedisResult<RV> {
        cmd("TS.ADD")
            .arg(key)
            .arg(ts)
            .arg(value)
            .arg(options)
            .query(self)
    }

    /// Adds multiple time series values to an existing redis time series.
    fn ts_madd<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        values: &[(K, TS, V)],
    ) -> RedisResult<RV> {
        cmd("TS.MADD").arg(values).query(self)
    }

    /// Increments a time series value with redis system time.
    fn ts_incrby_now<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.INCRBY").arg(key).arg(value).query(self)
    }

    /// Increments a time series value with given timestamp.
    fn ts_incrby<K: ToRedisArgs, V: ToRedisArgs, TS: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.INCRBY")
            .arg(key)
            .arg(value)
            .arg("TIMESTAMP")
            .arg(ts)
            .query(self)
    }

    /// Increments a time series value with timestamp. Time series will be created if it
    /// not already exists.
    fn ts_incrby_create<K: ToRedisArgs, V: ToRedisArgs, TS: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
        options: TsOptions,
    ) -> RedisResult<RV> {
        cmd("TS.INCRBY")
            .arg(key)
            .arg(value)
            .arg("TIMESTAMP")
            .arg(ts)
            .arg(options)
            .query(self)
    }

    /// Decrements a time series value with redis system time.
    fn ts_decrby_now<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.DECRBY").arg(key).arg(value).query(self)
    }

    /// Decrements a time series value with given timestamp.
    fn ts_decrby<K: ToRedisArgs, V: ToRedisArgs, TS: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
    ) -> RedisResult<RV> {
        cmd("TS.DECRBY")
            .arg(key)
            .arg(value)
            .arg("TIMESTAMP")
            .arg(ts)
            .query(self)
    }

    /// Decrements a time series value with timestamp. Time series will be created if it
    /// not already exists.
    fn ts_decrby_create<K: ToRedisArgs, V: ToRedisArgs, TS: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ts: TS,
        value: V,
        options: TsOptions,
    ) -> RedisResult<RV> {
        cmd("TS.DECRBY")
            .arg(key)
            .arg(value)
            .arg("TIMESTAMP")
            .arg(ts)
            .arg(options)
            .query(self)
    }

    /// Creates a new redis time series compaction rule.
    fn ts_createrule<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        source_key: K,
        dest_key: K,
        aggregation_type: TsAggregationType,
    ) -> RedisResult<RV> {
        cmd("TS.CREATERULE")
            .arg(source_key)
            .arg(dest_key)
            .arg(aggregation_type)
            .query(self)
    }

    /// Deletes an existing redis time series compaction rule.
    fn ts_deleterule<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        source_key: K,
        dest_key: K,
    ) -> RedisResult<RV> {
        cmd("TS.DELETERULE")
            .arg(source_key)
            .arg(dest_key)
            .query(self)
    }

    /// Executes a redis time series range query.
    fn ts_range<
        K: ToRedisArgs,
        FTS: ToRedisArgs,
        TTS: ToRedisArgs,
        C: ToRedisArgs,
        TS: std::marker::Copy + FromRedisValue,
        V: std::marker::Copy + FromRedisValue,
    >(
        &mut self,
        key: K,
        from_timestamp: FTS,
        to_timestamp: TTS,
        count: Option<C>,
        aggregation_type: Option<TsAggregationType>,
    ) -> RedisResult<TsRange<TS, V>> {
        let mut c = cmd("TS.RANGE");
        c.arg(key).arg(from_timestamp).arg(to_timestamp);
        if let Some(ct) = count {
            c.arg("COUNT").arg(ct);
        }
        c.arg(aggregation_type).query(self)
    }

    /// Executes multiple redis time series range queries.
    fn ts_mrange<
        FTS: ToRedisArgs,
        TTS: ToRedisArgs,
        C: ToRedisArgs,
        TS: std::default::Default + FromRedisValue + Copy,
        V: std::default::Default + FromRedisValue + Copy,
    >(
        &mut self,
        from_timestamp: FTS,
        to_timestamp: TTS,
        count: Option<C>,
        aggregation_type: Option<TsAggregationType>,
        filter_options: TsFilterOptions,
    ) -> RedisResult<TsMrange<TS, V>> {
        let mut c = cmd("TS.MRANGE");
        c.arg(from_timestamp).arg(to_timestamp);
        if let Some(ct) = count {
            c.arg("COUNT").arg(ct);
        }
        c.arg(aggregation_type).arg(filter_options);
        c.query(self)
    }

    /// Returns the latest (current) value in a redis time series.
    fn ts_get<K: ToRedisArgs, TS: FromRedisValue, V: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisResult<Option<(TS, V)>> {
        cmd("TS.GET").arg(key).query(self).or_else(|_| Ok(None))
    }

    /// Returns the latest (current) value from multiple redis time series.
    fn ts_mget<
        TS: std::default::Default + FromRedisValue,
        V: std::default::Default + FromRedisValue,
    >(
        &mut self,
        filter_options: TsFilterOptions,
    ) -> RedisResult<TsMget<TS, V>> {
        cmd("TS.MGET").arg(filter_options).query(self)
    }

    /// Returns information about a redis time series key.
    fn ts_info<K: ToRedisArgs>(&mut self, key: K) -> RedisResult<TsInfo> {
        cmd("TS.INFO").arg(key).query(self)
    }

    /// Returns a filtered list of redis time series keys.
    fn ts_queryindex(&mut self, filter_options: TsFilterOptions) -> RedisResult<Vec<String>> {
        cmd("TS.QUERYINDEX")
            .arg(filter_options.get_filters())
            .query(self)
    }
}

impl<T> TsCommands for T where T: ConnectionLike {}
