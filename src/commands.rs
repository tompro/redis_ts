
use redis::{cmd, ConnectionLike, FromRedisValue, RedisResult, ToRedisArgs};
use crate::types::{TsOptions, TsAggregationType, TsFilterOptions, TsInfo, TsMgetResult,TsMrangeResult};

pub trait TsCommands: ConnectionLike + Sized {

    fn ts_create<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, options:TsOptions) -> RedisResult<RV> {
        cmd("TS.CREATE").arg(key).arg(options).query(self)
    }

    fn ts_alter<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, options:TsOptions) -> RedisResult<RV> {
        cmd("TS.ALTER").arg(key).arg(options.uncompressed(false)).query(self)
    }

    fn ts_add<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, ts:TS, value: V) -> RedisResult<RV> {
        cmd("TS.ADD").arg(key).arg(ts).arg(value).query(self)
    }

    fn ts_add_now<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V) -> RedisResult<RV> {
        cmd("TS.ADD").arg(key).arg("*").arg(value).query(self)
    }

    fn ts_add_create<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, ts: Option<TS>, value: V, options: TsOptions) -> RedisResult<RV> {
        let mut c = cmd("TS.ADD");
        c.arg(key);
        if ts.is_some() {
            c.arg(ts.unwrap());
        } else {
            c.arg("*");
        }
        c.arg(value).arg(options).query(self)
    }

    fn ts_madd<K: ToRedisArgs, TS: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, values:&[(K, TS, V)]) -> RedisResult<RV> {
        cmd("TS.MADD").arg(values).query(self)
    }

    fn ts_incrby<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V) -> RedisResult<RV> {
        cmd("TS.INCRBY").arg(key).arg(value).query(self)
    }

    fn ts_incrby_ts<K: ToRedisArgs, V: ToRedisArgs, TS:ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V, ts:TS) -> RedisResult<RV> {
        cmd("TS.INCRBY").arg(key).arg(value).arg("TIMESTAMP").arg(ts).query(self)
    }

    fn ts_incrby_create<K: ToRedisArgs, V: ToRedisArgs, TS:ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V, ts:Option<TS>, options:TsOptions) -> RedisResult<RV> {
        let mut c = cmd("TS.INCRBY");
        c.arg(key).arg(value);
        if ts.is_some() {
            c.arg("TIMESTAMP").arg(ts.unwrap());
        }
        c.arg(options).query(self)
    }

    fn ts_decrby<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V) -> RedisResult<RV> {
        cmd("TS.DECRBY").arg(key).arg(value).query(self)
    }

    fn ts_decrby_ts<K: ToRedisArgs, V: ToRedisArgs, TS:ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V, ts:TS) -> RedisResult<RV> {
        cmd("TS.DECRBY").arg(key).arg(value).arg("TIMESTAMP").arg(ts).query(self)
    }

    fn ts_decrby_create<K: ToRedisArgs, V: ToRedisArgs, TS:ToRedisArgs, RV: FromRedisValue>(
        &mut self, key: K, value: V, ts:Option<TS>, options:TsOptions) -> RedisResult<RV> {
        let mut c = cmd("TS.DECRBY");
        c.arg(key).arg(value);
        if ts.is_some() {
            c.arg("TIMESTAMP").arg(ts.unwrap());
        }
        c.arg(options).query(self)
    }

    fn ts_createrule<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self, source_key: K, dest_key: K, aggregation_type:TsAggregationType) -> RedisResult<RV> {
        cmd("TS.CREATERULE").arg(source_key).arg(dest_key).arg(aggregation_type).query(self)
    }

    fn ts_deleterule<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self, source_key: K, dest_key: K) -> RedisResult<RV> {
        cmd("TS.DELETERULE").arg(source_key).arg(dest_key).query(self)
    }

    fn ts_range<K:ToRedisArgs, FTS:ToRedisArgs, TTS:ToRedisArgs, C:ToRedisArgs, TS:FromRedisValue, V:FromRedisValue>(
        &mut self, key:K, from_timestamp:FTS, to_timestamp:TTS, count:Option<C>,
        aggregation_type:Option<TsAggregationType>) -> RedisResult<Vec<(TS,V)>> {
        let mut c = cmd("TS.RANGE");
        c.arg(key).arg(from_timestamp).arg(to_timestamp);
        if count.is_some() {
            c.arg("COUNT").arg(count.unwrap());
        }
        c.arg(aggregation_type).query(self)
    }

    fn ts_mrange<K:ToRedisArgs, FTS:ToRedisArgs, TTS:ToRedisArgs, C:ToRedisArgs, TS: std::default::Default + FromRedisValue, V: std::default::Default + FromRedisValue>(
        &mut self, key:K, from_timestamp:FTS, to_timestamp:TTS, count:Option<C>, 
        aggregation_type:Option<TsAggregationType>, filter_options:TsFilterOptions) -> RedisResult<TsMrangeResult<TS,V>> {
        let mut c = cmd("TS.MRANGE");
        c.arg(key).arg(from_timestamp).arg(to_timestamp);
        if count.is_some() {
            c.arg("COUNT").arg(count.unwrap());
        }
        c.arg(aggregation_type).arg(filter_options);
        c.query(self)
    }

    fn ts_get<K:ToRedisArgs, TS: FromRedisValue, V:FromRedisValue>(&mut self, key:K) -> RedisResult<Option<(TS,V)>> {
        cmd("TS.GET").arg(key).query(self).or_else(|_| { Ok(None) })
    }

    fn ts_mget<K:ToRedisArgs, TS: std::default::Default +  FromRedisValue, V: std::default::Default + FromRedisValue>(
        &mut self, filter_options:TsFilterOptions) -> RedisResult<TsMgetResult<TS,V>> {
        cmd("TS.MGET").arg(filter_options).query(self)
    }

    fn ts_info<K:ToRedisArgs>(&mut self, key:K) -> RedisResult<TsInfo> {
        cmd("TS.INFO").arg(key).query(self)
    }

    fn ts_query_index(&mut self, filter_options:TsFilterOptions) -> RedisResult<Vec<String>> {
        cmd("TS.QUERYINDEX").arg(filter_options.with_labels(false)).query(self)
    }
}

impl<T> TsCommands for T where T: ConnectionLike {}
