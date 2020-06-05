use std::collections::HashMap;
use redis::{ToRedisArgs, RedisWrite, Value, FromRedisValue, RedisResult, from_redis_value, RedisError};
use std::str;

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum TsAggregationType {
    Avg(u64),
    Sum(u64),
    Min(u64),
    Max(u64),
    Range(u64),
    Count(u64),
    First(u64),
    Last(u64),
    StdP(u64),
    StdS(u64),
    VarP(u64),
    VarS(u64),
}

impl ToRedisArgs for TsAggregationType {
    fn write_redis_args<W>(&self, out: &mut W) where
        W: ?Sized + RedisWrite {
        let (t, val) = match *self {
            TsAggregationType::Avg(v) => ("avg", v),
            TsAggregationType::Sum(v) => ("sum", v),
            TsAggregationType::Min(v) => ("min", v),
            TsAggregationType::Max(v) => ("max", v),
            TsAggregationType::Range(v) => ("range", v),
            TsAggregationType::Count(v) => ("count", v),
            TsAggregationType::First(v) => ("first", v),
            TsAggregationType::Last(v) => ("last", v),
            TsAggregationType::StdP(v) => ("std.p", v),
            TsAggregationType::StdS(v) => ("std.s", v),
            TsAggregationType::VarP(v) => ("var.p", v),
            TsAggregationType::VarS(v) => ("var.s", v),
        };

        out.write_arg("AGGREGATION".as_bytes());
        out.write_arg(t.as_bytes());
        val.write_redis_args(out);

    }
}

#[derive(Default, Debug, Clone)]
pub struct TsOptions {
    retention_time:Option<u64>,
    uncompressed:Option<bool>,
    labels:Option<Vec<Vec<u8>>>
}

impl TsOptions {

    pub fn retention_time(mut self, time:u64) -> Self {
        self.retention_time = Some(time);
        self
    }

    pub fn uncompressed(mut self, value:bool) -> Self {
        self.uncompressed = Some(value);
        self
    }

    pub fn labels(mut self, labels:Vec<(&str, &str)>) -> Self {
        self.labels = Some(ToRedisArgs::to_redis_args(&labels));
        self
    }

    pub fn label(mut self, name:&str, value:&str) -> Self {
        let mut l = ToRedisArgs::to_redis_args(&vec![(name, value)]);
        let mut res:Vec<Vec<u8>> = vec![];
        let cur = self.labels;
        if cur.is_some() {
            res.append(&mut cur.unwrap());
        }
        res.append(&mut l);
        self.labels = Some(res);
        self
    }

    pub fn clear_uncompressed(mut self) -> Self {
        self.uncompressed = None;
        self
    }

}

impl ToRedisArgs for TsOptions {
    fn write_redis_args<W>(&self, out: &mut W) where
        W: ?Sized + RedisWrite {

        if let Some(ref rt) = self.retention_time {
            out.write_arg("RETENTION".as_bytes());
            out.write_arg(format!("{}", rt).as_bytes());
        }

        if let Some(uc) = self.uncompressed {
            if uc {
                out.write_arg("UNCOMPRESSED".as_bytes());
            }
        }

        if let Some(ref l) = self.labels {
            out.write_arg("LABELS".as_bytes());
            for arg in l {
                out.write_arg(&arg);
            }
        }

    }
}

#[derive(Default, Debug, Clone)]
pub struct TsValue<TS, V> {
    ts: TS,
    value: V
}

#[derive(Default, Debug, Clone)]
pub struct TsOptionValue<TS, V> {
    value:Option<TsValue<TS,V>>
}

impl<TS:FromRedisValue,V:FromRedisValue> FromRedisValue for TsValue<TS,V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
      match *v {
          Value::Bulk(ref values) if values.len() == 2 => {
              Ok(TsValue {
                  ts: from_redis_value(&values[0])?,
                  value: from_redis_value(&values[1])?
              })
          },
          _ => Err(RedisError::from(
              std::io::Error::new(std::io::ErrorKind::Other, "no_ts_data"),
          ))
      }
    }
}

impl<TS:FromRedisValue,V:FromRedisValue> FromRedisValue for TsOptionValue<TS,V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) if values.len() == 2 => {
                Ok(TsOptionValue{
                    value: Some(TsValue {
                    ts: FromRedisValue::from_redis_value(&values[0])?,
                    value: FromRedisValue::from_redis_value(&values[1])?
                })})
            },
            Value::Bulk(_) => Ok(TsOptionValue{value: None}),
            _ => Err(RedisError::from(
                std::io::Error::new(std::io::ErrorKind::Other, "invalid_ts_response"),
            ))
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum TsCompare {
    Eq,
    NotEq,
}

impl ToRedisArgs for TsCompare {
    fn write_redis_args<W>(&self, out: &mut W) where
        W: ?Sized + RedisWrite {
        let val = match *self {
            TsCompare::Eq => "=",
            TsCompare::NotEq => "!=",
        };

        val.write_redis_args(out);
    }
}

#[derive(Debug, Default)]
pub struct TsFilterOptions {
    with_labels:bool,
    filters:Vec<TsFilter>,
}

impl TsFilterOptions {
    
    pub fn with_labels(mut self, value:bool) -> Self {
        self.with_labels = value;
        self
    }

    pub fn equals<L: ToRedisArgs, V:ToRedisArgs>(mut self, name:L, value:V) -> Self {
        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: value.to_redis_args(),
            compare: TsCompare::Eq

        });
        self
    }

    pub fn not_equals<L: ToRedisArgs, V:ToRedisArgs>(mut self, name:L, value:V) -> Self {
        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: value.to_redis_args(),
            compare: TsCompare::NotEq

        });
        self
    }

    pub fn in_set<L: ToRedisArgs>(mut self, name:L, values:Vec<&str>) -> Self {
        let set = format!("({:?})", values.join(","));
        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: set.to_redis_args(),
            compare: TsCompare::Eq
        });
        self
    }

    pub fn not_in_set<L: ToRedisArgs>(mut self, name:L, values:Vec<&str>) -> Self {
        let set = format!("({:?})", values.join(","));

        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: set.to_redis_args(),
            compare: TsCompare::NotEq
        });
        self
    }

    pub fn has_label<L: ToRedisArgs>(mut self, name:L) -> Self {
        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: vec![vec![]],
            compare: TsCompare::NotEq
        });
        self
    }

    pub fn not_has_label<L: ToRedisArgs>(mut self, name:L) -> Self {
        self.filters.push(TsFilter {
            name: name.to_redis_args(),
            value: vec![],
            compare: TsCompare::Eq
        });
        self
    }

}

impl ToRedisArgs for TsFilterOptions {
    fn write_redis_args<W>(&self, out: &mut W) where
        W: ?Sized + RedisWrite {

        if self.with_labels {
            out.write_arg("WITHLABELS".as_bytes());
        }
        out.write_arg("FILTER".as_bytes());

        for f in self.filters.iter() {
            f.write_redis_args(out);
        }
    }
}


#[derive(Debug)]
pub struct TsFilter {
    name:Vec<Vec<u8>>,
    value:Vec<Vec<u8>>,
    compare:TsCompare
}

impl ToRedisArgs for TsFilter {

    fn write_redis_args<W>(&self, out: &mut W) where
        W: ?Sized + RedisWrite {

        let mut res:Vec<u8> = vec![];
        for v in self.name[0].iter() {
            res.push(*v);
        }
        for v in self.compare.to_redis_args()[0].iter() {
            res.push(*v);
        }
        for v in self.value[0].iter() {
            res.push(*v);
        }
        out.write_arg(&res);
    }

}


#[derive(Debug,Default)]
pub struct TsInfo {
    pub total_samples: u64,
    pub memory_usage: u64,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub retention_time: u64,
    pub chunk_count: u64,
    pub max_samples_per_chunk: u16,
    pub labels: Vec<(String,String)>,
    pub source_key: Option<String>,
    pub rules: Vec<(String,u64,String)>,
}

impl FromRedisValue for TsInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
      match *v {
          Value::Bulk(ref values) => {
            let mut result = TsInfo::default();
            let mut map:HashMap<String,Value> = HashMap::new();
            for pair in values.chunks(2) {
                map.insert(
                    from_redis_value(&pair[0])?,
                    pair[1].clone()
                );
            }

            if let Some(v) = map.get("totalSamples") {
                result.total_samples = from_redis_value(v)?;
            }

            if let Some(v) = map.get("memoryUsage") {
                result.memory_usage = from_redis_value(v)?;
            }

            if let Some(v) = map.get("firstTimestamp") {
                result.first_timestamp = from_redis_value(v)?;
            }

            if let Some(v) = map.get("lastTimestamp") {
                result.last_timestamp = from_redis_value(v)?;
            }

            if let Some(v) = map.get("retentionTime") {
                result.retention_time = from_redis_value(v)?;
            }

            if let Some(v) = map.get("chunkCount") {
                result.chunk_count = from_redis_value(v)?;
            }

            if let Some(v) = map.get("maxSamplesPerChunk") {
                result.max_samples_per_chunk = from_redis_value(v)?;
            }

            if let Some(v) = map.get("sourceKey") {
                result.source_key = from_redis_value(v)?;
            }

            result.rules = match map.get("rules") {
                Some(Value::Bulk(ref values)) => {
                    values.iter().flat_map(|value| {
                        match value {
                            Value::Bulk(ref vs) => Some((
                                    from_redis_value(&vs[0]).unwrap(),
                                    from_redis_value(&vs[1]).unwrap(),
                                    from_redis_value(&vs[2]).unwrap(),
                                )),
                            _ => None,
                        }
                    }).collect()
                },
                _ => vec![]
            };

            result.labels = match map.get("labels") {
                Some(Value::Bulk(ref values)) => {
                    values.iter().flat_map(|value| {
                        match value {
                            Value::Bulk(ref vs) => Some((
                                    from_redis_value(&vs[0]).unwrap(),
                                    from_redis_value(&vs[1]).unwrap(),
                                )),
                            _ => None,
                        }
                    }).collect()
                },
                _ => vec![]
            };

            Ok(result)
          },
          _ => Err(RedisError::from(
              std::io::Error::new(std::io::ErrorKind::Other, "no_ts_info_data"),
          ))
      }
    }
}

#[derive(Debug,Default)]
pub struct TsMgetResult<TS: FromRedisValue,V: FromRedisValue> {
    pub key: String,
    pub labels: Vec<(String,String)>,
    pub value: Option<(TS,V)>
}

impl <TS: std::default::Default + FromRedisValue,V: std::default::Default + FromRedisValue> FromRedisValue for TsMgetResult<TS,V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
      match *v {
          Value::Bulk(_) => {
              Ok(TsMgetResult::default())
          },
          _ => Err(RedisError::from(
              std::io::Error::new(std::io::ErrorKind::Other, "no_mget_data"),
          ))
      }
    }
}


#[derive(Debug,Default)]
pub struct TsMrangeResult<TS: FromRedisValue,V: FromRedisValue> {
    key: String,
    labels: Vec<(String,String)>,
    value: Vec<(TS,V)>
}

impl <TS: std::default::Default + FromRedisValue,V: std::default::Default + FromRedisValue> FromRedisValue for TsMrangeResult<TS,V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
      match *v {
          Value::Bulk(_) => {
              Ok(TsMrangeResult::default())
          },
          _ => Err(RedisError::from(
              std::io::Error::new(std::io::ErrorKind::Other, "no_mget_data"),
          ))
      }
    }
}