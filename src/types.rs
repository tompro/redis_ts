use redis::{
    from_redis_value, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value,
};
use std::collections::HashMap;
use std::str;

/// Allows you to specify a redis time series aggreation with a time
/// bucket.
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
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
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

        out.write_arg(b"AGGREGATION");
        out.write_arg(t.as_bytes());
        val.write_redis_args(out);
    }
}

/// Different options for handling inserts of duplicate values. Block
/// is the behaviour redis time series was using before preventing all
/// inserts of values older or equal to latest value in series. Fist
/// will simply ignore the new value (as opposed to returning an error),
/// Last will use the new value, Min the lower and Max the higher value.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum TsDuplicatePolicy {
    Block,
    First,
    Last,
    Min,
    Max,
    Other(String),
}

impl ToRedisArgs for TsDuplicatePolicy {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let policy = match self {
            TsDuplicatePolicy::Block => "BLOCK",
            TsDuplicatePolicy::First => "FIRST",
            TsDuplicatePolicy::Last => "LAST",
            TsDuplicatePolicy::Min => "MIN",
            TsDuplicatePolicy::Max => "MAX",
            TsDuplicatePolicy::Other(v) => v.as_str(),
        };
        out.write_arg(b"DUPLICATE_POLICY");
        out.write_arg(policy.as_bytes());
    }
}

impl FromRedisValue for TsDuplicatePolicy {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let string: String = from_redis_value(v)?;
        let res = match string.as_str() {
            "block" => TsDuplicatePolicy::Block,
            "first" => TsDuplicatePolicy::First,
            "last" => TsDuplicatePolicy::Last,
            "min" => TsDuplicatePolicy::Min,
            "max" => TsDuplicatePolicy::Max,
            v => TsDuplicatePolicy::Other(v.to_string()),
        };
        Ok(res)
    }
}

/// Options for a redis time series key. Can be used in multiple redis
/// time series calls (CREATE, ALTER, ADD, ...). The uncompressed option
/// will only be respected in a TS.CREATE.
#[derive(Default, Debug, Clone)]
pub struct TsOptions {
    retention_time: Option<u64>,
    uncompressed: bool,
    labels: Option<Vec<Vec<u8>>>,
    duplicate_policy: Option<TsDuplicatePolicy>,
    chunk_size: Option<u64>,
}

/// TsOptions allows you to build up your redis time series configuration. It
/// supports default and a builder pattern so you can use it the following way:
///
/// ```rust
/// use redis_ts::TsOptions;
/// use redis_ts::TsDuplicatePolicy;
///
/// let opts:TsOptions = TsOptions::default()
///     .retention_time(60000)
///     .uncompressed(false)
///     .chunk_size(16000)
///     .duplicate_policy(TsDuplicatePolicy::Last)
///     .label("label_1", "value_1")
///     .label("label_2", "value_2");
/// ```
///
impl TsOptions {
    /// Specifies the retention time in millis for this time series options.
    pub fn retention_time(mut self, time: u64) -> Self {
        self.retention_time = Some(time);
        self
    }

    /// Switches this time series into uncompressed mode. Note that
    /// redis ts only respects this flag in TS.CREATE. All other options
    /// usages will ignore this flag.
    pub fn uncompressed(mut self, value: bool) -> Self {
        self.uncompressed = value;
        self
    }

    /// Resets all labels to the items in given labels. All labels that
    /// where previously present will be removed. If the labels are empty
    /// no labels will be used for the time series.
    pub fn labels(mut self, labels: Vec<(&str, &str)>) -> Self {
        if !labels.is_empty() {
            self.labels = Some(ToRedisArgs::to_redis_args(&labels));
        } else {
            self.labels = None;
        }
        self
    }

    /// Adds a single label to this time series options.
    pub fn label(mut self, name: &str, value: &str) -> Self {
        let mut l = ToRedisArgs::to_redis_args(&vec![(name, value)]);
        let mut res: Vec<Vec<u8>> = vec![];
        if let Some(mut cur) = self.labels {
            res.append(&mut cur);
        }
        res.append(&mut l);
        self.labels = Some(res);
        self
    }

    /// Overrides the servers default dplicatePoliciy.
    pub fn duplicate_policy(mut self, policy: TsDuplicatePolicy) -> Self {
        self.duplicate_policy = Some(policy);
        self
    }

    /// Sets the allocation size for data in bytes.
    pub fn chunk_size(mut self, size: u64) -> Self {
        self.chunk_size = Some(size);
        self
    }
}

impl ToRedisArgs for TsOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref rt) = self.retention_time {
            out.write_arg(b"RETENTION");
            out.write_arg(format!("{}", rt).as_bytes());
        }

        if self.uncompressed {
            out.write_arg(b"UNCOMPRESSED");
        }

        if let Some(ref policy) = self.duplicate_policy {
            policy.write_redis_args(out);
        }

        if let Some(ref alloc) = self.chunk_size {
            out.write_arg(b"CHUNK_SIZE");
            out.write_arg(format!("{}", alloc).as_bytes());
        }

        if let Some(ref l) = self.labels {
            out.write_arg(b"LABELS");
            for arg in l {
                out.write_arg(arg);
            }
        }
    }
}

/// Let's you build redis time series filter query options via a builder pattern. Filters
/// can be used in different commands like TS.MGET, TS.MRANGE and TS.QUERYINDEX.
#[derive(Debug, Default, Clone)]
pub struct TsFilterOptions {
    with_labels: bool,
    filters: Vec<TsFilter>,
}

/// TsFilterOptions allows you to build up your redis time series filter query. It
/// supports default and a builder pattern so you can use it the following way:
///
/// ```rust
/// use redis_ts::TsFilterOptions;
///
/// let filters = TsFilterOptions::default()
///     .with_labels(true)
///     .equals("label_1", "value_1")
///     .not_equals("label_2", "hello")
///     .in_set("label_3", vec!["a", "b", "c"])
///     .not_in_set("label_3", vec!["d", "e"])
///     .has_label("some_other")
///     .not_has_label("unwanted");
/// ```
///
impl TsFilterOptions {
    /// Will add the WITHLABELS flag to the filter query. The query response will have
    /// label information attached.
    pub fn with_labels(mut self, value: bool) -> Self {
        self.with_labels = value;
        self
    }

    /// Select time series where the given label contains the the given value.
    pub fn equals<L: std::fmt::Display + ToRedisArgs, V: std::fmt::Display + ToRedisArgs>(
        mut self,
        name: L,
        value: V,
    ) -> Self {
        self.filters.push(TsFilter {
            name: format!("{}", name),
            value: format!("{}", value),
            compare: TsCompare::Eq,
        });
        self
    }

    /// Select time series where given label does not contain the given value.
    pub fn not_equals<L: std::fmt::Debug + ToRedisArgs, V: std::fmt::Debug + ToRedisArgs>(
        mut self,
        name: L,
        value: V,
    ) -> Self {
        self.filters.push(TsFilter {
            name: format!("{:?}", name),
            value: format!("{:?}", value),
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select time series where given label contains any of the given values.
    pub fn in_set<L: std::fmt::Debug + ToRedisArgs, V: std::fmt::Debug + ToRedisArgs>(
        mut self,
        name: L,
        values: Vec<V>,
    ) -> Self {
        let set = format!(
            "({:?})",
            values
                .iter()
                .map(|v| { format!("{:?}", v) })
                .collect::<Vec<String>>()
                .join(",")
        );
        self.filters.push(TsFilter {
            name: format!("{:?}", name),
            value: set,
            compare: TsCompare::Eq,
        });
        self
    }

    /// Select time series where given label does not contain any of the given values.
    pub fn not_in_set<L: std::fmt::Debug + ToRedisArgs, V: std::fmt::Debug + ToRedisArgs>(
        mut self,
        name: L,
        values: Vec<V>,
    ) -> Self {
        let set = format!(
            "({:?})",
            values
                .iter()
                .map(|v| { format!("{:?}", v) })
                .collect::<Vec<String>>()
                .join(",")
        );
        self.filters.push(TsFilter {
            name: format!("{:?}", name),
            value: set,
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select all time series that have the given label.
    pub fn has_label<L: std::fmt::Debug + ToRedisArgs>(mut self, name: L) -> Self {
        self.filters.push(TsFilter {
            name: format!("{:?}", name),
            value: "".to_string(),
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select all time series that do not have the given label.
    pub fn not_has_label<L: std::fmt::Debug + ToRedisArgs>(mut self, name: L) -> Self {
        self.filters.push(TsFilter {
            name: format!("{:?}", name),
            value: "".to_string(),
            compare: TsCompare::Eq,
        });
        self
    }

    pub fn get_filters(self) -> Vec<TsFilter> {
        self.filters
    }
}

impl ToRedisArgs for TsFilterOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.with_labels {
            out.write_arg(b"WITHLABELS");
        }
        out.write_arg(b"FILTER");

        for f in self.filters.iter() {
            f.write_redis_args(out)
        }
    }
}

/// Provides information about a redis time series key.
#[derive(Debug, Default)]
pub struct TsInfo {
    pub total_samples: u64,
    pub memory_usage: u64,
    pub first_timestamp: u64,
    pub last_timestamp: u64,
    pub retention_time: u64,
    pub chunk_count: u64,
    pub max_samples_per_chunk: u16,
    pub chunk_size: u64,
    pub duplicate_policy: Option<TsDuplicatePolicy>,
    pub labels: Vec<(String, String)>,
    pub source_key: Option<String>,
    pub rules: Vec<(String, u64, String)>,
}

impl FromRedisValue for TsInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) => {
                let mut result = TsInfo::default();
                let mut map: HashMap<String, Value> = HashMap::new();

                for pair in values.chunks(2) {
                    map.insert(from_redis_value(&pair[0])?, pair[1].clone());
                }

                //println!("{:?}", map);

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

                if let Some(v) = map.get("chunkSize") {
                    result.chunk_size = from_redis_value(v)?;
                }

                if let Some(v) = map.get("sourceKey") {
                    result.source_key = from_redis_value(v)?;
                }

                if let Some(v) = map.get("duplicatePolicy") {
                    result.duplicate_policy = from_redis_value(v)?;
                }

                result.rules = match map.get("rules") {
                    Some(Value::Bulk(ref values)) => values
                        .iter()
                        .flat_map(|value| match value {
                            Value::Bulk(ref vs) => Some((
                                from_redis_value(&vs[0]).unwrap(),
                                from_redis_value(&vs[1]).unwrap(),
                                from_redis_value(&vs[2]).unwrap(),
                            )),
                            _ => None,
                        })
                        .collect(),
                    _ => vec![],
                };

                result.labels = match map.get("labels") {
                    Some(Value::Bulk(ref values)) => values
                        .iter()
                        .flat_map(|value| match value {
                            Value::Bulk(ref vs) => Some((
                                from_redis_value(&vs[0]).unwrap(),
                                from_redis_value(&vs[1]).unwrap(),
                            )),
                            _ => None,
                        })
                        .collect(),
                    _ => vec![],
                };

                Ok(result)
            }
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no_ts_info_data",
            ))),
        }
    }
}

/// Represents a TS.MGET redis time series result. The concrete types for timestamp
/// and value eg <u64,f64> can be provided from the call site.
#[derive(Debug)]
pub struct TsMget<TS: FromRedisValue, V: FromRedisValue> {
    pub values: Vec<TsMgetEntry<TS, V>>,
}

impl<TS: std::default::Default + FromRedisValue, V: std::default::Default + FromRedisValue>
    FromRedisValue for TsMget<TS, V>
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let res = match *v {
            Value::Bulk(ref values) => TsMget {
                values: FromRedisValue::from_redis_values(values)?,
            },
            _ => TsMget { values: vec![] },
        };
        Ok(res)
    }
}

/// Represents a TS.MGET redis time series entry. The concrete types for timestamp
/// and value eg <u64,f64> can be provided from the call site.
#[derive(Debug, Default)]
pub struct TsMgetEntry<TS: FromRedisValue, V: FromRedisValue> {
    pub key: String,
    pub labels: Vec<(String, String)>,
    pub value: Option<(TS, V)>,
}

impl<TS: std::default::Default + FromRedisValue, V: std::default::Default + FromRedisValue>
    FromRedisValue for TsMgetEntry<TS, V>
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) => {
                let result = TsMgetEntry::<TS, V> {
                    key: from_redis_value(&values[0])?,
                    labels: match values[1] {
                        Value::Bulk(ref vs) => vs
                            .iter()
                            .flat_map(|value| match value {
                                Value::Bulk(ref v) => Some((
                                    from_redis_value(&v[0]).unwrap(),
                                    from_redis_value(&v[1]).unwrap(),
                                )),
                                _ => None,
                            })
                            .collect(),
                        _ => vec![],
                    },
                    value: match values[2] {
                        Value::Bulk(ref vs) if !vs.is_empty() => Some((
                            from_redis_value(&vs[0]).unwrap(),
                            from_redis_value(&vs[1]).unwrap(),
                        )),
                        _ => None,
                    },
                };

                Ok(result)
            }
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no_mget_data",
            ))),
        }
    }
}

/// Represents a TS.RANGE redis time series result. The concrete types for timestamp
/// and value eg <u64,f64> can be provided from the call site.
#[derive(Debug)]
pub struct TsRange<TS: FromRedisValue + Copy, V: FromRedisValue + Copy> {
    pub values: Vec<(TS, V)>,
}

impl<TS: FromRedisValue + Copy, V: FromRedisValue + Copy> FromRedisValue for TsRange<TS, V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) => {
                let items: Vec<TsValueReply<TS, V>> = FromRedisValue::from_redis_values(values)?;
                Ok(TsRange {
                    values: items.iter().map(|i| (i.ts, i.value)).collect(),
                })
            }
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no_range_data",
            ))),
        }
    }
}

/// Represents a TS.MRANGE redis time series result with multiple entries. The concrete types for timestamp
/// and value eg <u64,f64> can be provided from the call site.
#[derive(Debug)]
pub struct TsMrange<TS: FromRedisValue + Copy, V: FromRedisValue + Copy> {
    pub values: Vec<TsMrangeEntry<TS, V>>,
}

impl<
        TS: std::default::Default + FromRedisValue + Copy,
        V: std::default::Default + FromRedisValue + Copy,
    > FromRedisValue for TsMrange<TS, V>
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let res = match *v {
            Value::Bulk(ref values) => TsMrange {
                values: FromRedisValue::from_redis_values(values)?,
            },
            _ => TsMrange { values: vec![] },
        };
        Ok(res)
    }
}

/// Represents a TS.MRANGE redis time series value. The concrete types for timestamp
/// and value eg <u64,f64> can be provided from the call site.
#[derive(Debug, Default)]
pub struct TsMrangeEntry<TS: FromRedisValue + Copy, V: FromRedisValue + Copy> {
    pub key: String,
    pub labels: Vec<(String, String)>,
    pub values: Vec<(TS, V)>,
}

impl<
        TS: std::default::Default + FromRedisValue + Copy,
        V: std::default::Default + FromRedisValue + Copy,
    > FromRedisValue for TsMrangeEntry<TS, V>
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) => {
                let result = TsMrangeEntry::<TS, V> {
                    key: from_redis_value(&values[0]).unwrap(),
                    labels: match values[1] {
                        Value::Bulk(ref vs) => vs
                            .iter()
                            .flat_map(|value| match value {
                                Value::Bulk(ref v) => Some((
                                    from_redis_value(&v[0]).unwrap(),
                                    from_redis_value(&v[1]).unwrap(),
                                )),
                                _ => None,
                            })
                            .collect(),
                        _ => vec![],
                    },
                    values: match values[2] {
                        Value::Bulk(ref vs) => {
                            let items: Vec<TsValueReply<TS, V>> =
                                FromRedisValue::from_redis_values(vs)?;
                            items.iter().map(|i| (i.ts, i.value)).collect()
                        }
                        _ => vec![],
                    },
                };

                Ok(result)
            }
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no_mget_data",
            ))),
        }
    }
}

#[derive(Debug)]
struct TsValueReply<TS: FromRedisValue, V: FromRedisValue> {
    pub ts: TS,
    pub value: V,
}

impl<TS: FromRedisValue, V: FromRedisValue> FromRedisValue for TsValueReply<TS, V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match *v {
            Value::Bulk(ref values) => Ok(TsValueReply {
                ts: from_redis_value(&values[0]).unwrap(),
                value: from_redis_value(&values[1]).unwrap(),
            }),
            _ => Err(RedisError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "no_value_data",
            ))),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
enum TsCompare {
    Eq,
    NotEq,
}

impl ToRedisArgs for TsCompare {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let val = match *self {
            TsCompare::Eq => "=",
            TsCompare::NotEq => "!=",
        };

        val.write_redis_args(out);
    }
}

#[derive(Debug, Clone)]
pub struct TsFilter {
    name: String,
    value: String,
    compare: TsCompare,
}

impl ToRedisArgs for TsFilter {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let comp = match self.compare {
            TsCompare::Eq => "=",
            TsCompare::NotEq => "!=",
        };

        let arg = format!("{}{}{}", self.name, comp, self.value);
        out.write_arg(arg.as_bytes());
    }
}
