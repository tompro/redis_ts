use redis::{
    from_redis_value, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value,
};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
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
    Twa(u64),
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
            TsAggregationType::Twa(v) => ("twa", v),
        };

        out.write_arg(b"AGGREGATION");
        out.write_arg(t.as_bytes());
        val.write_redis_args(out);
    }
}

///A time bucket alignment control for AGGREGATION. It controls the time bucket
/// timestamps by changing the reference timestamp on which a bucket is defined.
/// - Start: The reference timestamp will be the query start interval time.
/// - End: The reference timestamp will be the query end interval time.
/// - Ts(time): A specific timestamp: align the reference timestamp to a specific time.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TsAlign {
    Start,
    End,
    Ts(u64),
}

impl ToRedisArgs for TsAlign {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(b"ALIGN");
        match self {
            TsAlign::Start => out.write_arg(b"-"),
            TsAlign::End => out.write_arg(b"+"),
            TsAlign::Ts(v) => v.write_redis_args(out),
        }
    }
}

/// Bucket timestamp controls how bucket timestamps are reported.
/// - Low: the bucket's start time (default).
/// - High: the bucket's end time.
/// - Mid: the bucket's mid time (rounded down if not an integer).
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum TsBucketTimestamp {
    Low,
    High,
    Mid,
}

impl ToRedisArgs for TsBucketTimestamp {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(b"BUCKETTIMESTAMP");
        match self {
            TsBucketTimestamp::Low => out.write_arg(b"-"),
            TsBucketTimestamp::High => out.write_arg(b"+"),
            TsBucketTimestamp::Mid => out.write_arg(b"~"),
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub enum Integer {
    Usize(usize),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Isize(isize),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
}

impl ToRedisArgs for Integer {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Integer::Usize(v) => v.write_redis_args(out),
            Integer::U8(v) => v.write_redis_args(out),
            Integer::U16(v) => v.write_redis_args(out),
            Integer::U32(v) => v.write_redis_args(out),
            Integer::U64(v) => v.write_redis_args(out),
            Integer::Isize(v) => v.write_redis_args(out),
            Integer::I8(v) => v.write_redis_args(out),
            Integer::I16(v) => v.write_redis_args(out),
            Integer::I32(v) => v.write_redis_args(out),
            Integer::I64(v) => v.write_redis_args(out),
        }
    }
}

impl From<usize> for Integer {
    fn from(value: usize) -> Self {
        Integer::Usize(value)
    }
}

impl From<u8> for Integer {
    fn from(value: u8) -> Self {
        Integer::U8(value)
    }
}

impl From<u16> for Integer {
    fn from(value: u16) -> Self {
        Integer::U16(value)
    }
}

impl From<u32> for Integer {
    fn from(value: u32) -> Self {
        Integer::U32(value)
    }
}

impl From<u64> for Integer {
    fn from(value: u64) -> Self {
        Integer::U64(value)
    }
}

impl From<isize> for Integer {
    fn from(value: isize) -> Self {
        Integer::Isize(value)
    }
}

impl From<i8> for Integer {
    fn from(value: i8) -> Self {
        Integer::I8(value)
    }
}

impl From<i16> for Integer {
    fn from(value: i16) -> Self {
        Integer::I16(value)
    }
}

impl From<i32> for Integer {
    fn from(value: i32) -> Self {
        Integer::I32(value)
    }
}

impl From<i64> for Integer {
    fn from(value: i64) -> Self {
        Integer::I64(value)
    }
}

/// Let's you build a ts range query with all options via a builder pattern:
///
/// ```rust
/// use redis_ts::{TsAggregationType, TsBucketTimestamp, TsRangeQuery};
/// let query = TsRangeQuery::default()
///     .from(1234)
///     .to(5678)
///     .latest(true)
///     .filter_by_value(1.0, 5.0)
///     .aggregation_type(TsAggregationType::Avg(5000))
///     .bucket_timestamp(TsBucketTimestamp::High)
///     .empty(true);
/// ```
///
#[derive(Default, Debug, Clone)]
pub struct TsRangeQuery {
    from: Option<Integer>,
    to: Option<Integer>,
    latest: bool,
    filter_by_ts: Vec<Integer>,
    filter_by_value: Option<(f64, f64)>,
    count: Option<u64>,
    align: Option<TsAlign>,
    aggregation_type: Option<TsAggregationType>,
    bucket_timestamp: Option<TsBucketTimestamp>,
    empty: bool,
}

impl TsRangeQuery {
    /// Start timestamp of the series to query. Defaults to '-' (earliest sample)
    /// if left empty.
    pub fn from<T: Into<Integer>>(mut self, from: T) -> Self {
        self.from = Some(Into::into(from));
        self
    }

    /// End timestamp of the series to query. Defaults to '+' (latest sample)
    /// if left empty.
    pub fn to<T: Into<Integer>>(mut self, to: T) -> Self {
        self.to = Some(Into::into(to));
        self
    }

    /// Will enable the LATEST flag on the query.
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    /// Will enable the FILTER_BY_TS option with given timestamps. Will only
    /// be added if the given Vec contains any ts values.
    pub fn filter_by_ts<T: Into<Integer>>(mut self, ts: Vec<T>) -> Self {
        self.filter_by_ts = ts.into_iter().map(|v| Into::into(v)).collect();
        self
    }

    /// Will enable the FILTER_BY_VALUE option with given min and max values.
    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    /// Determines the max amount of returned samples.
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }

    /// Controls the aggregation alignment. Will only be added if the query actually
    /// contains aggregation params.
    pub fn align(mut self, align: TsAlign) -> Self {
        self.align = Some(align);
        self
    }

    /// The type of aggregation to be performed on the series.
    pub fn aggregation_type(mut self, aggregation_type: TsAggregationType) -> Self {
        self.aggregation_type = Some(aggregation_type);
        self
    }

    /// Controls reporting of aggregation bucket timestamps. Will only be added if the
    /// query actually contains aggregation params.
    pub fn bucket_timestamp(mut self, bucket_timestamp: TsBucketTimestamp) -> Self {
        self.bucket_timestamp = Some(bucket_timestamp);
        self
    }

    /// Enables the EMPTY flag on the query.
    pub fn empty(mut self, empty: bool) -> Self {
        self.empty = empty;
        self
    }
}

impl ToRedisArgs for TsRangeQuery {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref from) = self.from {
            from.write_redis_args(out);
        } else {
            out.write_arg(b"-");
        }

        if let Some(ref to) = self.to {
            to.write_redis_args(out);
        } else {
            out.write_arg(b"+");
        }

        if self.latest {
            out.write_arg(b"LATEST");
        }

        if !self.filter_by_ts.is_empty() {
            out.write_arg(b"FILTER_BY_TS");
            for ts in self.filter_by_ts.iter() {
                ts.write_redis_args(out);
            }
        }

        if let Some((min, max)) = self.filter_by_value {
            out.write_arg(b"FILTER_BY_VALUE");
            min.write_redis_args(out);
            max.write_redis_args(out);
        }

        if let Some(count) = self.count {
            out.write_arg(b"COUNT");
            count.write_redis_args(out);
        }

        if let Some(ref agg) = self.aggregation_type {
            if let Some(ref align) = self.align {
                align.write_redis_args(out);
            }

            agg.write_redis_args(out);

            if let Some(ref bkt_ts) = self.bucket_timestamp {
                bkt_ts.write_redis_args(out);
            }

            if self.empty {
                out.write_arg(b"EMPTY")
            }
        }
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
            out.write_arg(format!("{rt}").as_bytes());
        }

        if self.uncompressed {
            out.write_arg(b"UNCOMPRESSED");
        }

        if let Some(ref policy) = self.duplicate_policy {
            policy.write_redis_args(out);
        }

        if let Some(ref alloc) = self.chunk_size {
            out.write_arg(b"CHUNK_SIZE");
            out.write_arg(format!("{alloc}").as_bytes());
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
    pub fn equals<L: Display + ToRedisArgs, V: Display + ToRedisArgs>(
        mut self,
        name: L,
        value: V,
    ) -> Self {
        self.filters.push(TsFilter {
            name: format!("{name}"),
            value: format!("{value}"),
            compare: TsCompare::Eq,
        });
        self
    }

    /// Select time series where given label does not contain the given value.
    pub fn not_equals<L: Display + ToRedisArgs, V: Display + ToRedisArgs>(
        mut self,
        name: L,
        value: V,
    ) -> Self {
        self.filters.push(TsFilter {
            name: format!("{name}"),
            value: format!("{value}"),
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select time series where given label contains any of the given values.
    pub fn in_set<L: Display + ToRedisArgs, V: Display + ToRedisArgs>(
        mut self,
        name: L,
        values: Vec<V>,
    ) -> Self {
        let set = format!(
            "({})",
            values
                .iter()
                .map(|v| { format!("{v}") })
                .collect::<Vec<String>>()
                .join(",")
        );
        self.filters.push(TsFilter {
            name: format!("{name}"),
            value: set,
            compare: TsCompare::Eq,
        });
        self
    }

    /// Select time series where given label does not contain any of the given values.
    pub fn not_in_set<L: Display + ToRedisArgs, V: Display + ToRedisArgs>(
        mut self,
        name: L,
        values: Vec<V>,
    ) -> Self {
        let set = format!(
            "({})",
            values
                .iter()
                .map(|v| { format!("{v}") })
                .collect::<Vec<String>>()
                .join(",")
        );
        self.filters.push(TsFilter {
            name: format!("{name}"),
            value: set,
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select all time series that have the given label.
    pub fn has_label<L: Display + ToRedisArgs>(mut self, name: L) -> Self {
        self.filters.push(TsFilter {
            name: format!("{name}"),
            value: "".to_string(),
            compare: TsCompare::NotEq,
        });
        self
    }

    /// Select all time series that do not have the given label.
    pub fn not_has_label<L: Display + ToRedisArgs>(mut self, name: L) -> Self {
        self.filters.push(TsFilter {
            name: format!("{name}"),
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

impl<TS: Default + FromRedisValue, V: Default + FromRedisValue> FromRedisValue for TsMget<TS, V> {
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

impl<TS: Default + FromRedisValue, V: Default + FromRedisValue> FromRedisValue
    for TsMgetEntry<TS, V>
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

impl<TS: Default + FromRedisValue + Copy, V: Default + FromRedisValue + Copy> FromRedisValue
    for TsMrange<TS, V>
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

impl<TS: Default + FromRedisValue + Copy, V: Default + FromRedisValue + Copy> FromRedisValue
    for TsMrangeEntry<TS, V>
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
