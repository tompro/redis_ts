//! redis_ts proivdes a small trait with extension functions for the 
//! [redis](https://docs.rs/redis/0.16.0/redis) crate to allow 
//! working with redis time series data that can be installed as 
//! a [redis module](https://oss.redislabs.com/redistimeseries). Time 
//! series commands are available as synchronous and asynchronous versions.
//! 
//! The crate is called `redis_ts` and you can depend on it via cargo:
//!
//! ```ini
//! [dependencies.redis_ts]
//! version = "*"
//! ```
//!
//! Or via git:
//!
//! ```ini
//! [dependencies.redis_ts]
//! git = "https://github.com/tompro/redis_ts.git"
//! ```
//! 
//! 
//! # Synchronous usage
//! 
//! To enable redis time series commands you simply load the 
//! redis_ts::TsCommands into the scope. All redis time series 
//! commands will then be available on your redis connection.
//! 
//!  
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! use redis::Commands;
//! use redis_ts::{TsCommands, TsOptions};
//! 
//! let client = redis::Client::open("redis://127.0.0.1/")?;
//! let mut con = client.get_connection()?;
//! 
//! let _:() = con.ts_create("my_ts", TsOptions::default())?;
//! # Ok(()) }
//! ```
//! 
//! # Supported commands
//! 
//! The following examples work with the synchronous and asynchronous 
//! API. For simplicity all examples will use the synchronous API. To 
//! use them async simply run them whithin an async function and append 
//! the .await after the command call.
//! 
//! ## TS.CREATE
//! Creates new time series keys. TsOptions can help you build the time
//! series configuration you want to have.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let my_opts = TsOptions::default()
//!   .retention_time(60000)
//!   .uncompressed(false)
//!   .label("component", "engine")
//!   .label("sensor", "temperature");
//! 
//! let _:() = con.ts_create("my_engine", my_opts)?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.ALTER
//! Modifies existing time series keys. Note: You can not modify the uncompressed 
//! option of an existing time series so the flag will be ignored.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//! 
//! let _:() = con.ts_alter("my_engine", my_opts)?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.ADD
//! Add a value to time series. When providing time series options with 
//! the add command the series will be created if it does not yet exist.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_add("my_engine", 123456789, 36.1)?;
//! 
//! /// Auto redis timestamp
//! let now:u64 = con.ts_add_now("my_engine", 36.2)?;
//! 
//! /// Add with auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//! 
//! let create_ts:u64 = con.ts_add_create("my_engine", "*", 35.7, my_opts)?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.MADD
//! Add multiple values to one or multiple time series.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! let r:Vec<u64> = con.ts_madd(&[
//!   ("my_engine", 1234, 36.0), 
//!   ("other_engine", 4321, 33.9)
//! ])?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.INCRBY
//! Increment a time series value.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_incrby("my_engine", 123456789, 2)?;
//! 
//! /// Auto redis timestamp
//! let now:u64 = con.ts_incrby_now("my_engine", 7.0)?;
//! 
//! /// With auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//! 
//! let create_ts:u64 = con.ts_incrby_create("my_engine", "*", 16.97, my_opts)?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.DECRBY
//! Decrement a time series value.
//! 
//! ```rust,no_run
//! # fn run() -> redis::RedisResult<()> {
//! # use redis::Commands;
//! # use redis_ts::{TsCommands, TsOptions};
//! # let client = redis::Client::open("redis://127.0.0.1/")?;
//! # let mut con = client.get_connection()?;
//! /// With a timestamp
//! let ts:u64 = con.ts_decrby("my_engine", 123456789, 2)?;
//! 
//! /// Auto redis timestamp
//! let now:u64 = con.ts_decrby_now("my_engine", 7.0)?;
//! 
//! /// With auto create.
//! let my_opts = TsOptions::default()
//!   .retention_time(600000)
//!   .label("component", "spark_plug")
//!   .label("sensor", "temperature");
//! 
//! let create_ts:u64 = con.ts_decrby_create("my_engine", "*", 16.97, my_opts)?;
//! # Ok(()) }
//! ```
//! 
//! ## TS.CREATERULE
//! Create time series compaction rules.
//! 
//! ## TS.DELETERULE
//! Delete time series compaction rules.
//! 
//! ## TS.RANGE
//! Query for a range of time series data.
//! 
//! ## TS.MRANGE
//! Batch query multiple ranges of time series data.
//! 
//! ## TS.GET
//! Get the most recent value of a time series.
//! 
//! ## TS.MGET
//! Get the most recent value of multiple time series.
//! 
//! ## TS.INFO
//! Get information about a time series key.
//! 
//! ## TS.QUERYINDEX
//! Get the keys of time series filtered by given filter.
//! 
pub use crate::commands::TsCommands;

pub use crate::types::{
    TsOptions,
    TsFilterOptions,
    TsInfo,
    TsMgetResult,
    TsMrangeResult,
    TsAggregationType
};

mod commands;
mod types;
