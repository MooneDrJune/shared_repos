use chrono::NaiveDateTime;
use polars::datatypes::AnyValue;
use polars::frame::row::Row;
use polars::prelude::NamedFrom;
use polars::prelude::SerReader;
use polars::prelude::{
    DataFrame, DataType, Field, JsonFormat, JsonReader, PolarsError, Schema, Series,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::Path;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<HashMap<String, QuoteData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_type: Option<Exception>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quotes {
    #[serde(flatten)]
    pub instruments: HashMap<String, QuotesData>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotesData {
    pub instrument_token: u64,
    pub timestamp: String,
    pub last_trade_time: String,
    pub last_price: f64,
    pub last_quantity: u64,
    pub buy_quantity: u64,
    pub sell_quantity: u64,
    pub volume: u64,
    pub average_price: f64,
    pub oi: u64,
    pub oi_day_high: u64,
    pub oi_day_low: u64,
    pub net_change: f64,
    pub lower_circuit_limit: f64,
    pub upper_circuit_limit: f64,
    pub ohlc: OhlcInner,
    pub depth: Depth,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteData {
    pub instrument_token: u64,
    #[serde(
        with = "optional_naive_date_time_from_str",
        skip_serializing_if = "Option::is_none"
    )]
    pub timestamp: Option<NaiveDateTime>,
    #[serde(
        with = "optional_naive_date_time_from_str",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_trade_time: Option<NaiveDateTime>,
    pub last_price: f64,
    pub last_quantity: i64,
    pub buy_quantity: u64,
    pub sell_quantity: u64,
    pub volume: u64,
    pub average_price: f64,
    pub oi: u64,
    pub oi_day_high: u64,
    pub oi_day_low: u64,
    pub net_change: f64,
    pub lower_circuit_limit: f64,
    pub upper_circuit_limit: f64,
    pub ohlc: OhlcInner,
    pub depth: Depth,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Depth {
    pub buy: Vec<OrderDepth>,
    pub sell: Vec<OrderDepth>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderDepth {
    pub price: f64,
    pub quantity: u64,
    pub orders: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OhlcInner {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Exception {
    TokenException,
    UserException,
    OrderException,
    InputException,
    NetworkException,
    DataException,
    GeneralException,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    #[default]
    Success,
    Error,
    Failed,
}

pub fn read_json_from_file<P: AsRef<Path>>(path: P) -> Result<BufReader<File>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Ok(reader)
}

pub fn quote_to_polars_df_from_series_raghu(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let len = quote.instruments.len();
    let mut symbols = Vec::with_capacity(len);
    let mut instrument_tokens = Vec::with_capacity(len);
    let mut timestamps = Vec::with_capacity(len);
    let mut last_trade_times = Vec::with_capacity(len);
    let mut last_prices = Vec::with_capacity(len);
    let mut last_quantities = Vec::with_capacity(len);
    let mut buy_quantities = Vec::with_capacity(len);
    let mut sell_quantities = Vec::with_capacity(len);
    let mut volumes = Vec::with_capacity(len);
    let mut average_prices = Vec::with_capacity(len);
    let mut ois = Vec::with_capacity(len);
    let mut oi_day_highs = Vec::with_capacity(len);
    let mut oi_day_lows = Vec::with_capacity(len);
    let mut net_changes = Vec::with_capacity(len);
    let mut lower_circuit_limits = Vec::with_capacity(len);
    let mut upper_circuit_limits = Vec::with_capacity(len);
    let mut opens = Vec::with_capacity(len);
    let mut highs = Vec::with_capacity(len);
    let mut lows = Vec::with_capacity(len);
    let mut closes = Vec::with_capacity(len);

    for (symbol, q) in quote.instruments {
        symbols.push(symbol);
        instrument_tokens.push(q.instrument_token);
        timestamps.push(q.timestamp.clone());
        last_trade_times.push(q.last_trade_time.clone());
        last_prices.push(q.last_price);
        last_quantities.push(q.last_quantity);
        buy_quantities.push(q.buy_quantity);
        sell_quantities.push(q.sell_quantity);
        volumes.push(q.volume);
        average_prices.push(q.average_price);
        ois.push(q.oi);
        oi_day_highs.push(q.oi_day_high);
        oi_day_lows.push(q.oi_day_low);
        net_changes.push(q.net_change);
        lower_circuit_limits.push(q.lower_circuit_limit);
        upper_circuit_limits.push(q.upper_circuit_limit);
        opens.push(q.ohlc.open);
        highs.push(q.ohlc.high);
        lows.push(q.ohlc.low);
        closes.push(q.ohlc.close);
    }

    let df = DataFrame::new(vec![
        Series::new("symbol", &symbols),
        Series::new("instrument_token", &instrument_tokens),
        Series::new("timestamp", &timestamps),
        Series::new("last_trade_time", &last_trade_times),
        Series::new("last_price", &last_prices),
        Series::new("last_quantity", &last_quantities),
        Series::new("buy_quantity", &buy_quantities),
        Series::new("sell_quantity", &sell_quantities),
        Series::new("volume", &volumes),
        Series::new("average_price", &average_prices),
        Series::new("oi", &ois),
        Series::new("oi_day_high", &oi_day_highs),
        Series::new("oi_day_low", &oi_day_lows),
        Series::new("net_change", &net_changes),
        Series::new("lower_circuit_limit", &lower_circuit_limits),
        Series::new("upper_circuit_limit", &upper_circuit_limits),
        Series::new("open", &opens),
        Series::new("high", &highs),
        Series::new("low", &lows),
        Series::new("close", &closes),
    ])?;

    Ok(df)
}

pub fn quote_to_polars_df_from_series_v0(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let len = quote.instruments.len();
    let mut symbols = Vec::with_capacity(len);
    let mut instrument_tokens = Vec::with_capacity(len);
    let mut timestamps = Vec::with_capacity(len);
    let mut last_trade_times = Vec::with_capacity(len);
    let mut last_prices = Vec::with_capacity(len);
    let mut last_quantities = Vec::with_capacity(len);
    let mut buy_quantities = Vec::with_capacity(len);
    let mut sell_quantities = Vec::with_capacity(len);
    let mut volumes = Vec::with_capacity(len);
    let mut average_prices = Vec::with_capacity(len);
    let mut ois = Vec::with_capacity(len);
    let mut oi_day_highs = Vec::with_capacity(len);
    let mut oi_day_lows = Vec::with_capacity(len);
    let mut net_changes = Vec::with_capacity(len);
    let mut lower_circuit_limits = Vec::with_capacity(len);
    let mut upper_circuit_limits = Vec::with_capacity(len);
    let mut opens = Vec::with_capacity(len);
    let mut highs = Vec::with_capacity(len);
    let mut lows = Vec::with_capacity(len);
    let mut closes = Vec::with_capacity(len);
    let mut series_buf: Vec<Series> = Vec::with_capacity(20);

    for _ in 0..20 {
        series_buf.push(Series::new("symbol", vec![0u64; len]));
    }

    for (symbol, q) in quote.instruments {
        symbols.push(symbol);
        instrument_tokens.push(q.instrument_token);
        timestamps.push(q.timestamp.clone());
        last_trade_times.push(q.last_trade_time.clone());
        last_prices.push(q.last_price);
        last_quantities.push(q.last_quantity);
        buy_quantities.push(q.buy_quantity);
        sell_quantities.push(q.sell_quantity);
        volumes.push(q.volume);
        average_prices.push(q.average_price);
        ois.push(q.oi);
        oi_day_highs.push(q.oi_day_high);
        oi_day_lows.push(q.oi_day_low);
        net_changes.push(q.net_change);
        lower_circuit_limits.push(q.lower_circuit_limit);
        upper_circuit_limits.push(q.upper_circuit_limit);
        opens.push(q.ohlc.open);
        highs.push(q.ohlc.high);
        lows.push(q.ohlc.low);
        closes.push(q.ohlc.close);
    }

    assert_eq!(series_buf.len(), 20);

    series_buf[0] = Series::new("symbol", &symbols);
    series_buf[1] = Series::new("instrument_token", &instrument_tokens);
    series_buf[2] = Series::new("timestamp", &timestamps);
    series_buf[3] = Series::new("last_trade_time", &last_trade_times);
    series_buf[4] = Series::new("last_price", &last_prices);
    series_buf[5] = Series::new("last_quantity", &last_quantities);
    series_buf[6] = Series::new("buy_quantity", &buy_quantities);
    series_buf[7] = Series::new("sell_quantity", &sell_quantities);
    series_buf[8] = Series::new("volume", &volumes);
    series_buf[9] = Series::new("average_price", &average_prices);
    series_buf[10] = Series::new("oi", &ois);
    series_buf[11] = Series::new("oi_day_high", &oi_day_highs);
    series_buf[12] = Series::new("oi_day_low", &oi_day_lows);
    series_buf[13] = Series::new("net_change", &net_changes);
    series_buf[14] = Series::new("lower_circuit_limit", &lower_circuit_limits);
    series_buf[15] = Series::new("upper_circuit_limit", &upper_circuit_limits);
    series_buf[16] = Series::new("open", &opens);
    series_buf[17] = Series::new("high", &highs);
    series_buf[18] = Series::new("low", &lows);
    series_buf[19] = Series::new("close", &closes);

    DataFrame::new(series_buf)
}

pub fn quote_to_polars_df_from_series_v1(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let len = quote.instruments.len();
    let mut symbols = vec!["".to_string(); len];
    let mut instrument_tokens = vec![0; len];
    let mut timestamps = vec!["".to_string(); len];
    let mut last_trade_times = vec!["".to_string(); len];
    let mut last_prices = vec![0.0; len];
    let mut last_quantities = vec![0; len];
    let mut buy_quantities = vec![0; len];
    let mut sell_quantities = vec![0; len];
    let mut volumes = vec![0; len];
    let mut average_prices = vec![0.0; len];
    let mut ois = vec![0; len];
    let mut oi_day_highs = vec![0; len];
    let mut oi_day_lows = vec![0; len];
    let mut net_changes = vec![0.0; len];
    let mut lower_circuit_limits = vec![0.0; len];
    let mut upper_circuit_limits = vec![0.0; len];
    let mut opens = vec![0.0; len];
    let mut highs = vec![0.0; len];
    let mut lows = vec![0.0; len];
    let mut closes = vec![0.0; len];

    quote
        .instruments
        .iter()
        .enumerate()
        .for_each(|(i, (symbol, q))| {
            // Writing directly to vector elements to avoid push overhead
            symbols[i] = symbol.clone();
            instrument_tokens[i] = q.instrument_token;
            timestamps[i] = q.timestamp.clone();
            last_trade_times[i] = q.last_trade_time.clone();
            last_prices[i] = q.last_price;
            last_quantities[i] = q.last_quantity;
            buy_quantities[i] = q.buy_quantity;
            sell_quantities[i] = q.sell_quantity;
            volumes[i] = q.volume;
            average_prices[i] = q.average_price;
            ois[i] = q.oi;
            oi_day_highs[i] = q.oi_day_high;
            oi_day_lows[i] = q.oi_day_low;
            net_changes[i] = q.net_change;
            lower_circuit_limits[i] = q.lower_circuit_limit;
            upper_circuit_limits[i] = q.upper_circuit_limit;
            opens[i] = q.ohlc.open;
            highs[i] = q.ohlc.high;
            lows[i] = q.ohlc.low;
            closes[i] = q.ohlc.close;
        });

    DataFrame::new(vec![
        Series::new("symbol", &symbols),
        Series::new("instrument_token", &instrument_tokens),
        Series::new("timestamp", &timestamps),
        Series::new("last_trade_time", &last_trade_times),
        Series::new("last_price", &last_prices),
        Series::new("last_quantity", &last_quantities),
        Series::new("buy_quantity", &buy_quantities),
        Series::new("sell_quantity", &sell_quantities),
        Series::new("volume", &volumes),
        Series::new("average_price", &average_prices),
        Series::new("oi", &ois),
        Series::new("oi_day_high", &oi_day_highs),
        Series::new("oi_day_low", &oi_day_lows),
        Series::new("net_change", &net_changes),
        Series::new("lower_circuit_limit", &lower_circuit_limits),
        Series::new("upper_circuit_limit", &upper_circuit_limits),
        Series::new("open", &opens),
        Series::new("high", &highs),
        Series::new("low", &lows),
        Series::new("close", &closes),
    ])
}

pub fn quote_to_polars_df_from_series_v2(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let len = quote.instruments.len();
    let mut series_buf: Vec<Series> = Vec::with_capacity(20);
    let mut buf: Vec<Vec<AnyValue>> = vec![vec![AnyValue::Null; len]; 20];

    assert_eq!(buf.len(), 20);

    quote
        .instruments
        .iter()
        .enumerate()
        .for_each(|(i, (symbol, q))| {
            buf[0][i] = AnyValue::StringOwned(symbol.into());
            buf[1][i] = q.instrument_token.into();
            buf[2][i] = AnyValue::StringOwned(q.timestamp.clone().into());
            buf[3][i] = AnyValue::StringOwned(q.last_trade_time.clone().into());
            buf[4][i] = q.last_price.into();
            buf[5][i] = q.last_quantity.into();
            buf[6][i] = q.buy_quantity.into();
            buf[7][i] = q.sell_quantity.into();
            buf[8][i] = q.volume.into();
            buf[9][i] = q.average_price.into();
            buf[10][i] = q.oi.into();
            buf[11][i] = q.oi_day_high.into();
            buf[12][i] = q.oi_day_low.into();
            buf[13][i] = q.net_change.into();
            buf[14][i] = q.lower_circuit_limit.into();
            buf[15][i] = q.upper_circuit_limit.into();
            buf[16][i] = q.ohlc.open.into();
            buf[17][i] = q.ohlc.high.into();
            buf[18][i] = q.ohlc.low.into();
            buf[19][i] = q.ohlc.close.into();
        });
    series_buf.push(Series::from_any_values_and_dtype(
        "symbol",
        &buf[0],
        &DataType::String,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "instrument_token",
        &buf[1],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "timestamp",
        &buf[2],
        &DataType::String,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "last_trade_time",
        &buf[3],
        &DataType::String,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "last_price",
        &buf[4],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "last_quantity",
        &buf[5],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "buy_quantity",
        &buf[6],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "sell_quantity",
        &buf[7],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "volume",
        &buf[8],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "average_price",
        &buf[9],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "oi",
        &buf[10],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "oi_day_high",
        &buf[11],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "oi_day_low",
        &buf[12],
        &DataType::UInt64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "net_change",
        &buf[13],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "lower_circuit_limit",
        &buf[14],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "upper_circuit_limit",
        &buf[15],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "open",
        &buf[16],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "high",
        &buf[17],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "low",
        &buf[18],
        &DataType::Float64,
        true,
    )?);
    series_buf.push(Series::from_any_values_and_dtype(
        "close",
        &buf[19],
        &DataType::Float64,
        true,
    )?);
    DataFrame::new(series_buf)
}

pub fn quote_to_polars_df_from_series_v3(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let len = quote.instruments.len();
    let mut symbols = Vec::with_capacity(len);
    let mut instrument_tokens = Vec::with_capacity(len);
    let mut timestamps = Vec::with_capacity(len);
    let mut last_trade_times = Vec::with_capacity(len);
    let mut last_prices = Vec::with_capacity(len);
    let mut last_quantities = Vec::with_capacity(len);
    let mut buy_quantities = Vec::with_capacity(len);
    let mut sell_quantities = Vec::with_capacity(len);
    let mut volumes = Vec::with_capacity(len);
    let mut average_prices = Vec::with_capacity(len);
    let mut ois = Vec::with_capacity(len);
    let mut oi_day_highs = Vec::with_capacity(len);
    let mut oi_day_lows = Vec::with_capacity(len);
    let mut net_changes = Vec::with_capacity(len);
    let mut lower_circuit_limits = Vec::with_capacity(len);
    let mut upper_circuit_limits = Vec::with_capacity(len);
    let mut opens = Vec::with_capacity(len);
    let mut highs = Vec::with_capacity(len);
    let mut lows = Vec::with_capacity(len);
    let mut closes = Vec::with_capacity(len);
    let mut series_buf: Vec<Series> = Vec::with_capacity(20);

    for _ in 0..20 {
        series_buf.push(Series::new("symbol", vec![0u64; len]));
    }

    for _ in 0..len {
        symbols.push("".to_string());
        instrument_tokens.push(0);
        timestamps.push("".to_string());
        last_trade_times.push("".to_string());
        last_prices.push(0.0);
        last_quantities.push(0);
        buy_quantities.push(0);
        sell_quantities.push(0);
        volumes.push(0);
        average_prices.push(0.0);
        ois.push(0);
        oi_day_highs.push(0);
        oi_day_lows.push(0);
        net_changes.push(0.0);
        lower_circuit_limits.push(0.0);
        upper_circuit_limits.push(0.0);
        opens.push(0.0);
        highs.push(0.0);
        lows.push(0.0);
        closes.push(0.0);
    }

    quote
        .instruments
        .iter()
        .enumerate()
        .for_each(|(i, (symbol, q))| {
            symbols[i] = symbol.clone();
            instrument_tokens[i] = q.instrument_token;
            timestamps[i] = q.timestamp.clone();
            last_trade_times[i] = q.last_trade_time.clone();
            last_prices[i] = q.last_price;
            last_quantities[i] = q.last_quantity;
            buy_quantities[i] = q.buy_quantity;
            sell_quantities[i] = q.sell_quantity;
            volumes[i] = q.volume;
            average_prices[i] = q.average_price;
            ois[i] = q.oi;
            oi_day_highs[i] = q.oi_day_high;
            oi_day_lows[i] = q.oi_day_low;
            net_changes[i] = q.net_change;
            lower_circuit_limits[i] = q.lower_circuit_limit;
            upper_circuit_limits[i] = q.upper_circuit_limit;
            opens[i] = q.ohlc.open;
            highs[i] = q.ohlc.high;
            lows[i] = q.ohlc.low;
            closes[i] = q.ohlc.close;
        });

    assert_eq!(series_buf.len(), 20);

    series_buf[0] = Series::new("symbol", &symbols);
    series_buf[1] = Series::new("instrument_token", &instrument_tokens);
    series_buf[2] = Series::new("timestamp", &timestamps);
    series_buf[3] = Series::new("last_trade_time", &last_trade_times);
    series_buf[4] = Series::new("last_price", &last_prices);
    series_buf[5] = Series::new("last_quantity", &last_quantities);
    series_buf[6] = Series::new("buy_quantity", &buy_quantities);
    series_buf[7] = Series::new("sell_quantity", &sell_quantities);
    series_buf[8] = Series::new("volume", &volumes);
    series_buf[9] = Series::new("average_price", &average_prices);
    series_buf[10] = Series::new("oi", &ois);
    series_buf[11] = Series::new("oi_day_high", &oi_day_highs);
    series_buf[12] = Series::new("oi_day_low", &oi_day_lows);
    series_buf[13] = Series::new("net_change", &net_changes);
    series_buf[14] = Series::new("lower_circuit_limit", &lower_circuit_limits);
    series_buf[15] = Series::new("upper_circuit_limit", &upper_circuit_limits);
    series_buf[16] = Series::new("open", &opens);
    series_buf[17] = Series::new("high", &highs);
    series_buf[18] = Series::new("low", &lows);
    series_buf[19] = Series::new("close", &closes);

    DataFrame::new(series_buf)
}

pub fn quote_to_polars_df_from_json(
    json: BufReader<File>,
) -> Result<Option<DataFrame>, PolarsError> {
    let schema = Schema::from_iter([
        Field::new("symbol", DataType::String),
        Field::new("instrument_token", DataType::UInt64),
        Field::new("timestamp", DataType::String),
        Field::new("last_trade_time", DataType::String),
        Field::new("last_price", DataType::Float64),
        Field::new("last_quantity", DataType::UInt64),
        Field::new("buy_quantity", DataType::UInt64),
        Field::new("sell_quantity", DataType::UInt64),
        Field::new("volume", DataType::UInt64),
        Field::new("average_price", DataType::Float64),
        Field::new("oi", DataType::UInt64),
        Field::new("oi_day_high", DataType::UInt64),
        Field::new("oi_day_low", DataType::UInt64),
        Field::new("net_change", DataType::Float64),
        Field::new("lower_circuit_limit", DataType::Float64),
        Field::new("upper_circuit_limit", DataType::Float64),
        Field::new("open", DataType::Float64),
        Field::new("high", DataType::Float64),
        Field::new("low", DataType::Float64),
        Field::new("close", DataType::Float64),
    ]);

    let df = JsonReader::new(json)
        .with_json_format(JsonFormat::Json)
        .infer_schema_len(Some(NonZeroUsize::new(100).unwrap()))
        .with_schema_overwrite(&schema)
        .finish()?;
    Ok(Some(df))
}

pub fn quote_to_polars_df_from_rows_cols(quote: Quotes) -> Result<DataFrame, PolarsError> {
    let mut dfbuf: Vec<Row> = Vec::with_capacity(quote.instruments.len());
    let mut buf: Vec<AnyValue> = Vec::with_capacity(20);

    let schema = Schema::from_iter([
        Field::new("symbol", DataType::String),
        Field::new("instrument_token", DataType::UInt64),
        Field::new("timestamp", DataType::String),
        Field::new("last_trade_time", DataType::String),
        Field::new("last_price", DataType::Float64),
        Field::new("last_quantity", DataType::UInt64),
        Field::new("buy_quantity", DataType::UInt64),
        Field::new("sell_quantity", DataType::UInt64),
        Field::new("volume", DataType::UInt64),
        Field::new("average_price", DataType::Float64),
        Field::new("oi", DataType::UInt64),
        Field::new("oi_day_high", DataType::UInt64),
        Field::new("oi_day_low", DataType::UInt64),
        Field::new("net_change", DataType::Float64),
        Field::new("lower_circuit_limit", DataType::Float64),
        Field::new("upper_circuit_limit", DataType::Float64),
        Field::new("open", DataType::Float64),
        Field::new("high", DataType::Float64),
        Field::new("low", DataType::Float64),
        Field::new("close", DataType::Float64),
    ]);

    for (symbol, q) in quote.instruments {
        buf.clear();
        buf.push(AnyValue::StringOwned(symbol.into()));
        buf.push(q.instrument_token.into());
        buf.push(AnyValue::StringOwned(q.timestamp.into()));
        buf.push(AnyValue::StringOwned(q.last_trade_time.into()));
        buf.push(q.last_price.into());
        buf.push(q.last_quantity.into());
        buf.push(q.buy_quantity.into());
        buf.push(q.sell_quantity.into());
        buf.push(q.volume.into());
        buf.push(q.average_price.into());
        buf.push(q.oi.into());
        buf.push(q.oi_day_high.into());
        buf.push(q.oi_day_low.into());
        buf.push(q.net_change.into());
        buf.push(q.lower_circuit_limit.into());
        buf.push(q.upper_circuit_limit.into());
        buf.push(q.ohlc.open.into());
        buf.push(q.ohlc.high.into());
        buf.push(q.ohlc.low.into());
        buf.push(q.ohlc.close.into());
        dfbuf.push(Row::new(buf.clone()));
    }

    let df = DataFrame::from_rows_and_schema(&dfbuf, &schema)?;
    Ok(df)
}

pub mod optional_naive_date_from_str {
    use chrono::NaiveDate;
    use serde::{de, ser, Deserialize, Deserializer};
    const DT_FORMAT: &str = "%Y-%m-%d";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let maybe_naive_date_string: Option<String> = match Deserialize::deserialize(deserializer) {
            Ok(naive_date_string) => Some(naive_date_string),
            Err(_) => None,
        };

        match maybe_naive_date_string {
            Some(naive_date_string) => NaiveDate::parse_from_str(&naive_date_string, DT_FORMAT)
                .map(Some)
                .map_err(de::Error::custom),
            None => Ok(None),
        }
    }
    pub fn serialize<S>(naive_date: &Option<NaiveDate>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match *naive_date {
            Some(ref dt) => serializer
                .serialize_some(&dt.format(DT_FORMAT).to_string())
                .map_err(ser::Error::custom),
            None => serializer.serialize_none(),
        }
    }
}

pub mod optional_naive_date_time_from_str {
    use chrono::NaiveDateTime;
    use serde::{de, ser, Deserialize, Deserializer};
    const DT_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<NaiveDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let maybe_naive_date_time_string: Option<String> =
            match Deserialize::deserialize(deserializer) {
                Ok(naive_date_time_string) => Some(naive_date_time_string),
                Err(_) => None,
            };

        match maybe_naive_date_time_string {
            Some(naive_date_time_string) => {
                NaiveDateTime::parse_from_str(&naive_date_time_string, DT_FORMAT)
                    .map(Some)
                    .map_err(de::Error::custom)
            }
            None => Ok(None),
        }
    }
    pub fn serialize<S>(
        naive_date_time: &Option<NaiveDateTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match *naive_date_time {
            Some(ref dt) => serializer
                .serialize_some(&dt.format(DT_FORMAT).to_string())
                .map_err(ser::Error::custom),
            None => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    #[test]
    fn test_quote_json() -> serde_json::Result<()> {
        let jsonfile = read_json_from_file("kiteconnect-mocks/quote.json").unwrap();
        let deserialized: Quote = serde_json::from_reader(jsonfile)?;
        println!("{:#?}", &deserialized);
        let mut data: HashMap<String, QuoteData> = HashMap::new();
        data.insert(
            "NSE:INFY".to_owned(),
            QuoteData {
                instrument_token: 408065,
                timestamp: Some(NaiveDate::from_ymd(2021, 6, 8).and_hms(15, 45, 56)),
                last_trade_time: Some(NaiveDate::from_ymd(2021, 6, 8).and_hms(15, 45, 52)),
                last_price: 1412.95,
                last_quantity: 5,
                buy_quantity: 0,
                sell_quantity: 5191,
                volume: 7360198,
                average_price: 1412.47,
                oi: 0,
                oi_day_high: 0,
                oi_day_low: 0,
                net_change: 0.0,
                lower_circuit_limit: 1250.7,
                upper_circuit_limit: 1528.6,
                ohlc: OhlcInner {
                    open: 1396.0,
                    high: 1421.75,
                    low: 1395.55,
                    close: 1389.65,
                },
                depth: Depth {
                    buy: [
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                    ]
                    .to_vec(),
                    sell: [
                        OrderDepth {
                            price: 1412.95,
                            quantity: 5191,
                            orders: 13,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                        OrderDepth {
                            price: 0.0,
                            quantity: 0,
                            orders: 0,
                        },
                    ]
                    .to_vec(),
                },
            },
        );
        assert_eq!(
            deserialized,
            Quote {
                status: Status::Success,
                data: Some(data),
                ..Quote::default()
            }
        );
        let serialized = serde_json::to_string(&deserialized).unwrap();
        println!("{:#?}", &serialized);
        // assert_eq!(raw_data, serialized);
        Ok(())
    }

    #[test]
    fn test_quote_no_instruments() -> serde_json::Result<()> {
        let raw_data = r#"{"status":"success","data":{}}"#;
        let deserialized: Quote = serde_json::from_str(raw_data)?;
        println!("{:#?}", &deserialized);
        assert_eq!(
            deserialized,
            Quote {
                status: Status::Success,
                data: Some(HashMap::new()),
                ..Quote::default()
            }
        );
        Ok(())
    }

    #[test]
    fn test_quote_error() -> serde_json::Result<()> {
        let raw_data =
            r#"{"status":"error","message":"Error message","error_type":"GeneralException"}"#;
        let deserialized: Quote = serde_json::from_str(raw_data)?;
        println!("{:#?}", &deserialized);
        assert_eq!(
            deserialized,
            Quote {
                status: Status::Error,
                data: None,
                message: Some("Error message".to_owned()),
                error_type: Some(Exception::GeneralException),
            }
        );
        Ok(())
    }
}
