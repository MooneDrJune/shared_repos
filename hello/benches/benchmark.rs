use criterion::{criterion_group, criterion_main, Criterion};
use hello::{
    quote_to_polars_df_from_rows_cols, quote_to_polars_df_from_series_raghu,
    quote_to_polars_df_from_series_v0, quote_to_polars_df_from_series_v1,
    quote_to_polars_df_from_series_v2, quote_to_polars_df_from_series_v3,
};
use hello::{read_json_from_file, Quotes};
use std::hint::black_box;

fn criterion_benchmark(c: &mut Criterion) {
    let jsonfile = read_json_from_file("kiteconnect-mocks/quotes.json").unwrap();
    let quotes: Quotes = serde_json::from_reader(jsonfile).unwrap();
    c.bench_function("quote_to_polars_df_from_series_raghu", |b| {
        b.iter(|| quote_to_polars_df_from_series_raghu(quotes.clone()).unwrap())
    });
    c.bench_function("quote_to_polars_df_from_series_v0", |b| {
        b.iter(|| quote_to_polars_df_from_series_v0(quotes.clone()).unwrap())
    });
    c.bench_function("quote_to_polars_df_from_series_v1", |b| {
        b.iter(|| quote_to_polars_df_from_series_v1(quotes.clone()).unwrap())
    });
    c.bench_function("quote_to_polars_df_from_series_v2", |b| {
        b.iter(|| quote_to_polars_df_from_series_v2(quotes.clone()).unwrap())
    });
    c.bench_function("quote_to_polars_df_from_series_v3", |b| {
        b.iter(|| quote_to_polars_df_from_series_v3(quotes.clone()).unwrap())
    });
    c.bench_function("quote_to_polars_df_from_rows_cols", |b| {
        b.iter(|| quote_to_polars_df_from_rows_cols(quotes.clone()).unwrap())
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
