use hello::{
    quote_to_polars_df_from_rows_cols, quote_to_polars_df_from_series_raghu,
    quote_to_polars_df_from_series_v0, quote_to_polars_df_from_series_v1,
    quote_to_polars_df_from_series_v2, quote_to_polars_df_from_series_v3,
};
use hello::{read_json_from_file, Quotes};

fn main() {
    let jsonfile = read_json_from_file("kiteconnect-mocks/quotes.json").unwrap();
    let quotes: Quotes = serde_json::from_reader(jsonfile).unwrap();
    let df = quote_to_polars_df_from_series_raghu(quotes.clone()).unwrap();
    println!("{:#?}", &df);
    let df = quote_to_polars_df_from_series_v0(quotes.clone()).unwrap();
    println!("{:#?}", &df);
    let df = quote_to_polars_df_from_series_v1(quotes.clone()).unwrap();
    println!("{:#?}", &df);
    let df = quote_to_polars_df_from_series_v2(quotes.clone()).unwrap();
    println!("{:#?}", &df);
    let df = quote_to_polars_df_from_series_v3(quotes.clone()).unwrap();
    println!("{:#?}", &df);
    let df = quote_to_polars_df_from_rows_cols(quotes.clone()).unwrap();
    println!("{:#?}", &df);
}
