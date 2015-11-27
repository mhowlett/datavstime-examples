## Example data-vs-time proxy adapter targets

Currently there are three:

1. A minimal but fully featured example written in **go** (working
but not quite finished).
2. A more substantial currency exchange rate example in **C#** 
(targeting CoreCLR) that sources data from an InfluxDb database.
Note that functionality for acquiring the currency data is not
included.
3. An nginx module written in **C** which provides data for the
demo on the website.
Code for this is in a separate repo:
[ngx_data_vs_time](https://github.com/mhowlett/ngx_data_vs_time).
