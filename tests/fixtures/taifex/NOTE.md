# tests/fixtures/taifex

## daily_market_report_fut_20261030.csv

The answer of `https://openapi.taifex.com.tw/v1/DailyMarketReportFut`, recorded in full and unchanged on
2026-11-01 at 19:13 Asia/Taipei: one request of `TaifexWrapper._http_bytes` (the wrapper's own transport, the
`requests` default client identity, no `Accept` header, no cookies), 162,390 bytes, which is under the 200 KB
limit, so nothing was trimmed. The body is byte for byte what the endpoint sent:

- UTF-8 with a byte order mark, CRLF line ends, no quoting, a trailing line end;
- a Chinese header line of 19 columns, then 2,150 data rows, all dated `20261030` (1,969 `一般` rows and 181 `盤後` rows);
- the cells are the published strings: `-` and `NULL` stand for a missing figure, `漲跌%` carries its `%` sign, and
  `TradingHalt` and the last column are empty text.

A check of the same endpoint on 2026-11-01 at 12:56 saw HTTP 200, `application/octet-stream` and the same 162,390
bytes, also with `Accept: application/json`; the fetch stored on 2026-10-31 at 17:18 was JSON.

The file has CRLF line ends on purpose (it is the recorded body), so `git diff --check` reports trailing whitespace
for it; do not rewrite its bytes.
