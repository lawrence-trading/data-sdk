# Warrant crawler changelog

## 2026-11-12

### Share one raw-table loader between build and validate

- `validate.py` now reads raw tables through `load_raw_table()` instead of
  its own copy of the same glob/concat/clean logic, so validation sees the
  tables exactly as the builders do. A missing required table still raises
  `FileNotFoundError`.

### Read the TEJ adjustment seed once

- `build_history()` now reads the TEJ adjustment parquet once, takes the
  seed end date from its raw `年月日`, and passes the frame to
  `events_from_tej()`. Previously the file was read a second time just to
  obtain that date.

### Reuse the snapshot within a history build

- Read `warrant_active_snapshot` once in `build_history()` and share its
  crawl date between snapshot differences and target closure. Previously,
  the entire snapshot was read and cleaned again just to obtain that date.

## 2026-11-04

### Track underlying conversions as event-time state (`09f1386`)

- Conversions observed in 2026 included 中光電 `5371` → 中光電投控 `3718`
  (2026-09-03), 精星 `8183` → 精成科 `6191`, and 三商壽 `2867` → 玉山金
  `2884`. These cases motivated treating the underlying as mutable in the
  dimension snapshot and as event-time state in history. Expired warrants
  retained the underlying from their expiry, while live snapshots reflected
  the conversion.
- TEJ's 2020-06-19 event recorded 勝麗 `6238` → 同欣電 `6271`, demonstrating
  that each event's underlying was needed for historical quote joins.
- No fallback `target_change` closure event was needed in the 2026-11-03 build.

## 2026-09-29

### Use published last-trading dates and the Taiwan trading calendar (`b88b9c5`)

- Plain business-day arithmetic gave a warrant expiring on 2026-09-30 a
  last trading day of 09-28 (教師節), instead of 09-24; 09-25 was 中秋.
  The trading-calendar lookup corrected this holiday case, retaining a
  business-day fallback when calendar coverage or fetching is unavailable.

## 2026-09-17

### Synthesise missing issuance states (`9209bb4`)

- The first TEJ row was already a change for 2,011 warrants. Treating any
  event as issuance coverage left no earlier terms for these warrants, or
  for post-seed listings whose first event was a listing-day reset.
  Coverage therefore had to require an issuance event.

## 2026-09-15

### Capture OTC and future-effective adjustments (`3043d0f`)

- OTC strike history was missing until 2026-09-15 because event reports
  required both TWSE and OTC endpoints.
- MOPS applied the 德微 ex-dividend snapshot adjustment a day early on
  2026-09-14. The same problem affected 1,416 TSMC warrants on 2026-09-15,
  the eve of their ex-dividend date. Snapshot-dated differences could
  therefore introduce premature history states. Pending adjustment rows
  supplied their own 生效日 and were kept separate from the incremental
  cursor so future dates would not block later runs.

## 2026-09-14

### Resume truncated TPEx OpenAPI downloads (`f4b012c`)

- On 2026-09-14, two of three TPEx OpenAPI requests returned HTTP 200 and
  a full Content-Length but disconnected after a few hundred KB. Range
  requests allowed resuming the body, with JSON parsing as the final
  completeness check.

### Recalculate last-trading dates after expiry changes (`f1c3516`)

- All 60 early terminations checked against exchange data had their last
  trading day two business days before the new expiry.

### Exclude extendable warrants from history (`ea8daa7`)

- The extendable-warrant sample contained 104 warrants from 2014–2019,
  with names such as 元展06, 富展01, and 永豐金元大61展01. 大展證券's
  plain warrants from 2005–2008 also used suffixes such as 大展01, requiring
  an issuer exclusion. Extensions were not modelled in history.

### Preserve issuance and pre-listing reset dates (`38eac74`)

- Of 2,692 MOPS resets, 2,646 fell exactly on `list_date`; the remainder were
  up to three days earlier. TEJ placed 1,443 resets on the day before listing.
  Issuance and reset dates were retained as distinct states.

### Use the raw TEJ coverage boundary (`6e7bccc`)

- In one build, deriving the TEJ coverage boundary from transformed issuance
  dates moved it to 09-02 even though raw TEJ events ended on 09-01. This
  incorrectly dropped all 989 MOPS adjustments effective 09-02 as already
  covered. The boundary was changed to the maximum raw TEJ 年月日.

## 2026-09-11

### Retry NFS package cleanup (`fedbaa8`)

- On the cache's NFS mount with `fsc` enabled, dlt package cleanup encountered
  lingering `.nfs*` files and `ENOTEMPTY` / `EBUSY`. Entries cleared in about
  15 ms on an idle machine but took 1–30 seconds under heavy shared-mount
  load (load averages in the hundreds). These measurements motivated the
  approximately 64-second retry backoff.

## 2026-09-09

### Repair historical MOPS dates and scheduled expiries (`88cf704`)

- MOPS served corrupted `list_date` and `exercise_start_date` values set to
  `2023-12-26` on approximately 51,500 rows, mostly OTC warrants expired in
  2011–2019. Replaying the live MOPS query confirmed the source corruption.
  The detailed sample contained 51,537 corrupted rows, 48,739 of them OTC.
- TEJ supplied replacement dates within its coverage, beginning in 2019.
  A frozen FinMind summary, covering listings from 2011, supplied a second
  repair pass for approximately 48,700 rows predating TEJ's window.
  Unrepairable dates were left null.
- Across 19,687 warrants shared by the three sources, FinMind listing dates
  agreed with TEJ exactly in 97.7% of cases and within three days in 99.9%.
  This supported placing FinMind below TEJ and using it only for visibly
  invalid MOPS dates. Matching tried both last-trading and expiry dates,
  because FinMind's `end_date` had different meanings for TWSE and OTC.
- Crawls before 2026-09 stored blank cells as the literal string `nan`.
- TEJ's 預定最後交易日 differed from the actual last trading day by 1–7 days
  on approximately 6,800 warrants without early termination. Differences
  clustered around market closures (2025-10-22 and 2026-02-10) and schedule
  arithmetic. Taking scheduled dates literally put 0.9% of live warrants
  one day away from exchange expiry dates.
- A group of 35 warrants ended on 2026-08-20 despite a 2026-09-11 schedule.
  These observations motivated the seven-day rescheduling cutoff. Detecting
  terminations within a week still requires announcement-based classification.

### Infer exercise style from dates (`d6ee083`)

- Inferring American versus European exercise style from listing and
  exercise-start dates agreed with TEJ's 權證類型 on all 418,527 overlapping
  warrants, with no exceptions in that sample.

## 2026-09-08

### Initial point-in-time history and source validation (`e64bba0`)

- Listing code `030001` had been reused for three distinct warrants,
  illustrating why identity requires the warrant name as well as the code.
- The recorded TEJ adjustment seed began on 2020-01-02. Approximately 95% of
  historical strike changes existed only in that export because MOPS retained
  a rolling window of roughly 18 months. Captured announcements covered
  approximately 2026-01 onward.
- An earlier snapshot-gap investigation highlighted still-live extendable
  warrants listed before 2020 whose only event was a synthesised issuance.
  Their strikes could stay stale without snapshot reconciliation. This is
  a historical observation; extendable warrants are excluded from history.
- The dimension-date fallback was observed to cover the approximately 99%
  of warrants that had never been rescheduled.
- The earliest observed t95sb01 announcement was 115/01/28, consistent with
  a rolling retention window of approximately eight months.
- Early-termination filenames were checked against TEJ: their second field
  matched the new expiry exactly. For example,
  `7790-20260810-20260805-141118-9.doc` encoded 2026-08-10 in Gregorian format.
- Extension filenames did not share that meaning: warrant `03029X`'s
  2026-09-07 extension named 2026-03-04, six months in the past. Extensions
  affected 43 extendable warrants in that investigation and were excluded
  rather than interpreting the field as a new expiry.
