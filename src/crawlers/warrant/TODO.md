# Warrant crawler TODO

## Proposal: daily 07:30 OpenAPI snapshots for new data

Evaluate replacing the ongoing MOPS event / active-snapshot / diff pipeline
with a full TWSE + TPEx OpenAPI capture every day at 07:30 Asia/Taipei.
This is a proposal only; do not remove existing logic until the conditions
below are resolved. Preserve all existing historical data at the cutover.

Preferred starting point: keep MOPS for basic metadata missing from OpenAPI,
and use OpenAPI for daily current terms. Consider dropping MOPS entirely only
after a field-by-field coverage audit proves it unnecessary. The currently
used endpoints are `t187ap37_L` (TWSE) and `mopsfin_t187ap37_O` (TPEx).

- Archive the full daily response and normalized daily states, one row per
  warrant incarnation per observation date. This avoids reconstructing new
  events through snapshot diffs; retaining an event-only SCD2 output would
  still require change detection. Coordinate with the same-day redesign below.
- Audit every required field: latest strike, allocation ratio (per 1000
  units), expiry, last trading day, original strike, cap/floor, exercise start,
  listing date, issuer, underlying code/name, market and exercise style.
  Current response samples contain most price/date fields but no dedicated
  listing-date, issuer or underlying-code fields. Exercise start is not a
  substitute for listing date for European-style warrants.
- Verify publication timing around 07:30, including adjustment/reset days,
  new listings, holidays and corrections. Store fetch time and source report
  date separately. A successful fetch does not establish same-day freshness:
  samples fetched on 2026-11-13 had first-row report dates of 2026-11-11
  (TWSE) and 2026-11-12 (TPEx). Check all rows/markets, not just the first row.
- Define the new history as observed daily state unless actual effective
  dates are independently available. Do not infer adjustment vs reset or
  backdate changes merely from the capture date. Missing days cannot be
  reconstructed from a later current-state snapshot alone.
- Define retries, stale/partial response handling, new-warrant metadata
  enrichment, and disappearance/expiry handling; absence from a response
  alone must not establish termination. Preserve the last successful archive.
- Compare both pipelines before cutover, including listing resets,
  ex-dividend adjustments, early termination and underlying conversions.
  The existing OpenAPI verifier compares only four terms on matched warrants;
  it does not prove complete universe or metadata coverage.

## Align directory defaults with the other SDK components

Make warrant directory resolution follow the same rules as
`finmind_broker_wrapper` and the shared `env_path()` helper:

- Explicit directory arguments take precedence.
- Otherwise read `DATA_SDK_WARRANT_CACHE_PATH`, `DATA_SDK_TEJ_WARRANTS_PATH`,
  or `DATA_SDK_FINMIND_WARRANTS_PATH`, as appropriate.
- An unset or empty environment variable falls back to the current working
  directory and prints a warning to stderr; remove the built-in NFS defaults.

Keep the existing warrant directory functions as the public interface. Move
the shared path helper to the package root so crawlers and wrappers can use
it, updating existing imports without changing other wrappers' behavior.
Remove the duplicate NFS fallback in `update.sh`: forward `--cache-dir` only
when explicitly supplied, otherwise let Python resolve the environment.
Update CLI help and documentation to match.

Before rollout, confirm scheduled jobs explicitly set all three directory
variables to their existing NFS locations. Verify explicit overrides, set,
unset and empty environment variables, and existing wrapper imports; no
crawl or history rebuild is needed for this change.

## Redesign same-day event resolution (`sequence` / `source_rank`)

Today a warrant can carry several events on one date, and two columns decide
what survives:

- `sequence` orders events within a day (0 synthesised issuance, 1 TEJ /
  MOPS strike, 2 expiry announcement, 9 snapshot diff / target closure).
- `source_rank` breaks ties on the same `(effective_date, sequence)`, on the
  assumption that such rows are one event reported by two sources; only the
  lower rank is kept.

This is more machinery than the data needs. In practice a warrant's terms are
updated at most once a day, so the history could hold **one row per warrant
per date**: merge every event dated that day field by field (each source sets
only the fields it knows: strike/ratio, expiry, underlying), with an explicit
per-field precedence where two sources disagree. `sequence` and
`source_rank` would then disappear from the output.

Known defect of the current design, which the redesign removes: the snapshot
diff and the target closure are different events but share `sequence` 9 and
`source_rank` 5. If both fire for the same warrant on the same date (an
unrecorded share conversion that also moves strike/ratio), `chain_events()`
keeps the snapshot diff and drops the closure, so the underlying change is
lost and the old code is forward-filled. The closure produces 0 events today,
so the defect is latent.
