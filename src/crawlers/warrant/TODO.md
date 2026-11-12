# Warrant crawler TODO

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
