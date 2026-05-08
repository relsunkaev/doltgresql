# Delta metadata producer contract

This is the contract that **producers** (sync workers, agents, background-batch
jobs) must satisfy when emitting per-commit delta metadata for the
constrained-merge fast path.

The fast path declines (and falls back to the full Dolt merge/review path) if
a producer violates these rules at delta-validate time. The recommended way to
satisfy the contract is to use `deltameta.Builder`, which enforces every rule
below by construction.

See:

- [`docs/customer-branch-storage-architecture.md`](./customer-branch-storage-architecture.md)
  for the full storage architecture, including the fast-path decline
  vocabulary and retention/GC policy.
- `server/branchstorage/deltameta` for the Go API.

## Required fields

Every delta record must populate the following fields. `Builder` populates all
of them by default; producers using the lower-level `Delta` struct must set
each explicitly.

| Field | Source | Notes |
| --- | --- | --- |
| `Format` | `deltameta.FormatVersion1` | Bumped when the on-the-wire shape changes; older readers refuse unknown formats. |
| `BaseRoot` | the branch's recorded base root at branch creation | Must be the exact root the branch forked from. The fast path declines `declined_missing_delta_metadata` if it does not match the merge driver's expectation. |
| `TargetRef` | `refs/heads/<branch-name>` | Free-form but non-empty. Used for diagnostics and audit. |
| `TargetCommit` | the new branch commit's hash | The fast path uses this to bind the delta to the commit it describes. |
| `Tables` | one entry per affected table | At least one entry required. |
| `Tables[i].Rows` | one entry per changed row | At least one entry per table required. |

## Per-row rules

Every `RowChange` must encode exactly one of three shapes:

| Shape | `OldRowHash` | `NewRowHash` | `ChangedScalars` | `TouchedComplex` |
| --- | --- | --- | --- | --- |
| INSERT | `nil` | non-nil | empty | optional |
| DELETE | non-nil | `nil` | empty | optional |
| UPDATE | non-nil | non-nil, distinct from old | required if scalars changed | optional |

Additional invariants:

- **Primary key uniqueness within a table**: a row's primary key may appear at
  most once in any single `TableDelta`.
- **No identical-hash UPDATE**: if `OldRowHash == NewRowHash`, the row didn't
  change. Producers must not emit such entries (they bloat the delta and
  confuse fast-path decision logic).
- **No empty column names**: `ChangedScalars` and `TouchedComplex` entries
  must be non-empty after trimming.
- **No scalar/complex overlap**: a column may appear in `ChangedScalars` *or*
  `TouchedComplex`, never both. The two categories are mutually exclusive.
- **No duplicates**: each column listed at most once per row in each list.

## What `ChangedScalars` and `TouchedComplex` mean

- `ChangedScalars` lists scalar columns whose value differs between the
  recorded base row and the recorded target row. The fast path uses this list
  to permit same-row, disjoint-column merges: if main also edited the row
  but its changes touch *different* scalars, the fast path can build the
  merged row from the union of disjoint edits.
- `TouchedComplex` lists non-scalar columns that the row touched (blob, JSON,
  generated values, anything whose merge semantics are not safely
  column-local). If both main and the branch touched a row that has any
  `TouchedComplex` entry, the fast path declines `declined_unsupported_column`
  conservatively. Producers should mark *any* column whose merge semantics
  they are not certain are scalar and column-local.

If a producer cannot safely classify a column, **mark it as
`TouchedComplex`**. Over-marking is safe: it forces a decline in ambiguous
cases. Under-marking is a correctness bug.

## Provenance

`Provenance` is an optional `map[string]string` for audit metadata: job ID,
worker name, request correlation, etc. It does not influence merge decisions
and is preserved verbatim through encoding.

## Rejection behavior

- A producer-side `Validate` failure must abort the commit. Persisting an
  invalid delta would leak through to the fast-path driver as
  `declined_missing_delta_metadata` at merge time, masking the producer bug.
- The architecture's commit pipeline calls `Validate` synchronously before
  attaching the delta to the commit, so producers see the error at write time.

## Recommended pattern

```go
b := deltameta.NewBuilder(baseRoot, "refs/heads/sync-2026-05-07", commitHash)
b.Provenance("job", "sync-worker-42")
for _, ev := range events {
    switch ev.kind {
    case eventInsert:
        b.AddInsert(ev.table, ev.pk, ev.newRowHash)
    case eventUpdate:
        b.AddUpdate(ev.table, ev.pk, ev.oldRowHash, ev.newRowHash, ev.changedScalars, ev.touchedComplex)
    case eventDelete:
        b.AddDelete(ev.table, ev.pk, ev.oldRowHash)
    }
}
delta, err := b.Build() // Validate + canonicalize
if err != nil {
    return fmt.Errorf("producer bug: %w", err)
}
encoded, _ := deltameta.Encode(delta)
// hand `encoded` to the deltastore alongside the commit
```

## Anti-patterns

- **Listing every column on every UPDATE.** The fast path's decline reasons
  can only inform the operator if `ChangedScalars` reflects the *actual*
  edits. Listing unchanged columns forces same-column conflicts that are not
  real conflicts and starves the fast path.
- **Omitting `ChangedScalars` on UPDATEs.** If the branch hash differs from
  the base hash but no columns are listed, the fast path cannot merge with a
  main-side edit; it declines `declined_missing_delta_metadata`. Always list
  the changed scalars.
- **Treating JSON or blob columns as scalars.** Merging non-scalar values
  byte-for-byte without type-aware semantics will corrupt the data on a
  same-row collision. Mark them as `TouchedComplex`.
- **Building deltas in the absence of a stable `BaseRoot`.** The branch must
  have a recorded base root before the producer starts. Re-using a `BaseRoot`
  from a previous branch leads to stale-binding declines at merge time.
