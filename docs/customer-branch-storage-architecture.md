# Customer branch storage architecture

This document defines the storage architecture target for customer-local
Doltgres deployments where one Doltgres database and Dolt repository is
authoritative for a customer's data. The north star is fast interactive reads on
`main`, explicit branch reads for batch jobs, and auditable merges that become
visible through ordinary Dolt commits.

This is a storage plan only. It does not choose application table boundaries,
data migrations, accounting domain models, or downstream read-model topology.

## Goals

- Keep `main` read-optimized for ordinary interactive PostgreSQL queries.
- Let generated sync, agent, and background jobs write on explicit branches.
- Keep branch reads fast after each branch commit.
- Make constrained branch merges typically subsecond for batches up to about
  10k changed rows.
- Preserve Dolt durability, rollback, ref, root-hash, diff, conflict, and merge
  semantics.
- Keep every visible branch head and `main` commit backed by a deterministic
  materialized Dolt root with eager indexes.

The plan may use ideas from immutable-storage and MVCC systems for snapshots,
deltas, and garbage collection. It must not replace Dolt's visible root model
with logical-only MVCC snapshots.

## Workload Model

The primary workload is read-heavy customer access. `main` is the default read
view. Branch reads are explicit and are normally tied to a batch job, review
workflow, or generated write lane.

Writes are expected to arrive mostly as sync, agent, and background batches
rather than arbitrary high-QPS foreground single-row writes. Large imports,
backfills, or schema-shaping changes may use the existing slower merge path.
The fast path is for constrained batches whose row-level effects are known.

Target benchmark shapes:

| Shape | Approximate size |
| --- | --- |
| Median customer | 4k-8k journal-line scale |
| Average customer | 6k journals / 12k lines |
| Largest known customer | 132k journals / 348k lines |

## Branch and Root Invariant

Every visible branch commit owns a materialized Dolt root. Branch reads use the
same root/index read path as `main`; they are not reconstructed from patches at
query time.

Required behavior:

- branch creation records the selected base root
- branch commits materialize the branch target root before visibility
- secondary indexes are updated eagerly in the branch root
- successful merges update `main`'s materialized root and indexes before
  visibility
- dozens of active branches per customer are expected and should not require a
  special read path

Downstream PostgreSQL or Electric-style read models may consume commits
asynchronously. They are not part of the merge visibility boundary and must not
block a successful Dolt merge from becoming visible.

## Delta Metadata

Dolt core should store per-commit delta metadata for generated batch branch
commits. The metadata is an optimization and diagnostics surface, not a new
source of truth. If it is missing, stale, unsupported, or too large, merge falls
back to the existing Dolt merge and review path.

Each delta metadata record should include:

| Field | Purpose |
| --- | --- |
| affected tables | Restrict validation and merge work to changed tables. |
| primary keys | Identify changed rows using durable table key encoding. |
| old row hash | Prove the row matched the recorded base state. |
| new row hash | Prove the branch target row matches the recorded commit state. |
| changed scalar columns | Enable same-row, different-column scalar merges. |
| base root | Bind the delta to the branch base used for conflict checks. |
| target branch metadata | Bind the delta to the branch commit being merged. |
| job/provenance metadata | Optional audit information for generated jobs. |

The metadata must be deterministic and durable with the commit it describes.
It should be compact enough that the measured overhead is acceptable for the
10k-row constrained-batch target.

## Constrained Merge Fast Path

The fast path applies only to generated or batch branch writes with known base
roots and valid row-delta metadata. It is a correctness-preserving optimization
for cases where the merge decision can be made from the base root, current
`main`, target branch root, and recorded row deltas.

The merge implementation should:

1. Verify the branch delta is present, well-formed, and bound to the expected
   base root and target branch commit.
2. Reject schema changes from the fast path.
3. Reject batches above the configured fast-path row-change limit.
4. For every changed row, compare the recorded base row hash, the current
   `main` row, and the target branch row.
5. Apply clean row additions, deletes, and updates when existing Dolt merge
   semantics can be preserved.
6. Allow same-row, different-column scalar updates when both sides edited
   disjoint supported scalar columns from the same base row.
7. Update `main`'s materialized root and eager indexes before publishing the new
   `main` commit.

Same-column edits are conflicts for the fast path. Complex columns are
conservative by default: generated values, blobs, JSON-like documents, and any
type whose merge semantics are not explicitly scalar and column-local decline
into fallback when both sides touch the value.

The fast path must produce the same visible committed result as an accepted
Dolt merge would produce for the supported case. Unsupported cases are
declines, not partial semantic changes.

## Decline Reasons

Fast-path merge status should be exposed internally and through diagnostics.
The status vocabulary is:

| Status | Meaning |
| --- | --- |
| `fast_path_applied` | The constrained merge was applied and published. |
| `declined_conflict` | The delta found same-column, delete/update, or other conflict-shaped edits. |
| `declined_unsupported_column` | The merge touched unsupported complex, generated, blob, JSON-like, or otherwise non-scalar columns. |
| `declined_missing_delta_metadata` | Required delta metadata was missing, stale, malformed, or not bound to the expected roots. |
| `declined_batch_too_large` | The changed-row count exceeded the configured fast-path limit. |
| `declined_schema_change` | The branch commit included schema changes. |

Every decline must be deterministic and must hand control to the existing full
Dolt merge/review path with enough context for diagnostics.

## Retention and Garbage Collection

Retention must preserve audit history and active review branches while limiting
storage growth from abandoned branch materializations and obsolete delta
metadata.

Policy requirements:

- keep all reachable commits needed for audit and normal Dolt history
- preserve active branch heads and active pull/review branches
- retain delta metadata while it can support diagnostics, review, rollback, or
  replay for configured windows
- garbage-collect materialized roots for abandoned branches after retention
  windows when no visible ref or audit requirement needs them
- garbage-collect obsolete delta metadata after its owning commit is no longer
  within the configured diagnostic or fast-merge window

GC must not remove data needed to resolve visible refs, compute ordinary Dolt
diffs, reproduce audit commits, or complete fallback merge/review flows.

## Benchmarks

The benchmark suite should use customer-sized databases and compare Doltgres to
PostgreSQL for representative accounting queries where applicable.

Measure:

- `main` read latency for representative accounting queries
- branch creation latency
- branch batch commit latency up to 10k changed rows
- branch read latency after commit
- constrained merge latency for non-overlapping scalar column edits
- fallback latency and correctness for conflict-shaped changes
- storage overhead per branch materialization
- storage overhead per delta metadata entry

Correctness coverage:

- same-row different-column merge succeeds
- same-column conflict falls back
- complex-column conflict falls back
- missing or stale delta metadata falls back
- schema changes fall back
- crash recovery preserves visible roots and merge decisions
- branch, diff, and merge results match existing Dolt semantics on fallback
  cases

The performance target is subsecond typical full merge visibility for
constrained batches up to about 10k changed rows. This target includes updating
the `main` materialized root and indexes before visibility. It does not include
eventual downstream read-model catchup.
