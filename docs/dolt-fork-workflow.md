# Dolt fork workflow

This repo vendors `dolthub/dolt` as a git submodule under `third_party/dolt`,
pointing at our private mirror at `uplinqai/dolt`. A `replace` directive in
`go.mod` makes Go resolve `github.com/dolthub/dolt/go` against the submodule
rather than the published module path. This document is the workflow for:

- cloning and building the repo
- making local dolt changes
- pulling in upstream dolt commits
- updating the submodule pointer when the dolt-side changes are ready

## Why a private mirror, not a fork

GitHub does not allow private forks of public repositories. The fork would
inherit the upstream's public visibility. The standard workaround for
private internal forks is to create a new private repo seeded from a bare
mirror of the upstream. That is what `uplinqai/dolt` is.

Trade-off: GitHub does not show our mirror as "forked from `dolthub/dolt`"
and we cannot open PRs back to `dolthub/dolt` directly from it. To send a
patch upstream, push the branch to a personal/public fork and open the PR
from there.

## First-time setup

Clone with submodules:

```bash
git clone --recurse-submodules git@github.com:uplinqai/doltgresql.git
cd doltgresql
```

Or, if you've already cloned without `--recurse-submodules`:

```bash
git submodule update --init --recursive
```

Add the upstream remote inside the submodule (one-time, per checkout):

```bash
git -C third_party/dolt remote add upstream https://github.com/dolthub/dolt.git
```

## Daily build

The `replace` directive in the top-level `go.mod` already points Go at the
submodule. Standard `go build` / `go test` commands work as usual.

If you switch to a branch that updates the submodule pointer, run
`git submodule update` to fast-forward your local submodule checkout.

## Making a local dolt change

Work happens inside `third_party/dolt` on a branch in our private mirror.

```bash
cd third_party/dolt
git checkout -b dolt/<change-name>          # branch in third_party/dolt
# edit files, build, test...
git add ...
git commit -m "..."
git push origin dolt/<change-name>          # pushes to uplinqai/dolt

cd ../..                                     # back to doltgresql root
git add third_party/dolt                    # records the new submodule SHA
git commit -m "build: bump dolt to <change-name>"
```

The doltgresql-side commit captures the new submodule SHA. When a teammate
pulls, `git submodule update` fast-forwards them to the same dolt commit.

## Pulling upstream dolt commits and rebasing

Periodically (and before any dolt-side change is sent for review), rebase
our `main` in the mirror onto `dolthub/dolt:main`.

```bash
cd third_party/dolt
git fetch upstream
git fetch origin

# If main on the mirror has no local-only commits, fast-forward:
git checkout main
git merge --ff-only upstream/main
git push origin main

# If main on the mirror has local-only commits, rebase them onto upstream/main:
git checkout main
git rebase upstream/main
git push --force-with-lease origin main
```

Rebasing rewrites history, so use `--force-with-lease` (not `--force`) so
the push fails safely if someone else pushed in between.

After the rebase, update the submodule pointer in doltgresql:

```bash
cd ../..
git add third_party/dolt
git commit -m "build: rebase third_party/dolt onto dolthub/dolt main"
```

Run the test suite afterwards. Upstream dolt changes can break
doltgres-side code (tuple encoding, context threading, etc.); fix any
regressions in the same commit or as direct follow-ups.

## Bumping to a tagged release

```bash
cd third_party/dolt
git fetch upstream --tags
git checkout v<version>
cd ../..
git add third_party/dolt
git commit -m "build: bump dolt to v<version>"
```

## Sending a dolt-side change upstream

Our mirror is private, so we cannot open a `dolthub/dolt` PR from it
directly. Workflow:

1. Push the branch to a personal public fork of `dolthub/dolt`:

   ```bash
   cd third_party/dolt
   git remote add personal git@github.com:<your-username>/dolt.git   # one-time
   git push personal dolt/<change-name>
   ```

2. Open a PR from your personal fork against `dolthub/dolt`.
3. Once merged upstream, `git fetch upstream && git rebase upstream/main`
   in our mirror drops the local copy of the change cleanly.

## Don'ts

- **Don't `git push --force` on the mirror's `main` branch from the
  submodule directory.** Use `--force-with-lease` so concurrent pushes
  fail safely.
- **Don't rewrite history on a branch teammates are using.** If you need
  to rebase, coordinate.
- **Don't bypass the submodule by pinning a published dolt version in
  `go.mod`'s `require` section.** The `replace` directive ensures the
  submodule is the only source of truth.
- **Don't commit dolt-side changes inline in doltgresql commits.** Each
  dolt-side change must land in `third_party/dolt` first; the
  doltgresql commit only records the new submodule SHA.

## Troubleshooting

- **`go build` fails with `missing go.sum entry`**: run `go mod tidy`.
  The submodule's transitive dependencies may need re-resolving.
- **`git submodule update` says "Server does not allow request for
  unadvertised object"**: the local submodule SHA points at a commit
  that's no longer on the mirror (e.g., after a force-push that dropped
  it). Inspect with `git -C third_party/dolt fsck`. Fix by checking out
  a current ref and re-committing the doltgresql submodule pointer.
- **CGO/ICU build failure on macOS**: `brew install icu4c@78` then
  export `CGO_CPPFLAGS` and `CGO_LDFLAGS` per the standard doltgresql
  README's CGO instructions.
