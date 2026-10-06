---
name: create-worktree
description: Create a second SereneDB checkout (git worktree) with every submodule materialized offline from the main checkout through hardlinks - for a main baseline build, another branch, or text-only PR work, without touching the main checkout. Use when the user wants a new worktree, a baseline of main, or to work on another branch in parallel.
argument-hint: "<path> <branch> [<base>]"
---

# Create a worktree

`git worktree add` doesn't populate submodules, and cloning ~50 of them from the network is slow. `.claude/tools/create-worktree.sh` hardlinks each submodule's git directory from the main checkout's `.git/modules/` into the worktree's git directory and checks out the gitlink commit there: no network, no extra disk for git objects. Every file git rewrites (config, index, HEAD) is replaced by rename, so the main checkout's submodules stay untouched.

## Use

```bash
.claude/tools/create-worktree.sh ../serenedb-<topic> <branch> [<base, default origin/main>]
.claude/tools/create-worktree.sh --no-submodules ../serenedb-<topic> <branch>
```

- An existing local branch is checked out; an `origin/<branch>` gets a local branch of the same name; anything else becomes a new branch from `<base>` without upstream tracking.
- `--no-submodules` for text-only work (docs, `.claude/`, scripts): only the superproject files.
- With submodules expect ~2.5 GB of sources plus whatever you build.
- A gitlink commit missing from the main checkout's submodule stops the script with the `fetch` command to run in the main checkout.

## Build in it

- Configure a fresh build dir inside the worktree (`cmake --preset clangd` or `lldb`). The submodule heads match the gitlinks, so `AUTO_UPDATE_MODULES` has nothing to reset.
- A baseline for an A/B: build `main` in a worktree once, copy the binaries out, and keep them; see the `perf-measure` skill.

## Traps

- The worktree shares `.git/config` with the main checkout: never run `git config` without `-f <file>`, nor `git submodule init/sync`, inside it.
- Plain `git worktree remove` refuses worktrees with submodules. `git worktree remove --force <path>` deletes the worktree and its hardlinked copies; the originals stay. It also discards uncommitted work there, so check `git -C <path> status` first.
- Two checkouts building at once double the load on a shared machine.
