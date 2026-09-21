---
title: Versioning
split: page
---

This page explains the version numbers, release lines and related terms used across SereneDB releases.

- `vx.y.z`: a published SereneDB version, for example `v26.05.3`.
- `vx.y.z.w`: a hotfix release, for example `v26.09.1.1`. It carries fixes on top of `vx.y.z` and nothing else, and it leaves the next ordinary version (`v26.09.2`) free.
- Latest stable release: the newest GitHub release that is not marked as a pre-release.
- Release line: all patch releases that share the same `x.y`, for example `v26.05.2` and `v26.05.3`.
- Patch release: a release that keeps the same line and increments `z`.
- Hotfix release: a release built from a published version plus selected fixes, numbered by appending `.w`. It is used when a fix has to reach users without shipping everything else that has landed since.
- `main`: the development branch for the next release.
- Release branch: a branch used to prepare or patch one release line.
- Active release line: a release line that can still receive fixes.
- Pre-release: a GitHub release intended for testing before it is treated as stable.
- LTS release: a release line selected for longer support when the project announces one.
