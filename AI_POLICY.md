# SereneDB AI Policy

## Using AI as a contributor

You can use AI tools to work on SereneDB.

- **You are responsible for what you submit, however it was written.** Read and understand every line before you open a pull request, and be ready to explain it in your own words.
- **Reply to reviewers yourself.** Don't paste AI-generated answers into review threads.
- **Low-effort pull requests that cost maintainers more time than they save will be closed.**
- **Autonomous agents may not open pull requests or issues** on behalf of outside contributors.
- **Licensing:** the same intellectual-property rules apply as for hand-written code. Don't submit code you don't have the right to contribute.

## Our own bot: pudgeai

The maintainers run [@pudgeai](https://github.com/pudgeai), a bot built on Claude Code.

- **What it does:**
  - turns failed nightly runs into deduplicated issues labelled `pudgeai` and `ci-failure`;
  - answers maintainers who mention it, for example `@pudgeai investigate`, with a root-cause analysis.
- **Who can command it:** only people with write access to this repository. It ignores everyone else.
- **What it can't do:** it never merges, never pushes to `main`, and never acts without a maintainer asking it to (apart from filing nightly issues).
- **If it's wrong:** react 👎 to its comment and reply with the correction. Maintainer verdicts are how it gets better.
- **To silence it on a thread:** comment `@pudgeai stop`.
