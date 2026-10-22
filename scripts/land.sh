#!/bin/bash
set -euo pipefail

usage() {
	echo "usage: $0 <serenedb PR> [--yes]" >&2
	exit 2
}

pr=
yes=false
for arg in "$@"; do
	if [[ "$arg" == --yes ]]; then
		yes=true
	elif [[ "$arg" =~ ^[0-9]+$ && -z "$pr" ]]; then
		pr=$arg
	else
		usage
	fi
done
[[ -n "$pr" ]] || usage

repo=serenedb/serenedb
context='serenedb | gitlinks'
cd "$(git rev-parse --show-toplevel)"

die() {
	echo "$*" >&2
	exit 1
}

read -r state draft head < <(gh api "repos/$repo/pulls/$pr" --jq '"\(.state) \(.draft) \(.head.sha)"')
[[ "$state" == open ]] || die "#$pr is $state"
[[ "$draft" == false ]] || die "#$pr is a draft"

rules=$(gh api "repos/$repo/rules/branches/main" --jq '.[] |
  if .type == "required_status_checks" then
    "strict\t\(.parameters.strict_required_status_checks_policy)",
    (.parameters.required_status_checks[] | "check\t\(.context)")
  elif .type == "pull_request" then
    "approvals\t\(.parameters.required_approving_review_count)"
  else empty end')
rule() {
	awk -F '\t' -v key="$1" '$1 == key { print $2 }' <<<"$rules"
}

read -r base behind < <(gh api "repos/$repo/compare/main...$head" --jq '"\(.merge_base_commit.sha) \(.behind_by)"')
[[ "$behind" == 0 ]] || ! rule strict | grep -qx true ||
	die "#$pr is $behind commits behind main: update it and run CI again"

rollup=$(gh api graphql -F pr="$pr" -f query='
query($pr: Int!) {
  repository(owner: "serenedb", name: "serenedb") {
    pullRequest(number: $pr) {
      reviewDecision
      commits(last: 1) { nodes { commit { statusCheckRollup { contexts(first: 100) { nodes {
        __typename
        ... on StatusContext { context state }
        ... on CheckRun { name conclusion status }
      } } } } } }
    }
  }
}' --jq '.data.repository.pullRequest |
  "review\t\(.reviewDecision // "")",
  ((.commits.nodes[0].commit.statusCheckRollup.contexts.nodes // [])[] |
    if .__typename == "StatusContext" then "\(.context)\t\(.state)"
    else "\(.name)\t\(.conclusion // .status)" end)')
lookup() {
	awk -F '\t' -v key="$1" '$1 == key { print $2; exit }' <<<"$rollup"
}
review=$(lookup review)
[[ "$(rule approvals | sort -rn | head -1)" -le 0 || "$review" == APPROVED ]] ||
	die "#$pr: review decision $review"
red=
while IFS= read -r required; do
	[[ -n "$required" && "$required" != "$context" ]] || continue
	check=$(lookup "$required")
	[[ "$check" == SUCCESS ]] || red+=$'\n'"$required: ${check:-not reported}"
done < <(rule check)
[[ -z "$red" ]] || die "#$pr: required checks are not green:$red"

git fetch --quiet --no-tags origin "$head" "$base"
rc=0
out=$(python3 scripts/check_submodule_pointers.py --rev "$head" --base "$base" 2>&1) || rc=$?
[[ -z "$out" ]] || echo "$out"
[[ $rc == 0 || $rc == 3 ]] || die "#$pr: its gitlinks are wrong, see above"

landings=()
while read -r sha fork number branch; do
	[[ -n "$sha" ]] || continue
	ff=$(gh api "repos/$fork/compare/$branch...$sha" --jq .status)
	[[ "$ff" == ahead ]] ||
		die "$fork#$number does not fast-forward $branch ($ff): rebase it onto $branch, point the gitlink at the new head and run CI again"
	read -r fork_draft fork_review < <(gh pr view "$number" -R "$fork" --json isDraft,reviewDecision \
		--jq '"\(.isDraft) \(.reviewDecision)"')
	[[ "$fork_draft" == false ]] || die "$fork#$number is a draft"
	echo "land $fork#$number: $branch -> $sha${fork_review:+ ($fork_review)}"
	landings+=("$fork $branch $sha")
done < <(sed -n 's/^.*: gitlink \([0-9a-f]*\) is pending: head of open \([^#]*\)#\([0-9]*\) into \(.*\)$/\1 \2 \3 \4/p' <<<"$out")
echo "squash-merge #$pr at $head"

if [[ "$yes" != true ]]; then
	echo "dry run: rerun with --yes to land"
	exit 0
fi

for landing in "${landings[@]}"; do
	read -r fork branch sha <<<"$landing"
	gh api -X PATCH "repos/$fork/git/refs/heads/$branch" -f sha="$sha" -F force=false >/dev/null
	echo "landed $fork $branch at $sha"
done

gitlinks() {
	gh api "repos/$repo/commits/$head/status" \
		--jq ".statuses[] | select(.context == \"$context\" and .updated_at >= \"$1\") | .state"
}
if [[ "$(gitlinks 0)" != success ]]; then
	since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	gh workflow run gitlinks.yml -R "$repo" -f PR_NUMBER="$pr"
	for _ in $(seq 120); do
		sleep 5
		case "$(gitlinks "$since" || true)" in
		success) break ;;
		failure | error) die "#$pr: $context failed after landing, see its run" ;;
		esac
	done
	[[ "$(gitlinks "$since")" == success ]] || die "#$pr: $context did not pass within 10 minutes"
fi

gh api -X PUT "repos/$repo/pulls/$pr/merge" -f merge_method=squash -f sha="$head" --jq .message
