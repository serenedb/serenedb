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
if [[ "$behind" != 0 ]] && grep -qx true <<<"$(rule strict)"; then
	die "#$pr is $behind commits behind main: update it and run CI again"
fi

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
  "review\t\t\(.reviewDecision // "")",
  ((.commits.nodes[0].commit.statusCheckRollup.contexts.nodes // [])[] |
    if .__typename == "StatusContext" then "status\t\(.context)\t\(.state)"
    else "check\t\(.name)\t\(.conclusion // .status)" end)')
review=$(awk -F '\t' '$1 == "review" { print $3 }' <<<"$rollup")
[[ "$(rule approvals | sort -rn | head -1)" -le 0 || "$review" == APPROVED ]] ||
	die "#$pr: review decision $review"
red=
while IFS= read -r required; do
	[[ -n "$required" ]] || continue
	states=$(awk -F '\t' -v name="$required" \
		'$2 == name && !($1 == "status" && name == "pre-commit") { print $3 }' <<<"$rollup")
	bad=$(grep -vxE 'SUCCESS|SKIPPED|NEUTRAL' <<<"$states" || true)
	[[ -n "$states" && -z "$bad" ]] || red+=$'\n'"$required: ${bad:-not reported}"
done < <(rule check)
[[ -z "$red" ]] || die "#$pr: required checks are not green:$red"
waiting=$(awk -F '\t' '$1 == "status" && $2 == "pre-commit" && $3 != "SUCCESS"' <<<"$rollup")

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
refresh=false
if ((${#landings[@]})) || [[ -n "$waiting" ]]; then
	refresh=true
	echo "turn the pre-commit status of $head green"
fi
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

if [[ "$refresh" == true ]]; then
	after=$(python3 scripts/check_submodule_pointers.py --rev "$head" --base "$base" 2>&1) ||
		die "#$pr: its gitlinks are not on version branches after landing:"$'\n'"$after"
	gh workflow run gitlink-status.yml -R "$repo" -f SHA="$head" >/dev/null
	state=
	for _ in $(seq 120); do
		sleep 5
		state=$(gh api "repos/$repo/commits/$head/status" \
			--jq '.statuses[] | select(.context == "pre-commit") | .state') || continue
		[[ "$state" == success ]] && break
	done
	[[ "$state" == success ]] ||
		die "#$pr: its pre-commit status is ${state:-missing} 10 minutes after landing: see https://github.com/$repo/actions/workflows/gitlink-status.yml"
	echo "pre-commit status of $head is green"
fi

gh api -X PUT "repos/$repo/pulls/$pr/merge" -f merge_method=squash -f sha="$head" --jq .message
