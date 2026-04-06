#!/bin/bash
# Sends build status notifications to TG.
#
# Required:
#   TG_TOKEN, TG_CHAT_ID, TG_THREAD_ID_MAIN, TG_THREAD_ID_BUILD
#   JOB_STATUS (success/failure/warning/cancelled)
#   BRANCH, ACTOR, RUN_NAME, RUN_NUMBER, RUN_URL
#
# Optional:
#   TG_USER_MAP    - JSON mapping GitHub usernames to TG usernames
#   JOB_STATUS_*   - per-job results (e.g. JOB_STATUS_DOCKER=success)

set -euo pipefail

# Validate required vars
for var in TG_TOKEN TG_CHAT_ID TG_THREAD_ID_MAIN TG_THREAD_ID_BUILD \
	JOB_STATUS BRANCH ACTOR RUN_NAME RUN_NUMBER RUN_URL; do
	if [[ -z "${!var:-}" ]]; then
		echo "ERROR: Missing $var" >&2
		exit 1
	fi
done

# Resolve TG username from GitHub username
TG_USERNAME="${ACTOR}"
if [[ -n "${TG_USER_MAP:-}" ]]; then
	MAPPED=$(echo "${TG_USER_MAP}" | jq -r --arg u "${ACTOR}" '.[$u] // empty' 2>/dev/null || true)
	if [[ -n "${MAPPED}" ]]; then
		TG_USERNAME="${MAPPED}"
		echo "::add-mask::${TG_USERNAME}"
	fi
fi

# Status emoji
case "${JOB_STATUS}" in
success)
	EMOJI="✅"
	RESULT="SUCCESS"
	;;
warning)
	EMOJI="⚠️"
	RESULT="WARNING"
	;;
cancelled)
	EMOJI="🚫"
	RESULT="CANCELLED"
	;;
*)
	EMOJI="💥"
	RESULT="FAILURE"
	;;
esac

# Thread: main branch -> main thread, other branches -> build thread
if [[ "${BRANCH}" == "main" ]]; then
	THREAD_ID="${TG_THREAD_ID_MAIN}"
else
	THREAD_ID="${TG_THREAD_ID_BUILD}"
fi

# Build message
MESSAGE="${EMOJI} @${TG_USERNAME} ${RESULT}: BRANCH=${BRANCH} ${RUN_NAME} #${RUN_NUMBER} ${RUN_URL}"

# Append per-job status lines (from JOB_STATUS_* env vars)
JOB_LINES=""
while IFS='=' read -r key value; do
	[[ "$key" =~ ^JOB_STATUS_(.+)$ ]] || continue
	name="${BASH_REMATCH[1]}"
	case "${value}" in
	success) job_emoji="✅" ;;
	failure) job_emoji="💥" ;;
	cancelled) job_emoji="🚫" ;;
	skipped) job_emoji="⏭️" ;;
	*) job_emoji="➖" ;;
	esac
	JOB_LINES+=$'\n'"  ${job_emoji} ${name}: ${value}"
done < <(env | grep "^JOB_STATUS_" | sort)

if [[ -n "${JOB_LINES}" ]]; then
	MESSAGE+=$'\n'"Jobs:${JOB_LINES}"
fi

# Send
echo "TG notification: ${RESULT} (${BRANCH}, ${ACTOR})"
curl --silent --show-error --location --request POST \
	--form "text=${MESSAGE}" \
	--form "chat_id=${TG_CHAT_ID}" \
	--form "message_thread_id=${THREAD_ID}" \
	--form 'link_preview_options={"is_disabled":true}' \
	"https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
	>/dev/null ||
	echo "WARNING: Failed to send notification"
