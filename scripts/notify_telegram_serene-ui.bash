#!/bin/bash
###############################################################################
# Telegram Build Notification Script
#
# Sends build status notifications to a Telegram chat/thread.
# Safe for public repositories - no sensitive data exposed in logs.
#
# Required Environment Variables:
#   TG_TOKEN            - Telegram bot API token
#   TG_CHAT_ID          - Target chat ID
#   TG_THREAD_ID_MAIN   - Thread ID for main branch notifications
#   TG_THREAD_ID_BUILD  - Thread ID for other branch notifications
#   JOB_STATUS          - Overall build status (success/failure/cancelled)
#   BRANCH              - Git branch name
#   ACTOR               - User who triggered the build (GitHub username)
#   WORKFLOW            - Workflow name
#   RUN_NUMBER          - Build number
#   RUN_URL             - URL to the build
#
# Optional Environment Variables:
#   TG_USER_MAP         - JSON mapping of GitHub usernames to Telegram usernames
#                         Example: {"octocat":"john_doe_tg","user2":"tg_user2"}
#   JOB_STATUS_DOCKER   - Result of the build-docker job
#   JOB_STATUS_LINUX    - Result of the build-linux-app job
#   JOB_STATUS_WINDOWS  - Result of the build-windows-app job
#   JOB_STATUS_MACOS    - Result of the build-macos-app job
#   JOB_STATUS_RELEASE  - Result of the release job
#
###############################################################################

set -euo pipefail

#------------------------------------------------------------------------------
# Validate Required Environment Variables
#------------------------------------------------------------------------------

required_vars=(
  "TG_TOKEN"
  "TG_CHAT_ID"
  "TG_THREAD_ID_MAIN"
  "TG_THREAD_ID_BUILD"
  "JOB_STATUS"
  "BRANCH"
  "ACTOR"
  "WORKFLOW"
  "RUN_NUMBER"
  "RUN_URL"
)

missing_vars=()
for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    missing_vars+=("$var")
  fi
done

if [[ ${#missing_vars[@]} -gt 0 ]]; then
  echo "ERROR: Missing required environment variables:"
  printf '  - %s\n' "${missing_vars[@]}"
  exit 1
fi

#------------------------------------------------------------------------------
# Resolve Telegram Username
#------------------------------------------------------------------------------

TG_USERNAME="${ACTOR}"

if [[ -n "${TG_USER_MAP:-}" ]]; then
  if command -v jq &>/dev/null; then
    MAPPED_USER=$(echo "${TG_USER_MAP}" | jq -r --arg user "${ACTOR}" '.[$user] // empty' 2>/dev/null || true)
  else
    # Fallback: simple pattern matching for JSON like {"user":"tg_user"}
    MAPPED_USER=$(echo "${TG_USER_MAP}" | grep -o "\"${ACTOR}\":\"[^\"]*\"" | sed 's/.*:"$[^"]*$"/\1/' 2>/dev/null || true)
  fi

  if [[ -n "${MAPPED_USER}" ]]; then
    TG_USERNAME="${MAPPED_USER}"
    # Mask the Telegram username so it won't appear in logs
    echo "::add-mask::${TG_USERNAME}"
  fi
fi

#------------------------------------------------------------------------------
# Determine Status Emoji and Text
#------------------------------------------------------------------------------

case "${JOB_STATUS}" in
success)
  EMOJI="✅"
  RESULT="SUCCESS"
  ;;
cancelled)
  EMOJI="⚠️"
  RESULT="CANCELLED"
  ;;
*)
  EMOJI="💥"
  RESULT="FAILURE"
  ;;
esac

#------------------------------------------------------------------------------
# Determine Target Thread
#------------------------------------------------------------------------------

if [[ "${BRANCH}" == "main" ]]; then
  THREAD_ID="${TG_THREAD_ID_MAIN}"
else
  THREAD_ID="${TG_THREAD_ID_BUILD}"
fi

#------------------------------------------------------------------------------
# Helper: map a job result string to an emoji
#------------------------------------------------------------------------------

job_status_emoji() {
  case "${1:-}" in
  success) echo "✅" ;;
  failure) echo "💥" ;;
  cancelled) echo "⚠️" ;;
  skipped) echo "⏭️" ;;
  *) echo "➖" ;;
  esac
}

#------------------------------------------------------------------------------
# Build Message
#------------------------------------------------------------------------------

MESSAGE="${EMOJI} @${TG_USERNAME} ${RESULT}: BRANCH=${BRANCH} ${WORKFLOW} #${RUN_NUMBER} ${RUN_URL}"

# Append per-job status lines when the variables are provided
JOB_LINES=""

declare -A JOB_MAP=(
  ["Docker"]="${JOB_STATUS_DOCKER:-}"
  ["Linux"]="${JOB_STATUS_LINUX:-}"
  ["Windows"]="${JOB_STATUS_WINDOWS:-}"
  ["macOS"]="${JOB_STATUS_MACOS:-}"
  ["Release"]="${JOB_STATUS_RELEASE:-}"
)

# Preserve a consistent display order
JOB_ORDER=("Docker" "Linux" "Windows" "macOS" "Release")

for job_name in "${JOB_ORDER[@]}"; do
  job_result="${JOB_MAP[$job_name]}"
  if [[ -n "${job_result}" ]]; then
    job_emoji=$(job_status_emoji "${job_result}")
    JOB_LINES+=$'\n'"  ${job_emoji} ${job_name}: ${job_result}"
  fi
done

if [[ -n "${JOB_LINES}" ]]; then
  MESSAGE+=$'\n'"Jobs:${JOB_LINES}"
fi

#------------------------------------------------------------------------------
# Send Notification
#------------------------------------------------------------------------------

echo "Sending Telegram notification..."
echo "  Status:  ${RESULT}"
echo "  Branch:  ${BRANCH}"
echo "  Actor:   ${ACTOR} (GitHub)"

curl \
  --silent \
  --show-error \
  --location \
  --request POST \
  --form "text=${MESSAGE}" \
  --form "chat_id=${TG_CHAT_ID}" \
  --form "message_thread_id=${THREAD_ID}" \
  --form 'link_preview_options={"is_disabled":true}' \
  "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
  >/dev/null ||
  echo "WARNING: Failed to send Telegram notification"

echo "Done."
