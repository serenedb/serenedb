#!/bin/bash
# Reap orphan docker artifacts from earlier interrupted test runs.
#
# When the wrapping shell of tests/{sqllogic,drivers}/run_in_docker.sh is
# terminated by SIGKILL (OOM-killer, CI hard-timeout, ssh drop, `kill -9`),
# the EXIT trap that normally calls `docker compose down` never fires and
# the prior PREFIX's stack stays alive forever. Repeated occurrences pile
# up "Up About an hour" / "Up 8 days" containers on shared dev boxes.
#
# Source this file from a `run_in_docker.sh` once `COMPOSE_FILE` is set and
# call `reap_stale_docker_orphans`. Safe to run concurrently from multiple
# users / parallel sessions: live runs are identified by their own
# `docker compose -p <prj>` process and skipped.
#
# Sweeps:
#   1. Compose-managed stacks (tests / serenedb-single / their network +
#      volumes) whose project label matches our 4-char PREFIX shape and
#      have no live `docker compose ... -p <prj>` owner.
#   2. Standalone test deps (MinIO / iceberg-rest / Ollama / ClickHouse /
#      postgres / Azurite) spawned by tests/sqllogic/run.sh via the mounted
#      docker socket -- they are NOT compose-owned, so `compose down
#      --remove-orphans` doesn't see them. Removed when non-Up
#      (Exited/Created/Dead) OR Up but older than the live-run cutoff (6h is
#      well beyond any real test wall time; concurrent runs finish in
#      minutes). Keep this list in step with run.sh's launch_* helpers: a
#      service missing from it is never swept and leaks forever.
#   3. Test-network leftovers from either pass that have no remaining
#      attached containers.

# Cutoff for "Up but clearly orphaned" standalone test containers. No real
# test run lasts this long; we use it only to detect SIGKILL escapees that
# never had their wrapping shell run cleanup.
: "${REAP_STALE_AGE_HOURS:=6}"

reap_stale_docker_orphans() {
	if ! command -v docker >/dev/null 2>&1; then
		return 0
	fi

	# Pass 1: orphan docker-compose projects with our PREFIX shape.
	local prj
	for prj in $(docker ps -a --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null |
		sort -u | grep -E '^[a-z0-9]{4}$'); do
		# Skip projects with a live `docker compose ... -p <prj>` parent process.
		if pgrep -af "docker[- ]compose([[:space:]]+-[a-zA-Z-]+[[:space:]]+[^[:space:]]+)*[[:space:]]+-p[[:space:]]+${prj}\b" \
			>/dev/null 2>&1; then
			continue
		fi
		docker compose -p "${prj}" -f "${COMPOSE_FILE:-/dev/null}" down --volumes --remove-orphans \
			>/dev/null 2>&1 || true
		# Fallback if the compose file path didn't match anything in the project
		# (different test kind, different cwd, etc): force-remove by labels.
		# `-v` is critical -- it strips anonymous volumes the images carry via
		# VOLUME directives (postgres /var/lib/postgresql/data is the common
		# one); without -v they linger and accumulate hundreds of MB.
		docker ps -aq --filter "label=com.docker.compose.project=${prj}" 2>/dev/null |
			xargs -r docker rm -fv >/dev/null 2>&1 || true
		# Compose-project named volumes get cleaned by `compose down --volumes`.
		# But the label-based fallback above doesn't, so sweep them here too.
		docker volume ls -q --filter "label=com.docker.compose.project=${prj}" 2>/dev/null |
			xargs -r docker volume rm >/dev/null 2>&1 || true
	done

	# Pass 2: standalone test deps spawned with mounted docker socket from
	# inside the tests container. Names follow:
	#   <4char>-serenedb-test-(minio|iceberg-rest|ollama|clickhouse|postgres|azurite)-<pid>
	local cutoff=$(($(date +%s) - REAP_STALE_AGE_HOURS * 3600))
	local name status created_raw created_ts
	while IFS=$'\t' read -r name status; do
		[[ -z "$name" ]] && continue
		# Non-Up containers are unambiguous orphans -- standalone deps are
		# never restarted between runs.
		# `-v` strips anonymous volumes the underlying image carries
		# (MinIO /data, etc) -- without it the volumes leak and add up to
		# 100s of MB per orphan.
		if [[ "$status" != Up* ]]; then
			docker rm -fv "$name" >/dev/null 2>&1 || true
			continue
		fi
		# Up but older than the cutoff: SIGKILL escapee. `{{.Created}}` is UTC
		# RFC3339 ("2026-08-26T21:17:36.927393608Z"); trimming the fractional
		# seconds and the Z leaves "2026-08-26 21:17:36", which `date -d` reads
		# everywhere -- where it rejects the full RFC3339 outside GNU, and rejects
		# `docker ps --format {{.CreatedAt}}` ("2026-08-26 23:17:36 +0200 CEST")
		# everywhere. TZ=UTC is required, not cosmetic: the trimmed string carries
		# no zone, so without it a UTC instant is read as local time and the
		# container measures hours older than it is.
		created_raw=$(docker inspect -f '{{.Created}}' "$name" 2>/dev/null)
		created_raw=${created_raw%%.*}
		created_raw=${created_raw%Z}
		# The emptiness guard is load-bearing: `date -d ""` does not fail, it
		# returns TODAY AT MIDNIGHT -- stale under any cutoff for most of the day.
		# This sweep used to read an unparsed timestamp as 1970 and force-remove
		# every live container it matched; an age we cannot establish now means
		# KEEP, since leaving an orphan for the next sweep is far cheaper than
		# deleting a concurrent run's container.
		# TODO(Dronplane): MacOS date has no -d, so this will make no cleanup.
		created_ts=""
		[[ -n "$created_raw" ]] &&
			created_ts=$(TZ=UTC date -d "${created_raw/T/ }" +%s 2>/dev/null)
		if [[ "$created_ts" =~ ^[0-9]+$ ]] && ((created_ts < cutoff)); then
			docker rm -fv "$name" >/dev/null 2>&1 || true
		fi
	done < <(docker ps -a \
		--format '{{.Names}}{{"\t"}}{{.Status}}' 2>/dev/null |
		grep -E '^[a-z0-9]{4}-serenedb-test-(minio|iceberg-rest|ollama|clickhouse|postgres|azurite)-[0-9]+\b')

	# Pass 3: test-networks left dangling. The compose down in pass 1 removes
	# its own; standalone-spawned networks (sqllogic local-network mode)
	# linger if their pass-2 owner died.
	local net
	for net in $(docker network ls --format '{{.Name}}' 2>/dev/null |
		grep -E '^([a-z0-9]{4}_test-network|[a-z0-9]{4}-serenedb-test-net-[0-9]+)$'); do
		if [[ -z "$(docker network inspect "$net" --format '{{range .Containers}}x{{end}}' 2>/dev/null)" ]]; then
			docker network rm "$net" >/dev/null 2>&1 || true
		fi
	done
}
