#!/bin/sh -e
# SereneDB quick-start with Docker -- https://install.serenedb.com
# Usage: curl -fsSL https://install.serenedb.com | sh
# Override tags:
#   SERENEDB_TAG=v1.2.3 SERENE_UI_TAG=v0.5.0 PSQL_TAG=16 \
#     curl -fsSL https://quick-docker.serenedb.com | sh

SERENEDB_TAG="${SERENEDB_TAG:-latest}"
SERENE_UI_TAG="${SERENE_UI_TAG:-latest}"
PSQL_TAG="${PSQL_TAG:-latest}"

DIR="/tmp/serenedb-quick"
COMPOSE_FILE="${DIR}/docker-compose.yml"

main() {
	echo
	echo "             -============-             "
	echo "          ====:.............==          "
	echo "       =====...................==       "
	echo "     =====............         ..-=     "
	echo "    =====...........             ..=    "
	echo "  -=====-...........              ..=-  "
	echo "  =====+............               ..=  "
	echo " ======+............               ...= "
	echo "=======+.............              ....="
	echo "=======+..............            .....="
	echo "=======++...............         ......="
	echo "========+..............................="
	echo "=========+............................-="
	echo "===========-..........................=="
	echo " ============........................== "
	echo "  =============....................-==  "
	echo "  -===============...............====-  "
	echo "    ===================.....:=======    "
	echo "     ==============================     "
	echo "       ==========================       "
	echo "          ====================          "
	echo "             -============-             "
	echo
	echo "*** SereneDB quick-start (Docker) ***"
	echo

	check_docker
	write_compose
	start_stack
	print_next_steps
}

# ── Docker preflight ──────────────────────────────────────────────────────────

check_docker() {
	command -v docker >/dev/null 2>&1 || {
		echo >&2 "Docker is not installed."
		echo >&2 "Install Docker first: https://docs.docker.com/get-docker/"
		exit 1
	}
	docker info >/dev/null 2>&1 || {
		echo >&2 "Docker daemon is not running. Start it and try again."
		exit 1
	}
	docker compose version >/dev/null 2>&1 || {
		echo >&2 "Docker Compose v2 is required (docker compose subcommand)."
		echo >&2 "Please upgrade Docker: https://docs.docker.com/engine/install/"
		exit 1
	}
}

# ── Compose file ──────────────────────────────────────────────────────────────

write_compose() {
	mkdir -p "${DIR}"

	cat >"${COMPOSE_FILE}" <<EOF
services:
  serenedb:
    image: serenedb/serenedb:${SERENEDB_TAG}
    restart: on-failure
    stop_grace_period: 5s
    ports:
      - "7890-7895:7890"

  serene-ui:
    image: serenedb/serene-ui:${SERENE_UI_TAG}
    restart: on-failure
    stop_grace_period: 5s
    stop_signal: SIGINT
    ports:
      - "6543-6548:6543"
    environment:
      DEFAULT_CONNECTION_NAME: local
      DEFAULT_CONNECTION_STRING: postgres://postgres@serenedb:7890/postgres
    depends_on:
      - serenedb

  psql:
    image: alpine/psql:${PSQL_TAG}
    profiles: ["tools"]
    entrypoint: ["psql", "-h", "serenedb", "-p", "7890", "-U", "postgres"]
EOF

	echo "Compose file written to: ${COMPOSE_FILE}"
}

# ── Stack lifecycle ───────────────────────────────────────────────────────────

start_stack() {
	echo "Starting SereneDB and SereneUI..."
	docker compose -f "${COMPOSE_FILE}" up -d
}

# ── Hyperlink helper ──────────────────────────────────────────────────────────

# Prints an OSC 8 hyperlink; degrades to plain URL on unsupported terminals
hyperlink() {
	url="$1"
	printf '\e]8;;%s\e\\%s\e]8;;\e\\\n' "${url}" "${url}"
}

# ── Next steps ────────────────────────────────────────────────────────────────

print_next_steps() {
	UI_PORT=$(docker compose -f "${COMPOSE_FILE}" port serene-ui 6543 | cut -d: -f2)
	echo
	echo "========================================"
	echo " SereneDB is up and running"
	echo "========================================"
	echo
	echo "Open SereneUI in your browser:"
	hyperlink "http://localhost:${UI_PORT}"
	echo
	echo "Connect with psql in your terminal:"
	echo "  docker compose -f ${COMPOSE_FILE} run --rm psql"
	echo
	echo "----------------------------------------"
	echo "When you're done playing around:"
	echo "  docker compose -f ${COMPOSE_FILE} down -t 5 --remove-orphans"
	echo
	echo "To also remove the generated files:"
	echo "  docker compose -f ${COMPOSE_FILE} down -t 5 --remove-orphans && rm -rf ${DIR}"
	echo "========================================"
}

main
