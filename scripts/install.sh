#!/bin/bash -e
# SereneDB installer - https://install.serenedb.com
# Usage: curl -fsSL https://install.serenedb.com | bash
# Override version: SERENEDB_VERSION=26.04.4 curl -fsSL https://install.serenedb.com | bash

main() {
	OS=$(uname -s)
	ARCH=$(uname -m)

	command -v curl >/dev/null 2>&1 || {
		echo >&2 "Required tool curl could not be found. Aborting."
		exit 1
	}
	command -v tar >/dev/null 2>&1 || {
		echo >&2 "Required tool tar could not be found. Aborting."
		exit 1
	}
	command -v gzip >/dev/null 2>&1 || {
		echo >&2 "Required tool gzip could not be found. Aborting."
		exit 1
	}

	# Fetch latest version from GitHub releases API
	LATEST_VER=$(curl -fsSL https://api.github.com/repos/serenedb/serenedb/releases/latest |
		grep '"tag_name"' |
		sed 's/.*"tag_name": *"v\{0,1\}\([^"]*\)".*/\1/')

	if [ -z "${LATEST_VER}" ]; then
		echo >&2 "Could not determine latest SereneDB version from GitHub. Aborting."
		exit 1
	fi

	if [ -z "${SERENEDB_VERSION}" ]; then
		VER="${LATEST_VER}"
	else
		VER="${SERENEDB_VERSION}"
	fi

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
	echo "*** SereneDB installation script, version ${VER} ***"
	echo

	# Resolve architecture
	DIST=
	if [ "${OS}" = "Linux" ]; then
		if [ "${ARCH}" = "x86_64" ] || [ "${ARCH}" = "amd64" ]; then
			DIST=amd64
		elif [ "${ARCH}" = "aarch64" ] || [ "${ARCH}" = "arm64" ]; then
			DIST=arm64
		fi
	fi

	if [ -z "${DIST}" ]; then
		echo "SereneDB native binaries are not available for your platform (${OS}/${ARCH})."
		echo
		echo "You can run SereneDB via Docker:"
		echo "  docker run -it serenedb/serenedb"
		echo
		echo "See https://hub.docker.com/r/serenedb/serenedb for details."
		exit 0
	fi

	install_tar "${VER}" "${DIST}"
}

install_tar() {
	VER=$1
	DIST=$2

	URL="https://github.com/serenedb/serenedb/releases/download/v${VER}/serenedb-${VER}-linux-${DIST}.tar.gz"
	PREFIX="${HOME}/.serenedb/server"
	INST="${PREFIX}/${VER}"
	LATEST="${PREFIX}/latest"

	echo "Installing to ${INST}..."
	echo

	if [ -f "${INST}/usr/bin/serened" ] && smoke_test_quiet "${INST}/usr/bin/serened"; then
		echo "SereneDB ${VER} is already installed at ${INST}/serened"
	else
		mkdir -p "${INST}" || {
			echo >&2 "Failed to create ${INST}. Aborting."
			exit 1
		}

		curl --fail --location --progress-bar "${URL}" |
			tar -xz -C "${INST}" --strip-components=1 || {
			echo >&2 "Failed to download or extract ${URL}. Aborting."
			exit 1
		}

		SERENED_BIN="${INST}/usr/bin/serened"
		if [ -z "${SERENED_BIN}" ]; then
			echo >&2 "Could not find 'serened' binary after extraction in ${INST}. Aborting."
			exit 1
		fi

		chmod a+x "${SERENED_BIN}"

		# Symlink to a predictable path within INST
		if [ "${SERENED_BIN}" != "${INST}/serened" ]; then
			ln -sf "${SERENED_BIN}" "${INST}/serened"
		fi
		smoke_test "${INST}/serened" "${VER}"
	fi

	# Manage latest symlink only when installing the latest release
	if [ "${VER}" = "${LATEST_VER}" ]; then
		rm -f "${LATEST}"
		ln -s "${INST}" "${LATEST}"
		echo "Updated symlink: ${LATEST}/serened -> ${INST}/serened"
	fi

	# Auto-symlink to ~/.local/bin if available and not already occupied
	LOCALBIN="${HOME}/.local/bin"
	if [ "${VER}" = "${LATEST_VER}" ] &&
		[ -d "${LOCALBIN}" ] &&
		[ -w "${LOCALBIN}" ] &&
		[ ! -f "${LOCALBIN}/serened" ]; then
		ln -s "${LATEST}/serened" "${LOCALBIN}/serened"
		echo "Also created symlink: ${LOCALBIN}/serened -> ${LATEST}/serened"
	fi

	echo
	echo "Hint: Append the following line to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
	if [ "${VER}" = "${LATEST_VER}" ]; then
		echo "  export PATH='${LATEST}':\$PATH"
	else
		echo "  export PATH='${INST}':\$PATH"
	fi
}

install_psql() {
	if command -v psql >/dev/null 2>&1; then
		echo "psql is already installed: $(command -v psql)"
		return
	fi

	if ! [ -t 0 ]; then
		echo "Non-interactive shell detected, skipping psql installation."
		return
	fi

	echo
	printf "Install psql to connect to SereneDB? [Y/n] "
	read -r REPLY </dev/tty
	case "${REPLY}" in
	[nN][oO] | [nN])
		echo "Skipping psql installation."
		echo "To connect manually: psql -h 127.0.0.1 -p 7890"
		return
		;;
	esac

	echo "Installing psql..."

	if command -v apt-get >/dev/null 2>&1; then
		apt-get install -y -qq --no-install-recommends postgresql-client
	elif command -v dnf >/dev/null 2>&1; then
		dnf install -y -q postgresql
	elif command -v apk >/dev/null 2>&1; then
		apk add --no-cache postgresql-client
	elif command -v zypper >/dev/null 2>&1; then
		zypper install -y postgresql
	else
		echo "Could not install psql automatically."
		echo "Install the postgresql-client package for your distro manually."
		return
	fi

	echo "psql installed successfully."
}

print_next_steps() {
	BIN=$1
	VER=$2
	echo
	echo "========================================"
	echo " SereneDB ${VER} is ready"
	echo "========================================"
	echo
	echo "Start the server:"
	echo "  ${BIN}"
	echo
	echo "Connect to it (in another terminal):"
	echo "  psql -h 127.0.0.1 -p 7890"
	echo
	echo "========================================"
}

smoke_test_quiet() {
	"$1" --version >/dev/null 2>&1
}

smoke_test() {
	BIN=$1
	VER=$2
	if ! smoke_test_quiet "${BIN}"; then
		echo >&2 "Installed binary at ${BIN} failed to run. Something went wrong."
		exit 1
	fi
	echo
	echo "Successfully installed SereneDB ${VER} to ${BIN}"
	install_psql
	print_next_steps "${BIN}" "${VER}"
}

main
