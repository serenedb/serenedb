#!/bin/bash
set -eo pipefail

# Wait for systemd to boot
systemctl is-system-running --wait || true

# Drop the base-image policy-rc.d stub (exit 101) so the package maintainer
# scripts manage the service through systemd like on a real host
rm -f /usr/sbin/policy-rc.d

# Install the .deb (postinst auto-starts serenedb on the default endpoint)
apt-get update -qq
apt-get install -y /workspace/"$DEB_PACKAGE"

# Configure for testing: listen on all interfaces so the tests container can reach us
sed -i 's|^--listen=.*|--listen=postgres://0.0.0.0:7890|' /etc/serenedb/serened.conf

# On slow ARM cores catalog-DDL bursts starve new connections' startup/auth past
# the 30s slowloris deadline, which closes them without a log line and fails the
# sqllogic step with `connection closed`
if [[ "$(uname -m)" == "aarch64" ]]; then
	echo '--auth_timeout=600s' >>/etc/serenedb/serened.conf
fi

# Apply the new endpoint (postinst already started the service)
systemctl restart serenedb
