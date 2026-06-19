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
sed -i 's|^--server_endpoints=.*|--server_endpoints=pgsql+tcp://0.0.0.0:7890|' /etc/serenedb/serened.conf

# Apply the new endpoint (postinst already started the service)
systemctl restart serenedb
