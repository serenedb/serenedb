#!/bin/bash
set -eo pipefail

# Wait for systemd to boot
systemctl is-system-running --wait || true

# Install the .deb
apt-get update -qq
apt-get install -y /workspace/"$DEB_PACKAGE"

# Configure for testing
sed -i 's|^endpoint.*|endpoint = pgsql+tcp://0.0.0.0:7890|' /etc/serenedb/serened.conf
echo -e '\n[server]\nauthentication = false' >>/etc/serenedb/serened.conf

# Start the service
systemctl start serenedb
