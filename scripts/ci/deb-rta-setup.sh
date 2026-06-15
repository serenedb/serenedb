#!/bin/bash
set -eo pipefail

# Wait for systemd to boot
systemctl is-system-running --wait || true

# Install the .deb
apt-get update -qq
apt-get install -y /workspace/"$DEB_PACKAGE"

# Configure for testing: listen on all interfaces so the tests container can reach us
sed -i 's|^--server_endpoints=.*|--server_endpoints=pgsql+tcp://0.0.0.0:7890|' /etc/serenedb/serened.conf

# Start the service
systemctl start serenedb
