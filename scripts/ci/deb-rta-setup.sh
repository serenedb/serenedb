#!/bin/bash
set -e

# Wait for systemd to boot (we're exec'd by systemd as a oneshot service)
for i in $(seq 1 30); do
	systemctl is-system-running --wait 2>/dev/null | grep -qE "running|degraded" && break
	sleep 1
done

# Install the .deb
apt-get update -qq
apt-get install -y /workspace/"$DEB_PACKAGE"

# Configure for testing
sed -i 's|^endpoint.*|endpoint = pgsql+tcp://0.0.0.0:7890|' /etc/serenedb/serened.conf
echo -e '\n[server]\nauthentication = false' >>/etc/serenedb/serened.conf

# Start the service
systemctl start serenedb
