#!/bin/sh
# =============================================================================
# SereneDB Docker Setup Script
# Runs during image build to configure the environment
# =============================================================================
set -e

echo "=== SereneDB Docker Setup ==="

# Create required directories
echo "Creating directories..."

install -o root -g root -m 755 -d /var/lib/serenedb
install -o root -g root -m 777 -d /var/log/serenedb
mkdir -p /docker-entrypoint-initdb.d

# Patch configuration for container environment
echo "Patching configuration..."

CONFIG_FILE="/etc/serenedb/serened.conf"

if [ -f "$CONFIG_FILE" ]; then
  # Bind to all interfaces (required for container networking)
  sed -i -e 's~^endpoint.*8529$~endpoint = tcp://0.0.0.0:8529~' "$CONFIG_FILE"

  # Log to stdout instead of file (Docker best practice)
  sed -i -e 's!^$file\s*=\s*$.*!\1 -!' "$CONFIG_FILE"

  # Remove uid setting (allow running as arbitrary user)
  sed -i -e 's~^uid = .*$~~' "$CONFIG_FILE"
else
  echo "WARNING: Config file not found: $CONFIG_FILE"
fi

# Install rclone for backup functionality
echo "Installing rclone..."

RCLONE_VERSION="1.70.1"
RCLONE_ARCH="linux-amd64"
RCLONE_URL="https://github.com/rclone/rclone/releases/download/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-${RCLONE_ARCH}.zip"

apt-get update
apt-get install -y --no-install-recommends wget unzip

wget --no-check-certificate -q "$RCLONE_URL" -O /tmp/rclone.zip
unzip -q /tmp/rclone.zip -d /tmp
mv "/tmp/rclone-v${RCLONE_VERSION}-${RCLONE_ARCH}/rclone" /usr/sbin/
chmod +x /usr/sbin/rclone

# Cleanup
echo "Cleaning up..."

rm -rf /tmp/rclone*
apt-get purge -y --auto-remove wget unzip
apt-get clean
rm -rf /var/lib/apt/lists/*

echo "=== Setup complete ==="
