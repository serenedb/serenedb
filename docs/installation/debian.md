---
title: Debian / Ubuntu
sidebar_position: 3
---

import DocCallout from "@site/src/components/DocCallout";

# Install on Debian / Ubuntu

The `.deb` package runs SereneDB as a background service. Best for a Debian or Ubuntu server.

## Install

Download the latest `.deb` for your architecture from [GitHub Releases](https://github.com/serenedb/serenedb/releases) and install it:

```sh
sudo apt install ./serenedb_*.deb
```

Using `apt install` (rather than `dpkg -i`) pulls in the required `postgresql-client` and `tzdata` packages automatically.

## Start the service

```sh
sudo systemctl enable --now serenedb
```

SereneDB now starts on boot and runs as the dedicated `serenedb` user.

Your server is running. Continue with the [Quick Start](../quick-start.md) to connect and run your first query.

## Connect from another machine

By default SereneDB listens on `127.0.0.1` and accepts connections only from the same server. To allow remote clients, add the `--server_endpoints` flag to the service with a systemd drop-in:

```sh
sudo systemctl edit serenedb
```

In the editor that opens, add:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/serened --server_endpoints=pgsql+tcp://0.0.0.0:7890
```

The empty `ExecStart=` line clears the original command before setting the new one. Save and restart:

```sh
sudo systemctl restart serenedb
```

<DocCallout type="attention">
This exposes SereneDB on every network interface. Put a firewall in front of it on a public network.
</DocCallout>
