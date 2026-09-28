---
title: Debian / Ubuntu
sidebar_position: 3
split: page
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

By default SereneDB listens on `127.0.0.1` and accepts connections only from the same server. The service reads its flags from `/etc/serenedb/serened.conf`. To allow remote clients, change the `--listen` line there:

```ini
--listen=postgres://0.0.0.0:7890
```

`--listen` takes every endpoint in one comma-separated value, so keep any other endpoint in the same line. Then restart the service:

```sh
sudo systemctl restart serenedb
```

Remote logins need a password: the `postgres` role has none and is accepted only from the local machine. Set one before connecting from elsewhere, as [Security](../security/index.md) describes.

<DocCallout type="attention">
This exposes SereneDB on every network interface. Put a firewall in front of it on a public network.
</DocCallout>
