---
title: Tarball
sidebar_position: 4
---

# Install from a tarball

A self-contained build you can unpack anywhere. Useful for offline servers or custom setups.

## Download and unpack

Download `serenedb-<version>-linux-<arch>.tar.gz` for your architecture (`amd64` or `arm64`) from [GitHub Releases](https://github.com/serenedb/serenedb/releases) and unpack it:

```sh
tar -xzf serenedb-*-linux-*.tar.gz
```

This creates a `serenedb-<version>-linux-<arch>/` directory with the binary under `usr/bin`. Add that directory to your `PATH` (use the real directory name):

```sh
export PATH="$PWD/serenedb-<version>-linux-<arch>/usr/bin:$PATH"
```

Add the `export` line to your `~/.bashrc` or `~/.zshrc` to keep it after you close the terminal.

## Start the server

```sh
serened
```

The server listens on `127.0.0.1:7890` and stores its data in a `serenedb-data` folder in the current directory. To use a different location, pass it as an argument: `serened /var/lib/serenedb`.

Your server is running. Continue with the [Quick Start](../quick-start.md) to connect and run your first query.
