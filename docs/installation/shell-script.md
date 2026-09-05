---
title: Shell script
sidebar_position: 1
---

import DocCallout from "@site/src/components/DocCallout";

# Install with the shell script

The quickest way to install SereneDB on Linux. No root needed.

```sh
curl https://linux.serenedb.com | sh
```

This installs the `serened` command for your user. On macOS, Windows or an unsupported CPU the script stops and points you to [Docker](./docker.md).

<DocCallout type="tip">
If `serened` is not found afterwards, the script prints the one line to add to your `~/.bashrc` or `~/.zshrc`.
</DocCallout>

## Start the server

```sh
serened
```

The server listens on `127.0.0.1:7890` and stores its data in a `serenedb-data` folder in the current directory. To use a different location, pass it as an argument: `serened /var/lib/serenedb`.

Your server is running. Continue with the [Quick Start](../quick-start.md) to connect and run your first query.
