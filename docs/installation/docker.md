---
title: Docker
sidebar_position: 2
---

# Run with Docker

The easiest way to run SereneDB in a container or on macOS and Windows.

```sh
docker run -d --name serenedb -p 7890:7890 -v serenedb-data:/var/lib/serenedb serenedb/serenedb
```

- `-p 7890:7890` exposes the database port.
- `-v serenedb-data:/var/lib/serenedb` stores your data in a Docker-managed volume named `serenedb-data`, so it survives a restart. Leave it off for a throwaway container.

## Manage the container

```sh
docker stop serenedb     # stop
docker start serenedb    # start again
docker rm serenedb       # remove
```

Use `serenedb/serenedb:<version>` (for example `26.04.4`) to pin a release instead of the latest one.

Your server is running. Continue with the [Quick Start](../quick-start.md) to connect and run your first query.
