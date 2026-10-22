---
title: Docker
sidebar_position: 2
split: page
---

# Run with Docker

The easiest way to run SereneDB in a container or on macOS and Windows.

```sh
docker run -d --name serenedb -p 7890:7890 -e POSTGRES_PASSWORD=<password> -v serenedb-data:/var/lib/serenedb serenedb/serenedb
```

- `-p 7890:7890` exposes the database port.
- `-e POSTGRES_PASSWORD=<password>` sets the password of the `postgres` superuser when the data directory is first created. A connection through `-p` reaches the container from Docker's network, not over loopback, so the server asks for this password.
- `-v serenedb-data:/var/lib/serenedb` stores your data in a Docker-managed volume named `serenedb-data`, so it survives a restart. Leave it off for a throwaway container.

Connect with the password you set:

```sh
psql -h localhost -p 7890 -U postgres
```

For a throwaway local trial, `-e SERENEDB_HOST_AUTH_METHOD=trust` instead lets every connection in without a password, from any host that can reach the port.

## Manage the container

```sh
docker stop serenedb     # stop
docker start serenedb    # start again
docker rm serenedb       # remove
```

Use `serenedb/serenedb:<version>` (for example `26.04.4`) to pin a release instead of the latest one.

Your server is running. Continue with the [Quick Start](../quick-start.md) to run your first query.
