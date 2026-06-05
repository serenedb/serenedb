# psql parity tests

Diffs `psql`'s backslash meta-commands against committed PG-derived goldens
in [`tests.test`](tests.test). Harness: [`run.py`](run.py).

## Run

    tests/drivers/run.sh --lang psql --host 127.0.0.1 --port 5432

## Refresh goldens (after psql/PG version bumps or new commands)

    docker run --rm -d --name pg-oracle --network=host \
      -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18.3

    SDB_DRV_HOST=127.0.0.1 SDB_DRV_PORT=5432 SDB_DRV_ENGINE=postgres \
      tests/drivers/psql/run.py --update

    docker rm -f pg-oracle

Review the diff, commit the updated `tests.test`.

## tests.test block syntax

    # comments attach to the next block
    [skipif <engine>]              # 0+; suppresses test on that engine
    [skip-line: <N>]               # 0+; drop line N from the diff
    \dt                            # the command; <NL> = newline
    ----
    > <golden line>                # each prefixed `> `; `>` alone = blank
