#!/bin/bash
N=pgseq-probe-$$
docker run -d --rm --name $N -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18.3 >/dev/null
for i in $(seq 1 60); do docker exec $N pg_isready -U postgres >/dev/null 2>&1 && break; sleep 0.5; done
q() { docker exec $N psql -U postgres -Atqc "$1"; }
sleep 1
q "CREATE SEQUENCE s"
q "CREATE SEQUENCE c CACHE 10"
echo "plain sequence (CACHE 1): after N nextval calls -> last_value | log_cnt"
for n in 1 2 16 32 33 34; do
  while [ "$(q "SELECT coalesce((SELECT last_value FROM s WHERE is_called), 0)")" -lt $n ]; do q "SELECT nextval('s')" >/dev/null; done
  echo "  $n: $(q "SELECT last_value || ' | ' || log_cnt FROM s")"
done
q "SELECT nextval('c')" >/dev/null
echo "CACHE 10 sequence, after one nextval: last_value | log_cnt = $(q "SELECT last_value || ' | ' || log_cnt FROM c")"
q "CHECKPOINT"
q "SELECT nextval('s')" >/dev/null
echo "plain sequence, first nextval after CHECKPOINT: $(q "SELECT last_value || ' | ' || log_cnt FROM s")"
docker kill $N >/dev/null
