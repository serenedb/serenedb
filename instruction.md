build code - ~/projects/serenedb/build$ ninja
launch database - ~/projects/serenedb$ ./build/bin/serened ./build_dir --server.endpoint='pgsql+tcp://0.0.0.0:7778'
launch tests (specify dir) -  ~/projects/serenedb$ ./tests/sqllogic/run.sh --single-port 7777 --test 'tests/sqllogic/any/pg/simple/ctas.test' --debug true 
connect via psql - psql -h localhost -p 7777 -U postgres
