\set MODE RECURSIVE
\set invocations 1024
CALL dbgen(sf => 0.01);
DROP TYPE IF EXISTS linenumber CASCADE;
CREATE TYPE linenumber AS int;
