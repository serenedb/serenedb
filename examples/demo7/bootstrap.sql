\timing on
\if :{?jev_secret}
\else
\set jev_secret kev
\endif

DROP SECRET IF EXISTS kev;
DROP SECRET IF EXISTS typesafe;
DROP SECRET IF EXISTS openrouter;

CREATE SECRET kev (
    TYPE typesafe,
    base_url 'http://localhost:8009'
  );

CREATE SECRET typesafe (
    TYPE typesafe,
    api_key 'API_KEY'
  );

CREATE SECRET openrouter (
    TYPE typesafe,
    base_url 'https://openrouter.ai/api',
    path '/alpha/decisions',
    api_key 'OPENROUTER_API_KEY',
    model '~typesafe/jev-latest'
  );

\echo === tickets are triaged through the :jev_secret secret ===
DROP TABLE IF EXISTS tickets;

CREATE TABLE tickets (
  id        INTEGER PRIMARY KEY,
  customer  VARCHAR,
  plan      VARCHAR,
  opened_at TIMESTAMP,
  status    VARCHAR DEFAULT 'open',
  subject   VARCHAR,
  body      VARCHAR,
  triage    GENERATED ALWAYS AS (ai_system1(body, questions := {
              refund:  {type: 'noul',
                        instructions: 'Does the customer ask for a refund or a reversed charge?'},
              team:    {type: 'choice',
                        instructions: 'Which team should handle this ticket?',
                        criteria: ['billing', 'technical', 'sales', 'security', 'feedback']},
              urgency: {type: 'score',
                        instructions: 'How urgent is this ticket for the support team?',
                        criteria: ['low', 'medium', 'high', 'critical']}
            }, secret_name := :'jev_secret')) STORED
);
