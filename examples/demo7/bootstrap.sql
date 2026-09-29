\if :{?openrouter_api_key}
\else
\echo 'Pass your OpenRouter API key: psql -v openrouter_api_key="$OPENROUTER_API_KEY" -f bootstrap.sql'
\quit
\endif
\timing on

DROP SECRET IF EXISTS openrouter;

CREATE SECRET openrouter (
    TYPE typesafe,
    base_url 'https://openrouter.ai/api',
    path '/alpha/decisions',
    api_key :'openrouter_api_key',
    model '~typesafe/jev-latest',
);

DROP TABLE IF EXISTS tickets;

CREATE TABLE tickets (
    id        INTEGER PRIMARY KEY,
    customer  VARCHAR,
    plan      VARCHAR,
    opened_at TIMESTAMP,
    status    VARCHAR DEFAULT 'open',
    subject   VARCHAR,
    body      VARCHAR,
    triage    GENERATED ALWAYS AS (ai_system_one(
        body,
        questions := {
            refund: {
                type: 'noul',
                instructions: 'Does the customer ask for a refund or a reversed charge?',
            },
            team: {
                type: 'choice',
                instructions: 'Which team should handle this ticket?',
                criteria: ['billing', 'technical', 'sales', 'security', 'feedback'],
            },
            urgency: {
                type: 'score',
                instructions: 'How urgent is this ticket for the support team?',
                criteria: ['low', 'medium', 'high', 'critical'],
            },
        },
        secret_name := 'openrouter',
    )) STORED,
);
