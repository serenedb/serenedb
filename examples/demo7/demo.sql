\timing on
\if :{?jev_secret}
\else
\set jev_secret kev
\endif

SET sdb_ai_jev_default_secret = :'jev_secret';
SET sdb_ai_max_concurrent_requests = 1;
\echo === Jev secret: :jev_secret ===

\echo === [Q1] noul: which tickets ask for money back ===
SELECT id, subject,
       round(prompt_jev(body, 'Does the customer ask for a refund or a reversed charge?',
                        batch_size := 1), 3) AS refund_probability
FROM support_tickets
ORDER BY refund_probability DESC NULLS LAST;

\echo === [Q2] choice: route every ticket to a team ===
SELECT id, subject, (routing).choice AS team, round((routing).confidence, 3) AS confidence
FROM (
  SELECT id, subject,
         prompt_jev(body, 'Which team should handle this ticket?',
                    choice := [
                      {label: 'billing',   description: 'Payments, invoices, refunds and plan changes'},
                      {label: 'technical', description: 'Errors, outages, bugs and integrations'},
                      {label: 'sales',     description: 'Pricing, new seats, contracts and pre-sales questions'},
                      {label: 'security',  description: 'Suspicious access, data protection and compliance'},
                      {label: 'feedback',  description: 'Praise, feature requests and general comments'}
                    ],
                    batch_size := 1) AS routing
  FROM support_tickets
) t
ORDER BY id;

\echo === [Q3] score: urgency of the enterprise tickets on an ordered scale ===
SELECT id, subject, round((urgency).score, 2) AS urgency, (urgency).probabilities
FROM (
  SELECT id, subject,
         prompt_jev(body, 'How urgent is this ticket for the support team?',
                    score := ['low', 'medium', 'high', 'critical'],
                    batch_size := 1) AS urgency
  FROM support_tickets
  WHERE plan = 'enterprise'
) t
ORDER BY urgency DESC;

\echo === [Q4] enterprise customers at risk of leaving; other plans are never sent ===
SELECT id, customer, subject
FROM support_tickets
WHERE plan = 'enterprise'
  AND prompt_jev(body, 'Is this customer at risk of leaving for another vendor?',
                 batch_size := 1) > 0.6
ORDER BY id;

\echo === [Q5] triage every ticket once: three questions per request, stored in a table ===
DROP TABLE IF EXISTS ticket_triage;
CREATE TABLE ticket_triage AS
SELECT id,
       (answer).refund AS refund,
       (answer).team.choice AS team,
       (answer).team.confidence AS team_confidence,
       (answer).urgency.score AS urgency
FROM (
  SELECT id,
         prompt_jev(body, questions := {
           refund:  {type: 'noul',
                     instructions: 'Does the customer ask for a refund or a reversed charge?'},
           team:    {type: 'choice',
                     instructions: 'Which team should handle this ticket?',
                     criteria: ['billing', 'technical', 'sales', 'security', 'feedback']},
           urgency: {type: 'score',
                     instructions: 'How urgent is this ticket for the support team?',
                     criteria: ['low', 'medium', 'high', 'critical']}
         }) AS answer
  FROM support_tickets
) t;

SELECT id, round(refund, 3) AS refund, team, round(team_confidence, 3) AS team_confidence,
       round(urgency, 2) AS urgency
FROM ticket_triage
ORDER BY id;

\echo === [Q6] workload per team, from the stored answers ===
SELECT team,
       count(*) AS tickets,
       round(avg(urgency), 2) AS avg_urgency,
       count(*) FILTER (WHERE refund > 0.5) AS refunds
FROM ticket_triage
WHERE team IS NOT NULL
GROUP BY team
ORDER BY tickets DESC, team;

\echo === [Q7] the three least certain routings go to a human ===
SELECT s.id, s.subject, t.team, round(t.team_confidence, 3) AS confidence
FROM ticket_triage t
JOIN support_tickets s USING (id)
WHERE t.team IS NOT NULL
ORDER BY t.team_confidence
LIMIT 3;

\echo === [Q8] enterprise queue: urgent tickets first, oldest first within a level ===
SELECT s.id, s.customer, s.opened_at, t.team, round(t.urgency, 2) AS urgency
FROM ticket_triage t
JOIN support_tickets s USING (id)
WHERE s.plan = 'enterprise'
ORDER BY round(t.urgency) DESC, s.opened_at;
