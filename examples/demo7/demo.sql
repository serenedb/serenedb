\timing on
\if :{?jev_secret}
\else
\set jev_secret kev
\endif

SET sdb_ai_system1_default_secret = :'jev_secret';
SET sdb_ai_max_concurrent_requests = 1;

\echo === [S1] the first tickets arrive: the INSERT triages every row it writes ===
INSERT INTO tickets (id, customer, plan, opened_at, subject, body) VALUES
  (1, 'Acme Corp',       'enterprise', '2026-09-01 08:12', 'Charged twice for August',
   'Our card was charged twice for the August invoice. Please refund the duplicate payment.'),
  (2, 'Birch Labs',      'pro',        '2026-09-01 09:40', 'API returning 500',
   'Since 9am every call to /v2/orders returns HTTP 500. Our checkout is down.'),
  (3, 'Cobalt Inc',      'free',       '2026-09-02 11:05', 'Pricing for 50 seats',
   'We are a team of 50 people. Is there a volume discount on the Pro plan?'),
  (4, 'Delta Logistics', 'enterprise', '2026-09-02 14:30', 'Third outage this month',
   'This is the third outage this month. If it happens again we will move to another vendor at renewal.'),
  (5, 'Echo Media',      'pro',        '2026-09-03 10:00', 'Exporting dashboards',
   'Where can I export my dashboards as CSV?'),
  (6, 'Fjord Health',    'enterprise', '2026-09-03 16:45', 'Wrong VAT number',
   'The VAT number on our last invoice is wrong. We need a corrected invoice for our accounting.'),
  (7, 'Gamma Retail',    'pro',        '2026-09-04 07:55', 'SSO login loop',
   'After the update, SSO login redirects back to the login page forever. Nobody on our team can sign in.'),
  (8, 'Helix Bio',       'free',       '2026-09-04 12:20', 'Billed after cancelling',
   'I cancelled in July but was still billed for August and September. I want my money back.')
RETURNING id, subject, triage.team.choice AS team, round(triage.urgency.score, 2) AS urgency;

\echo === [S2] the queue reads the stored answers and sends no requests ===
SELECT id, customer, subject,
       triage.team.choice AS team,
       round(triage.urgency.score, 2) AS urgency,
       round(triage.refund, 3) AS refund
FROM tickets
WHERE status = 'open'
ORDER BY triage.urgency.score DESC, opened_at;

\echo === [S3] more tickets arrive; the empty one gets NULL answers and costs no request ===
INSERT INTO tickets (id, customer, plan, opened_at, subject, body) VALUES
  (9,  'Iris Studio',   'pro',        '2026-09-05 09:10', 'Dark mode',
   'Would love a dark mode for the editor. Not urgent, just a wish.'),
  (10, 'Juno Finance',  'enterprise', '2026-09-05 13:35', 'Data residency',
   'Before we sign the contract we need to confirm that our data stays in the EU region.'),
  (11, 'Kite Travel',   'pro',        '2026-09-06 08:25', 'Webhooks arrive late',
   'Webhooks arrive 20 to 30 minutes late since yesterday, so bookings are not confirmed in time.'),
  (12, 'Lumen Energy',  'enterprise', '2026-09-06 15:00', 'Switch to annual billing',
   'We would like to switch from monthly to annual billing and add 20 seats.'),
  (13, 'Mosaic Foods',  'free',       '2026-09-07 10:30', 'Empty ticket',
   NULL),
  (14, 'Nova Games',    'pro',        '2026-09-07 17:10', 'Thank you',
   'Just wanted to say the new release is great. Keep it up!'),
  (15, 'Orbit Systems', 'enterprise', '2026-09-08 06:50', 'Unknown logins',
   'We see logins to our account from a country where we have no staff. Please lock the account and investigate.'),
  (16, 'Pine Legal',    'pro',        '2026-09-08 11:15', 'Charged after downgrade',
   'I downgraded to the Free plan last week but was still charged for Pro. Please reverse the charge.')
RETURNING id, subject, triage.team.choice AS team, round(triage.urgency.score, 2) AS urgency;

\echo === [S4] a customer rewrites a ticket: the UPDATE re-triages that row in place ===
SELECT id, subject, triage.team.choice AS team, round(triage.urgency.score, 2) AS urgency
FROM tickets
WHERE id = 9;

UPDATE tickets
SET subject = 'Editor crashes on large files',
    body = 'The editor crashes every time I open a file larger than 10 MB, and I lost an hour of work today.'
WHERE id = 9
RETURNING id, subject, triage.team.choice AS team, round(triage.urgency.score, 2) AS urgency;

\echo === [S5] billing refunds and closes its tickets; the text is unchanged, so no requests ===
UPDATE tickets
SET status = 'closed'
WHERE triage.team.choice = 'billing' AND triage.refund > 0.5
RETURNING id, subject, round(triage.refund, 3) AS refund;

\echo === [S6] a new field, filled in place for the enterprise tickets only ===
ALTER TABLE tickets ADD COLUMN churn_risk DOUBLE;

UPDATE tickets
SET churn_risk = ai_system1(body, 'Is this customer at risk of leaving for another vendor?',
                            batch_size := 1)
WHERE plan = 'enterprise';

SELECT id, customer, subject, round(churn_risk, 3) AS churn_risk
FROM tickets
WHERE churn_risk IS NOT NULL
ORDER BY churn_risk DESC;

\echo === [S7] open workload per team, from the stored answers ===
SELECT triage.team.choice AS team,
       count(*) AS tickets,
       round(avg(triage.urgency.score), 2) AS avg_urgency,
       count(*) FILTER (WHERE triage.refund > 0.5) AS refunds
FROM tickets
WHERE status = 'open' AND triage IS NOT NULL
GROUP BY team
ORDER BY tickets DESC, team;
