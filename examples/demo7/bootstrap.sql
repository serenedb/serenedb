\timing on

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

DROP TABLE IF EXISTS ticket_triage;
DROP TABLE IF EXISTS support_tickets;

CREATE TABLE support_tickets (
  id        INTEGER PRIMARY KEY,
  customer  VARCHAR,
  plan      VARCHAR,
  opened_at TIMESTAMP,
  subject   VARCHAR,
  body      VARCHAR
);

INSERT INTO support_tickets VALUES
  (1,  'Acme Corp',       'enterprise', '2026-09-01 08:12', 'Charged twice for August',
   'Our card was charged twice for the August invoice. Please refund the duplicate payment.'),
  (2,  'Birch Labs',      'pro',        '2026-09-01 09:40', 'API returning 500',
   'Since 9am every call to /v2/orders returns HTTP 500. Our checkout is down.'),
  (3,  'Cobalt Inc',      'free',       '2026-09-02 11:05', 'Pricing for 50 seats',
   'We are a team of 50 people. Is there a volume discount on the Pro plan?'),
  (4,  'Delta Logistics', 'enterprise', '2026-09-02 14:30', 'Third outage this month',
   'This is the third outage this month. If it happens again we will move to another vendor at renewal.'),
  (5,  'Echo Media',      'pro',        '2026-09-03 10:00', 'Exporting dashboards',
   'Where can I export my dashboards as CSV?'),
  (6,  'Fjord Health',    'enterprise', '2026-09-03 16:45', 'Wrong VAT number',
   'The VAT number on our last invoice is wrong. We need a corrected invoice for our accounting.'),
  (7,  'Gamma Retail',    'pro',        '2026-09-04 07:55', 'SSO login loop',
   'After the update, SSO login redirects back to the login page forever. Nobody on our team can sign in.'),
  (8,  'Helix Bio',       'free',       '2026-09-04 12:20', 'Billed after cancelling',
   'I cancelled in July but was still billed for August and September. I want my money back.'),
  (9,  'Iris Studio',     'pro',        '2026-09-05 09:10', 'Dark mode',
   'Would love a dark mode for the editor. Not urgent, just a wish.'),
  (10, 'Juno Finance',    'enterprise', '2026-09-05 13:35', 'Data residency',
   'Before we sign the contract we need to confirm that our data stays in the EU region.'),
  (11, 'Kite Travel',     'pro',        '2026-09-06 08:25', 'Webhooks arrive late',
   'Webhooks arrive 20 to 30 minutes late since yesterday, so bookings are not confirmed in time.'),
  (12, 'Lumen Energy',    'enterprise', '2026-09-06 15:00', 'Switch to annual billing',
   'We would like to switch from monthly to annual billing and add 20 seats.'),
  (13, 'Mosaic Foods',    'free',       '2026-09-07 10:30', 'Empty ticket',
   NULL),
  (14, 'Nova Games',      'pro',        '2026-09-07 17:10', 'Thank you',
   'Just wanted to say the new release is great. Keep it up!'),
  (15, 'Orbit Systems',   'enterprise', '2026-09-08 06:50', 'Unknown logins',
   'We see logins to our account from a country where we have no staff. Please lock the account and investigate.'),
  (16, 'Pine Legal',      'pro',        '2026-09-08 11:15', 'Charged after downgrade',
   'I downgraded to the Free plan last week but was still charged for Pro. Please reverse the charge.');

SELECT count(*) AS tickets FROM support_tickets;
