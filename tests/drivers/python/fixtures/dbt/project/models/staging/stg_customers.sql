select id, name, signup_date, score, tier
from {{ ref('customers_seed') }}
