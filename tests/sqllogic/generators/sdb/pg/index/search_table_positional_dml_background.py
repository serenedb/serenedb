from search_dml_gen import Dml
from sqllogic_gen import Test

t = Test("Random inserts, deletes, updates and compactions, also from a second "
         "connection while a transaction is open, on a search table that also "
         "compacts in the background and a plain table with the same rows: both "
         "hold the same rows after every step.", seed=23)
Dml(t, "pd_b", background=True).run(80)
t.write()
