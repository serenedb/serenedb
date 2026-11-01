from search_dml_gen import Dml
from sqllogic_gen import Test

t = Test("Random inserts, deletes, updates and compactions, also from a second "
         "connection while a transaction is open, on a search table and a plain "
         "table with the same rows: both hold the same rows after every step, "
         "also when a compaction replaced the segments a removal's positions "
         "name.", seed=11)
Dml(t, "pd_a", background=False).run(80)
t.write()
