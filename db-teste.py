# -*- coding: utf-8 -*-
import src.euro_monitor as em

#cotation = em.get_euro_cotation()
#print(pprint.pformat(cotation, indent=4))

em.test_dd_create_table_cotation_euro()
print(em.test_dd_query('select * from bronze.cotation'))