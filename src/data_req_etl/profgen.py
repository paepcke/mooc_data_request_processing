import pstats
p = pstats.Stats('qetl_prof.txt')
p.sort_stats('cumulative').print_stats()
