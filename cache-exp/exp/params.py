NB_NODES = 9
SERVER_REPLICA_MANAGER_PORT= 3030
REP_PORT = 8880
MEMCACHED_LISTENING_PORT = 5555
EXECUTION_LOCAL = False
PATH_TO_TASKS = "/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/random_subset2.csv" if EXECUTION_LOCAL else "/home/csimohammed/exp/cache-exp/exp/traces/random_subset.csv" 
PATH_TO_CONFIG_FILE = "/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/configurations/conf.yaml" if EXECUTION_LOCAL else "/home/csimohammed/exp/cache-exp/configurations/conf.yaml" #
BANDWIDTH = 1024
ENABEL_MIGRATION = True
TTL_MIN = 2 #Un ttl qui peut etre mi a jour a chaque fois que la donnée est dans l'infra (on peu le minimiser selon l'infrastructure a partire des données de l'infrastructure)
TIME_SLOT = 100
STORAGE_SPACE = 40
MAX_MIGRATIONS = 2
MIN_REQUESTS = 1
WINDOW_SIZE = 20
THRESHOLD = 0.2