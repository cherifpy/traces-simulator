import time
import numpy as np
from pymemcache.client import base



class Cache:
    def __init__(self, cache_size, node_id):
        self.node = node_id
        self.cache_size = cache_size
        self.client = None
        self.ids_data = []
        self.datas_sizes = {}
        self.slot_time = 1
        self.time_to_live = np.zeros((len(self.ids_data,)))
        self.data_requested_hostoric = []
        self.data_access_frequency = {}
        self.client = None
        self.memory_used = None


    def addDataOnCache(self, id_data,data_size):
        if id_data in self.ids_data:
            return False
        
        else:
            self.ids_data.append(id_data)
            self.datas_sizes[id_data] = data_size
            self.cache_size = self.cache_size - data_size
            return True
    
    def getStats(self, verbos):
        stats = self.client.stats()

        if not verbos:
            return stats
        # Print the statistics
        for key, value in stats.items():
            print(f"{key.decode()}: {value}")

    def checkOnCacheMemorie(self, id_data):
        return True if id_data in self.ids_data else False

    def connectToMemcache(self,host='localhost', port=11211):
        
        try:
            self.client = base.Client((host, port))
            return self.client
        except Exception as e:
            print(f"Error connecting to Memcached: {e}")
            return None

    def addToMemcache(self, key, value, expires=0):
        
        try:
            self.client.add(key, value, expires=expires)
            return True
        except Exception as e:
            print(f"Error adding to Memcached: {e}")
            return False

    def getFromCache(self, key):
       
        try:
            value = self.client.get(key)
            return value
        except Exception as e:
            print(f"Error getting from Memcached: {e}")
            return None

    def deleteFromCache(self, key):
        """Deletes a value from Memcached."""
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            print(f"Error deleting from Memcached: {e}")
            return False

    def getCacheStats(self):
        
        try:
            stats = self.client.stats()
            return stats
        except Exception as e:
            print(f"Error getting Memcached stats: {e}")
            return None

    def getCacheSpaceUsed(self):
        
        try:
            stats = self.get_memcache_stats(self.client)
            if stats:
                used_memory = int(stats.get('bytes', 0))
                return used_memory
            else:
                return None
        except Exception as e:
            print(f"Error calculating Memcached space used: {e}")
            return None

    def checkExistence(client, key):
        result = client.get(key)
        if result:
            return True
        else: False

    ##Je teste des truc
    def Eviction(self):

        pass


#from pymemcache.client import base
#
## Don't forget to run `memcached' before running this next line:
#client = base.Client(('localhost', 11211))
#
## Once the client is instantiated, you can access the cache:
##client.set('some_key', 'testthis')
#
## Retrieve previously set data again:
##retour = client.get('test')
##print(retour)
#
#stats = client.stats()
#
## Print the statistics
#for key, value in stats.items():
#    print(f"{key.decode()}: {value}")