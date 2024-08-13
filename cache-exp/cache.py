import time
import numpy as np
import pylibmc
from exp.params import MEMCACHED_LISTENING_PORT
import os


class Cache:
    def __init__(self, cache_size, node_id):
        self.id_node = node_id
        self.cache_size = cache_size
        self.ids_data = []
        self.datas_sizes = {}
        self.slot_time = 1
        self.time_to_live = np.zeros((len(self.ids_data,)))
        self.data_requested_hostoric = []
        self.data_access_frequency = {}
        self.memory_used = 0
        self.is_memcached_installed = False

    def sendDataSetTo(self, ip_dst, id_dataset,size_ds):
        
        file_name = '/tmp/tmp.bin'
        file_size_octet = size_ds*1024*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        
        with open(file_name, "rb") as p:
            content = p.read()
         # Données massives de 5 MB
        servers = [f"{ip_dst}:{MEMCACHED_LISTENING_PORT}"]  # Adresse du serveur Memcached
        
        #TODO Check if the data is sended and ask the client to access id to set the LRU
        client_tmp = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
        r = client_tmp.set(id_dataset, content)

        return r 
    
    #TODO
    def accessData(self, id_dataset):
        value = self.client.get(id_dataset)
        if not value and value in self.ids_data:
            self.ids_data.remove(id_dataset)
            return False
        
        return True
    
    def getStats(self, verbos=False):
        stats = self.client.get_stats()
        if not verbos and stats:
            return stats
        # Print the statistics
        for key, value in stats.items():
            print(f"{key.decode()}: {value}")
        return stats
    
    def checkOnCacheMemorie(self, id_data):
        return True if id_data in self.ids_data else False
    
    def addData(self, id_data, ds_size):
        self.datas_sizes[id_data] = ds_size
        return self.ids_data.append(id_data)
    
    
    def connectToMemcache(self,host='localhost', port=MEMCACHED_LISTENING_PORT):
        try:
            self.client = pylibmc.Client([f'0.0.0.0:{MEMCACHED_LISTENING_PORT}'], binary=True, behaviors={"tcp_nodelay": True})
            return self.client
        except Exception as e:
            print(f"Error connecting to Memcached: {e}")
            return None


    def getFromCache(self, key):
        try:
            value = self.client.get(key)
            return value
        except Exception as e:
            print(f"Error getting from Memcached: {e}")
            return None

    def deleteFromCache(self, key):
        """Deletes a value from Memcached."""
        if not self.is_memcached_installed:
            return False
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


"""def addDataOnCache(self, id_data,data_size):
    
    if id_data in self.ids_data:
        return 0
    else:
        if self.memory_used + data_size < self.cache_size:  
            self.memory_used = self.memory_used + data_size
            self.ids_data.append(id_data)
            self.datas_sizes[id_data] = data_size
            return 1
        else: return -1
"""
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