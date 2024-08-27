import time
import numpy as np
import pylibmc
from exp.params import MEMCACHED_LISTENING_PORT
import os
import copy 
from pymemcache.client.base import Client
import re
import requests

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
        self.last_recently_used_item = []

    def sendDataSetTo(self, ip_dst, id_dataset,size_ds):
        
        file_name = '/tmp/tmp.bin'
        file_size_octet = int(size_ds)*1024*1024
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

    
    #TODO en cas de modification de politique d'eviction
    def accessData(self, id_dataset):
        value = self.client.get(id_dataset)
        if not value and id_dataset in self.ids_data:
            self.ids_data.remove(id_dataset)
            return False
        elif value and id_dataset in self.ids_data:
            if id_dataset in self.last_recently_used_item: self.last_recently_used_item.remove(id_dataset)
            self.last_recently_used_item.append(id_dataset)
        return True
    
    def getKeys(self):
        client = Client(("127.0.0.1", MEMCACHED_LISTENING_PORT)) 
        stats_items = client.stats('items')
        keys = []
        self.memory_used = 0
        # Parse slab IDs from the 'stats items' command
        for stat_key, stat_value in stats_items.items():
            if stat_key.decode().startswith('items:'):
                parts = stat_key.decode().split(':')
                if len(parts) == 3 and parts[2] == 'number':
                    slab_id = parts[1]
                    cachedump = client.stats(f'cachedump',f'{slab_id}','100')
                
                    for key, infos in cachedump.items():
                        numbers = re.findall(r'\d+', infos.decode())
                        self.memory_used += int(numbers[0])
                        keys.append(key.decode())
                
        self.ids_data = copy.deepcopy(keys)
        return self.ids_data

    def getStats(self, verbos=False):

        stats = pylibmc.Client([f'0.0.0.0:{MEMCACHED_LISTENING_PORT}'], binary=True, behaviors={"tcp_nodelay": True}).get_stats()
        return stats
    
    def predictEviction(self,ds_size):

        ds_size_bytes = (int(ds_size)*1024*1024)+65
        
        cache_size_bytes = self.cache_size
        stats = self.getStats()[0][1]
        used_memory = stats["bytes"].decode()
        
        if int(used_memory)+ds_size_bytes > cache_size_bytes:
            return True, self.last_recently_used_item
        
        return False, None

    def checkOnCacheMemorie(self, id_data):
        return True if id_data in self.ids_data else False
    
    def addData(self, id_data, ds_size):
        self.datas_sizes[id_data] = ds_size
        while id_data in self.ids_data: self.ids_data.remove(id_data)
        self.ids_data.append(id_data)
        return True
    
    
    def connectToMemcache(self):
        try:
            self.client = pylibmc.Client([f'0.0.0.0:{MEMCACHED_LISTENING_PORT}'], binary=True, behaviors={"tcp_nodelay": True})
            return self.client
        except Exception as e:
            print(f"Error connecting to Memcached: {e}")
            return None

    #TODO a revoire le return true dans except
    def deleteFromCache(self, key):
        """Deletes a value from Memcached."""
        try:
            client = pylibmc.Client([f'0.0.0.0:{MEMCACHED_LISTENING_PORT}'], binary=True, behaviors={"tcp_nodelay": True})
            r = client.delete(key)
            #ca retourne une exption la 
            self.last_recently_used_item.remove(key)
            self.ids_data.remove(key)
            return r
        
        except Exception as e:
            print(f"Error deleting from Memcached: {e}")
            return True


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

