from communication.messages import SendObject, RequestObject, Task
from cache import Cache
import pickle
import queue
from communication.cacheManagerServer import CacheManagerServer
import requests
import multiprocessing 
import time

#TODO
class CacheManager(object):

    def __init__(self, id, storage_space,listner_port,neighbors, data_manager_ip,data_manager_port,host) -> None:
        self.id_node = id
        self.host = host
        self.storage_space = storage_space
        self.time_limite = 0
        self.neighbors = neighbors
        self.listner_port = listner_port
        self.cache = Cache(self.storage_space, self.id_node)
        
        self.future_task = queue.Queue()
        self.cache_server = None
        self.server_is_running = False
        self.data_manager_ip = data_manager_ip
        self.data_manager_port = data_manager_port
        self.output = open(f"/tmp/log_{self.id_node}.txt",'w')
        

    def start(self):
        self.cache_server = CacheManagerServer(
            cache=self.cache,
            host=self.host,
            port=self.listner_port
        )
        self.server_is_running = self.cache_server.run()
        return True
        process = self.startThread()
        time.sleep(30)
        process.terminate()
        process.join()

        
    def startManagerFlaskServer(self):
        self.cache_server = CacheManagerServer(host=self.host,port=self.listner_port)
        self.server_is_running = self.cache_server.run()
    
    def startThread(self):
        flask_process = multiprocessing.Process(target=self.startManagerFlaskServer)
        flask_process.start()
        time.sleep(0.2)
        return flask_process
    
    def processMessage(self, message):
        if isinstance(message,Task):
            it_exist = True #self.cache.checkExistence(message.id_dataset)
            if it_exist:
                return True
            else:
                if self.cache.addToMemcache(message.key, message.object):
                    return True
                else:
                    return False
        elif isinstance(message,SendObject):
            it_exist = self.cache.checkExistence(message.key)
            if it_exist:
                return False
            else:
                if self.cache.addToMemcache(message.key, message.object):
                    return True
                else:
                    return False

        elif isinstance(message, RequestObject):
            it_exist = self.cache.checkExistence(message.key)
            if it_exist:
                obj = self.cache.getFromCache(message.key)
                self.sendObject(message.sender,message.key,obj)
                return True
            else:
                
                return False
        else: return False


    def sendObject(self,dist, key, message):
        sendingrequest = SendObject(dist, key, message)
        infos = self.neighbors[id]
        return self.communication.send(infos["ip"], infos["port"] ,sendingrequest)
        
    def deleteData(self, ip_address, node_port, id_dataset, dataset_size):
        url = f'http://{ip_address}:{node_port}/add-data'
        data = {
            "id_node": self.id_node, 
            "id_dataset":id_dataset,
            "dataset_size": dataset_size,
        }
            

        response = requests.post(url, json=data).json

        if response["status"]:
            self.location_table["node_i"].append("key")
            return True
        return False #response.json()
    

    """        
        while True:
            if self.cache.cache_size <= self.cache.memory_used:
                pass
            if self.server_is_running and self.cache_server.recieved_task.qsize() != 0:
                
                message = self.cache_server.recieved_task.get()

                self.output.write(f'\n{str(message)}')
                
                if message.dist == self.id:
                    self.processMessage(message) 
    """