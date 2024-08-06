from communication.communication import CommunicationREQREP
from communication.messages import SendObject, RequestObject, Task
from cache import Cache
import pickle
import queue
from communication.api.cacheManagerServer import CacheManagerServer

class CacheManager(object):

    def __init__(self, id, storage_space,listner_port,neighbors) -> None:
        self.id = id
        self.storage_space = storage_space
        self.time_limite = 0
        self.neighbors = neighbors
        self.listner_port = listner_port
        self.cache = Cache(self.storage_space, self.id)
        self.output = open(f"/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/outputs/log_{self.id}.txt",'w')
        self.future_task = queue.Queue()
        self.cache_server = None
        self.server_is_running = False

    def start(self):

        self.cache_server = CacheManagerServer(port=self.listner_port)
        self.server_is_running = self.cache_server.run()
        
        while True:

            if self.server_is_running and self.cache_server.recieved_task.qsize() != 0:
                
                message = self.cache_server.recieved_task.get()

                self.output.write(f'\n{str(message)}')
                
                if message.dist == self.id:
                    self.processMessage(message) 
    
    #TODO coninue cette fonction
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
        
