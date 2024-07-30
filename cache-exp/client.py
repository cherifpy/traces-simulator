from communication.communication import CommunicationREQREP, SendObject, RequestObject
from cache import Cache
import pickle

class CacheManager(object):

    def __init__(self, id, storage_space,listner_port,nieghbors) -> None:
        self.id = id
        self.storage_space = storage_space
        self.time_limite = 0
        self.nieghbors = nieghbors
        self.listner_port = listner_port
        self.cache = Cache(self.storage_space, self.id)
        self.output = open(f"/Users/cherif/Documents/Source Code//output/log_{self.id}.txt",'w')
    
    def server(self,port):
        self.communication = CommunicationREQREP(listner_port=self.listner_port, nieghbors=self.nieghbors)
        self.communication.connect()
        while True:
            message = self.communication.recv()
            if message.dist == self.id:
                self.processMessage(message) 
    
    #TODO coninue cette fonction
    def processMessage(self, message):
        
        if isinstance(message,SendObject):
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
        
