

from numpy import size


class Data(object):
    def __init__(self, id_dataset,size, replicas_location) -> None:
        self.id_dataset = id_dataset
        self.size = size
        self.popularity = 0
        self.nb_replica = 0
        self.replicas_location = replicas_location
        self.popularity_peer_noed = {}

    def updatePopularity(self):
        self.popularity +=1


    def updateNbReplica(self, add=True):
        if add: self.nb_replica +=1
        else: self.nb_replica -= 1

    def addPopularityPeerNode(self, id_node):
        if id_node in self.popularity_peer_noed:
            self.popularity_peer_noed[id_node]+=1
        else: self.popularity_peer_noed[id_node] = 1
