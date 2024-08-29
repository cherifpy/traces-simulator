

from numpy import size


class Data(object):
    def __init__(self, id_dataset,size, replicas_location) -> None:
        self.id_dataset = id_dataset
        self.size = size
        self.popularity = 0
        self.nb_replica = 0
        self.replicas_location = replicas_location
        
    def updatePopularity(self):
        self.popularity +=1


    def updateNbReplica(self):
        self.popularity +=1

    