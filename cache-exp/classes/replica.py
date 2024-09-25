import time

class Replica:
    num = 0
    def __init__(self, id_dataset, id_node) -> None:
        self.id_replica = self.num + 1
        self.id_dataset = id_dataset
        self.id_node = id_node
        self.creation_time = time.time()
        self.TTL = 2
        self.nb_access = 0
        self.nb_acces_peer_node = {}
        self.nb_migrations  = 0


    def updateTTL(self):
        self.TTL += 1
    
    def migration(self, id_new_node):
        self.id_node = id_new_node