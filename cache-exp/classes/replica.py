import time

class Replica:
    num = 0
    def __init__(self, id_dataset, id_node) -> None:
        self.id_replica = self.num + 1
        self.id_dataset = id_dataset
        self.id_node = id_node
        self.creation_time = time.time()