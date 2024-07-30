#here i have to manage tasks

import pandas as pd
import zmq 
from exp.params import NB_NODES
from communication.communication import Task
path_to_tasks ="/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/traces_with_datasets.csv" ##"/exp/traces/traces_with_datasets.csv"

#ce que je veux faire: 
"""
envoyer des taches sur les differents noeud
les taches vont contenire une donnéees est un temps d'execution
les données vont demander les taches
la données sera ensuite envoyer au noeud qui la demande directement
 lancer au meme temps as cast
"""

class TaskManager:
    
    def __init__(self, traces_path, connection) -> None:
        self.nb_nodes = NB_NODES
        self.traces_path = traces_path
        self.node_infos = None
        self.connection= None
    
    def start(self):

        if not self.node_infos and not self.connection:
            return False
        
        traces = pd.read_csv(self.traces_path)

        for index, row in traces.iterrows():
            #send the task to the clients
            #print(row)
            for n in range(NB_NODES):
                task_infos = {
                    'time' : row["time_compute (s)"],
                    'application_type': row["application_type"]
                }
                task = Task(
                    id_task=row("id_task"),
                    id_node= n,
                    infos= task_infos,
                    id_dataset= row("dataset"),
                    ds_size=row("dataset_dize")
                )
                node_ip = self.nodes_infos[n]["node_ip"]
                node_port = self.nodes_infos[n]["node_port"]
                self.connection.send(node_ip, node_port)

        return True
