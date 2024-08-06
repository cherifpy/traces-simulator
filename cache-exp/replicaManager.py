#here i have to manage tasks

import pandas as pd
import zmq 
from exp.params import NB_NODES
from communication.communication import CommunicationREQREP
import time
from communication.messages import Task
import requests

path_to_tasks ="/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/traces_with_datasets.csv" ##"/exp/traces/traces_with_datasets.csv"

#ce que je veux faire: 
"""
envoyer des taches sur les differents noeud
les taches vont contenire une donnéees est un temps d'execution
les données vont demander les taches
la données sera ensuite envoyer au noeud qui la demande directement
 lancer au meme temps as cast
"""

class ReplicaManager:
    
    def __init__(self, traces_path, connection,graphe) -> None:
        self.nb_nodes = NB_NODES
        self.traces_path = traces_path
        self.nodes_infos = []
        self.connection = connection
        self.graphe_infos = graphe
        self.location_table = {}
    
    def start(self):

        if not self.nodes_infos and not self.connection:
            return False
        self.connection.connect()
        traces = pd.read_csv(self.traces_path)
        i = 0

        b, self.nodes_infos = self.collecteData()
        if not b:
            return False
        
        for index, row in traces.iterrows():
            #send the task to the clients
            #print(row)

            task_infos = {
                'time' : row["time_compute (s)"],
                'application_type': row["application_type"]
            }
            task = Task(
                id_task=row["id_task"],
                id_node= i,
                infos= task_infos,
                id_dataset= row["dataset"],
                ds_size=row["dataset_size"]
            )

            condidated_nodes = self.getEmptyNode() 
            if len(condidated_nodes) != 0:
                pass 
            else:
                index = condidated_nodes[0]

            node_ip = self.nodes_infos[index]["node_ip"]
            node_port = self.nodes_infos[index]["node_port"]
            
            response = self.send_task(task,node_port, node_ip)
            print(str(task), response)
            i+=1
            
            time.sleep(1)  
            if i >= NB_NODES:
                i = 0
        return True
    

    def collecteData(self):
        if len(self.nodes_infos) == 0:
            return False, []
        
        for i, infos in enumerate(self.nodes_infos):
            url = f'http://{infos['node_ip']}:{infos["node_port"]}/infos'
            response = requests.post(url).json()
            
            self.nodes_infos[i]["storage_space"] = response["storage_space"]
            self.nodes_infos[i]["remaining_space"] = response["remaining_space"]
            
        return True, self.nodes_infos
    

    def getEmptyNode(self):
        nodes = []
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == node["storage_space"]:
                nodes.append(i)
        return nodes        

    def selectNodeBySpace(self, task):
        nodes = []
        if self.datasetOnNeigbors():
            return 
        for i, node in enumerate(self.nodes_infos):
            if not node["remaining_space"] == 0:
                if node["remaining_space"] < task.ds_size:
                    continue
                else:
                    nodes.append(i)
            else:
                nodes.append(i)
        return nodes


    def choseNode(self, task:Task):
        if self.datasetOnNeigbors():
            return 
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == 0 or node["remaining_space"] < task.ds_size:
                continue
            else:
                return i
        return False

    def datasetOnNeigbors(self, node_i, id_dataset):
        node_neighbors = self.graphe_infos[node_i]

        for i, val in enumerate(node_neighbors):
            if val > 0:
                if i in self.location_table[id_dataset]:
                    return True, i
        
        return False, None
    

    def send_task(self, task:Task, port, ip="localhost"):
        url = f'http://{ip}:{port}/execut'
        data = {"task": task.to_json(), "type":"task"}

        response = requests.post(url, json=data)
        return response.json()
    
    def send_data(self,node_i, key, value):
        url = f'http://{self.nodes_infos[0]['node_ip']}:{self.nodes_infos[0]['node_port']}/add-data'
        data = {"key": key, "value":value}

        response = requests.post(url, json=data).json

        if response["status"]:
            self.location_table[node_i].append(key)
            return True
        return False #response.json()

