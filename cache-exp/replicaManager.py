#here i have to manage tasks
from exp.params import NB_NODES, SERVER_REPLICA_MANAGER_PORT, PATH_TO_TASKS
from communication.send_data import recieveObject
from communication.messages import Task
from communication.cacheManagerServer import CacheManagerServer
import multiprocessing 
import pandas as pd
import time
import requests
from exp.params import SERVER_REPLICA_MANAGER_PORT, MEMCACHED_LISTENING_PORT
#import pylibmc
import os

#path_to_tasks ="/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/traces_with_datasets.csv" ##"/exp/traces/traces_with_datasets.csv"

#ce que je veux faire: 
"""
envoyer des taches sur les differents noeud
les taches vont contenire une donnéees est un temps d'execution
les données vont demander les taches
la données sera ensuite envoyer au noeud qui la demande directement
 lancer au meme temps as cast
"""

class ReplicaManager:
    
    def __init__(self,nb_nodes ,traces_path,graphe, ip) -> None:
        self.nb_nodes = nb_nodes
        self.traces_path = "/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/random_subset.csv"
        self.nodes_infos = dict()
        self.api_server = None
        self.graphe_infos = graphe
        self.location_table = {}
        self.port = SERVER_REPLICA_MANAGER_PORT
        self.ip = ip
        self.nb_data_trasnfert = 0
        self.output = open(f"/tmp/log_M.txt",'w')
        self.local_execution = True
        
    def start(self):

        if not self.nodes_infos:
            return False
        
        #process = self.startThread()
        

        traces = pd.read_csv(self.traces_path)
        i = 0

        b, self.nodes_infos = self.collecteData()
        self.output.write(str(self.nodes_infos))
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
            
            node_id = row["node_id"]
            print(node_id)
            #print(self.nodes_infos[int(node_id)])
            node_ip = self.nodes_infos[int(node_id)]["node_ip"]
            node_port = self.nodes_infos[node_id]["node_port"]
            
            response = self.sendTask(task,node_port, node_ip)
            if response['sendData']:
                #if with eviction change here add the condition to send the data somewhere
                self.sendDataSet(node_ip, id_ds=task.id_dataset, ds_size=task.ds_size)
                self.nb_data_trasnfert +=1
                self.addToLocationTable(id_dataset=task.id_dataset,id_node=node_id)
                self.output.write(f"{task.id_task},{task.id_dataset},{task.ds_size},{node_id},\n")
                print(f"{task.id_task},{task.id_dataset},{task.ds_size},{node_id}\n")
            else:
                pass

            #process.terminate()
            #process.join()
        self.output.close()
        return True
    

    def collecteData(self):
        if len(self.nodes_infos.keys()) == 0:
            return False, []
        
        for key in self.nodes_infos.keys():
            url = f'http://{self.nodes_infos[key]["node_ip"]}:{self.nodes_infos[key]["node_port"]}/infos'
            response = requests.get(url).json()
            
            self.nodes_infos[key]["storage_space"] = response["storage_space"]
            self.nodes_infos[key]["remaining_space"] = response["remaining_space"]
            #print(f"received data from {key}, {self.nodes_infos[key]}")
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
                    return True, {"sendData":"good"}
        
        return False, None
    
    def getDataSetLocation(self,id_ds):
        return self.location_table[id_ds]


    def sendTask(self, task:Task, port, ip="localhost"):

        url = f'http://{ip}:{port}/execut'
        data = {"task": task.to_json(), "type":"task"}

        response = requests.post(url, json=data)
        return response.json()

    def evectData(self,id_node,id_dataset, dataset_size, with_migration=False):

        if not with_migration:
            self.location_table[id_node] = [x for x in self.location_table[id_node] if x != id_node]
            self.nodes_infos[id_dataset]["remaining_space"] += dataset_size

            return "delete"
        else:
            return "migrate"
        
    def sendDataSet(self, ip_node, id_ds,ds_size):
        if self.local_execution:
            return True
        file_name = '/exp/tmp/tmp.bin'
        file_size_octet = ds_size*1024*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        
        with open(file_name, "rb") as p:
            content = p.read()
         # Données massives de 5 MB
        servers = [f"{ip_node}:MEMCACHED_LISTENING_PORT"]  # Adresse du serveur Memcached
        
        #client = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
        #TODO Check if the data is sended and ask the client to access id to set the LRu
        return True #client.set(id_ds, content)
    
    def startFlaskServer(self):
        self.api_server = CacheManagerServer(host=self.ip, port=self.port)
        server_is_running = self.api_server.run()


    def addToLocationTable(self, id_node, id_dataset):
        if id_dataset in self.location_table.keys():
            self.location_table[id_dataset].append(id_node)
        else:
            self.location_table[id_dataset] = [id_node]

    def startThread(self):
        flask_process = multiprocessing.Process(target=self.startFlaskServer)
        flask_process.start()
        time.sleep(0.2)
        return flask_process

    def transfertCost(self, latency, data_size, ):
        pass

    

if __name__ == "__main__":

    data = recieveObject()

    
    task_manager = ReplicaManager(
        nb_nodes = NB_NODES,
        traces_path=PATH_TO_TASKS,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"]
    )
    task_manager.output.write(f"{data}")
    task_manager.nodes_infos = data["infos"]
    task_manager.start()