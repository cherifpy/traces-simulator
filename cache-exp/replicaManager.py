#here i have to manage replica
from math import cos
from platform import node
from urllib import response
from exp.params import  (
    NB_NODES, 
    SERVER_REPLICA_MANAGER_PORT, 
    PATH_TO_TASKS, 
    SERVER_REPLICA_MANAGER_PORT, 
    MEMCACHED_LISTENING_PORT,
    BANDWIDTH,
    ENABEL_MIGRATION,
    TTL_MIN
)

from communication.send_data import recieveObject
from communication.messages import Task
from communication.cacheManagerServer import CacheManagerServer
from communication.replicaManagerAPI import ReplicaManagerAPI
from classes.data import Data
from classes.djikstra import dijkstra
from typing import Optional, Dict
import multiprocessing 
import pandas as pd
import numpy as np
import time
import requests
import pylibmc
import os
import threading



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
    
    def __init__(self,nb_nodes ,traces_path,graphe, ip, local_execution) -> None:
        self.nb_nodes = nb_nodes-1
        self.id = nb_nodes-1
        self.traces_path = PATH_TO_TASKS#"/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/random_subset.csv"
        self.nodes_infos = dict()
        self.api_server = None
        self.graphe_infos = graphe
        self.location_table = {}
        self.port = SERVER_REPLICA_MANAGER_PORT
        self.ip = ip
        self.nb_data_trasnfert = 0
        self.output = open(f"/tmp/log_M.txt",'a')
        #str = "/tmp/transfert_M.txt" if ENABEL_MIGRATION else "/tmp/transfert.txt"
        #self.transfert = open(str,'w')
        self.local_execution = local_execution
        self.num_evection = 0
        self.last_node_recieved = None
        self.data_sizes = {}
        self.nb_data_trasnfert_avoided = 0
        self.data: Dict[str, Data] = {}
        self.replicas = {}
        
    def start(self):
        sum_cost = 0
        process = None
        if not self.nodes_infos:
            self.writeOutput("no infos \n")
            return False
        
        #process = self.startThread()
        traces = pd.read_csv(self.traces_path)
        
        self.writeOutput(f"{str(self.nodes_infos)}\n")

        
        for index, row in traces.iterrows():
            b, self.nodes_infos = self.collecteData()
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response = self.sendTask(task,node_port, node_ip)

            if response["sendData"]:
                #is_eviction = True if self.nodes_infos[task.id_node]["remaining_space"] < (task.ds_size*1024*1024+65) else False
                if ENABEL_MIGRATION and response["eviction"]:
                    self.writeOutput(f"Eviction demandée {response}\n")
                    
                    for condidate in response["condidates"]: #enlever reversed pour que l'exp soit la meme avec celle de hier
                        self.writeOutput(f"condidate {condidate}\n")
                        if (task.ds_size*1024) + 65 > self.nodes_infos[task.id_node]["remaining_space"]:

                            r_eviction = self.manageEviction(task.id_node, condidate, self.data_sizes[condidate])
                            self.writeOutput(f"{r_eviction}\n")
                            #TODO erreur sponed with dataset
                            if r_eviction["send"]:
                                id_dst_node = r_eviction["id_dst_node"]
                                self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data_sizes[condidate])
                                #if r2 : self.notifyNode(self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , condidate)
                            else:
                                self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                                self.data[condidate].updateNbReplica(add=False)
                                
                elif not ENABEL_MIGRATION and response["eviction"]:
                        for data in reversed(response["condidates"]):
                            if (task.ds_size*1024) + 65 > self.nodes_infos[task.id_node]["remaining_space"]:
                                self.deleteFromCache(task.id_node, node_ip, node_port, data)
                                self.data[data].updateNbReplica(add=False)
                               
                else:
                    pass
                    #self.nodes_infos[task.id_node]["remaining_space"] -= task.ds_size*1024 + 65
                    

                _,l = self.searchForDataOnNeighbors(id_node=task.id_node, dataset=task.id_dataset)
                t = False
                if l:
                    t = self.askForATransfert(
                        src= l,
                        dst=task.id_node,
                        id_dataset=task.id_dataset,
                        size_ds=task.ds_size
                    )
                    if t: 
                        self.data[task.id_dataset].updateNbReplica(add=True)
                        cost = self.transfertCost(self.graphe_infos[l][task.id_node], task.ds_size)
                        self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                        self.nb_data_trasnfert +=1
                        self.writeTransfert(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost},transfert2\n")
                        print(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost}\n")
                        

                if not l or not t:
                    #if with eviction change here add the condition to send the data somewhere
                    self.sendDataSet(id_node=task.id_node,ip_node=node_ip, id_ds=task.id_dataset, ds_size=task.ds_size)
                    self.data[task.id_dataset].updateNbReplica(add=True)
                    self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                    self.nb_data_trasnfert +=1
                    cost = self.transfertCost(self.graphe_infos[self.id][task.id_node], task.ds_size)
                    self.writeTransfert(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost},transfert1\n")
                    print(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost}\n")

            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.ds_size},{task.id_node},0,NoTransfert\n")
                self.nb_data_trasnfert_avoided+=1
                

            
            self.accessData(task.id_node,task.id_dataset)
            self.data[task.id_dataset].addPopularityPeerNode(task.id_node)

        #process.terminate()
        #process.join()
        self.writeOutput(f"nb_transfert {self.nb_data_trasnfert}\n")
        self.writeOutput(f"nb transfert avoided: {self.nb_data_trasnfert_avoided}")
        print(sum_cost)
        return True
    
    #used a copie
    def collecteData(self):
        if len(self.nodes_infos.keys()) == 0:
            return False, {}
        self.location_table = {}
        for key in self.nodes_infos.keys():
            url = f'http://{self.nodes_infos[key]["node_ip"]}:{self.nodes_infos[key]["node_port"]}/infos'
            response = requests.get(url).json()
    
            self.nodes_infos[key]["storage_space"] = response["storage_space"]
            self.nodes_infos[key]["remaining_space"] = response["remaining_space"]
            self.nodes_infos[key]["keys"] = response['keys']

            for id_ds in self.nodes_infos[key]["keys"]:
                self.addToLocationTable(key, id_ds)
            #print(f"received data from {key}, {self.nodes_infos[key]}")
        self.writeOutput(f"{self.nodes_infos}")
        self.writeOutput("finishing collecting data from actors\n")
        
        return True, self.nodes_infos
    
    #used
    def searchForDataOnNeighbors(self, id_node, dataset):
        """
            need to change this to select from where to get th data
        """
        locations = []
        latency = []
        dataset_location = self.getDataSetLocation(id_ds=dataset)
        for node, c in enumerate(self.graphe_infos[id_node]):
            if c > 0 and c < self.graphe_infos[self.id][node] and node in dataset_location:
                locations.append(node)                
                latency.append(c)
            
        if len(locations) == 0:
            return None, None
        
        i_min = np.argmin(latency)

        return latency[i_min], locations[i_min]
    
    #used a copie
    def sendTask(self, task:Task, port, ip="localhost"):
        data = {"task": task.to_json(), "type":"task"}
        url = f'http://{ip}:{port}/execut'
        
        if self.isNeighbors(task.id_node):
            
            response = requests.post(url, json=data)
            self.writeOutput(f"task {task.id_task} sended to {task.id_node}\n")
            return response.json()
        else:
            path, cost =  dijkstra(self.graphe_infos, 0, task.id_node)
            
            self.writeOutput(f'{path}')
            n = path.pop(0)
            ip_n = self.nodes_infos[int(task.id_node)]["node_ip"]
            port_n = self.nodes_infos[int(task.id_node)]["node_port"] 

            data_to_send = {
                    'data' : data,
                    'url':url,
                    'methode':'POST',
                    'path': path,
                    'target':task.id_node, 
                }
             
            url = f'http://{ip_n}:{port_n}/process'
            response = requests.post(url, json=data_to_send)
            self.writeOutput(response.text)
            return response.json()

    #used a copie
    def manageEviction(self, id_node, id_ds, ds_size):
        """
            ici je supprimer direct si ka données et dans les voisie
        """
        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > ((ds_size*1024) + 65):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

    def manageEvictionV2(self, id_node, id_ds, ds_size, space_availabel):
        """
            here i will use the TTL to decide if a had to migrate or send 
        """
        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and space_availabel > ((ds_size*1024) + 65):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
    
    def manageEvictionWITHTTL(self, id_node, id_ds, ds_size):
        """
            ici je supprimer direct si ka données et dans les voisie
            pourquoi pas choisir la donnée avec un tres TTL pour la supprimer du cache
        """
        if self.data[id_ds].nb_replica >= TTL_MIN :
            return {"delete":True, "send":False}

        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > ((ds_size*1024) + 65):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
        

    def migrate(self, task:Task, condidates):
        
        operations = []
        self.writeOutput(f"Eviction demandée\n")  

        for condidate in condidates: #enlever reversed pour que l'exp soit la meme avec celle de hier
            
            self.writeOutput(f"condidate {condidate}\n")
            space_availabel = self.nodes_infos[task.id_node]["remaining_space"]
            if (task.ds_size*1024) + 65 > space_availabel:

                r_eviction = self.manageEvictionV1(task.id_node, condidate, self.data_sizes[condidate],space_availabel)

                self.writeOutput(f"{r_eviction}\n")
                if r_eviction["send"]:
                    id_dst_node = r_eviction["id_dst_node"]
                    operations.append(("migrate", condidate,id_dst_node))
                    #self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data_sizes[condidate])
                else:
                    space_availabel += (task.ds_size*1024) + 65
                    operations.append("delete", condidate, task.id_node)
                    #self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
        
        node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
        node_port = self.nodes_infos[int(task.id_node)]["node_port"]
        
        url = f'http://{node_ip}:{node_port}/operations'

        response = requests.post(url, json={
            "operations": operations
        })

        return response.json()

        
    def sendDataSet(self,id_node, ip_node, id_ds,ds_size):
        if self.local_execution:
            return True
        file_name = '/tmp/tmp.bin'
        file_size_octet = int(ds_size)*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        with open(file_name, "rb") as p:
            content = p.read()
        servers = [f"{ip_node}:{MEMCACHED_LISTENING_PORT}"]  # Adresse du serveur Memcached
        
        client = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
        self.last_node_recieved = ip_node

        #TODO Check if the data is sended and ask the client to access id to set the LRU
        r = client.set(id_ds, content)
        #if r: self.location_table[id_ds].append()
        self.last_node_recieved = None

        
        return r 
    
    def accessData(self, id_node, id_dataset, ip="localhost"):

        url = f'http://{self.nodes_infos[id_node]["node_ip"]}:{self.nodes_infos[id_node]["node_port"]}/access-data'

        response = requests.get(url,params={
            'id_dataset':id_dataset
        })
        
        return response.json()

    def accessDataOnThread(self, id_node, id_dataset, ip="localhost"):
        process = threading.Thread(target=self.accessData, args=(id_node, id_dataset))
        process.start()

        return process
    
    def askForATransfert(self, src, dst, id_dataset,size_ds):
        
        url = f'http://{self.nodes_infos[src]["node_ip"]}:{self.nodes_infos[src]["node_port"]}/transfert'
        
        data = {
            "dst_id": dst,
            "dst_ip": self.nodes_infos[dst]["node_ip"], 
            "id_dataset": id_dataset,
            "size_ds": size_ds
        }

        response = requests.post(url, json=data)
        #print(response.json()["response"])
        return response.json()["response"]
    
    def deleteAndSend(self, id_src_node, id_dst_node, id_dataset, ds_size):
        url = f'http://{self.nodes_infos[id_src_node]["node_ip"]}:{self.nodes_infos[id_src_node]["node_port"]}/send-and-delete'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
            'ds_size': ds_size,
            'ip_dst_node': self.nodes_infos[id_dst_node]["node_ip"],
            'port_dst_node':self.nodes_infos[id_dst_node]["node_port"]
        })
        self.writeOutput("migration declanchée\n")
        self.writeOutput(f"{response.text}")
        if response.json()["sended"]:
            cost = self.transfertCost(self.graphe_infos[int(id_src_node)][int(id_dst_node)],ds_size)
            self.writeTransfert(f"null,{id_dataset},{id_src_node},{ds_size},{id_dst_node},{cost},migration\n")
            self.nodes_infos[id_src_node]['remaining_space'] = response.json()['remaining_space']
            self.location_table[id_dataset].append(id_dst_node)
            self.location_table[id_dataset].remove(id_src_node)

            self.notifyNode(id_dst_node,self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , id_dataset, add=True)
            #self.accessData(id_src_node,id_dataset)
        return response.json()
    
    def deleteFromCache(self,node_id, node_ip, node_port, id_dataset):
        url = f'http://{node_ip}:{node_port}/delete-data'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
        })
        print(response.text)
        self.writeOutput(f"{response.text}")
        self.nodes_infos[node_id]["remaining_space"] = response.json()["remaining_space"]

        if response.json()['reponse']:
            if node_id in self.location_table[id_dataset]: self.location_table[id_dataset].remove(node_id)
            
            self.writeOutput(f"{id_dataset} deleted from {node_id}\n")
            self.notifyNode(node_id,node_ip,node_port , id_dataset, add=False)

        return response.json()

    def notifyNode(self, id_node,ip_node, port_node, id_dataset, add=True):
        url = f'http://{ip_node}:{port_node}/notify'
        
        data = { 
            "id_dataset": id_dataset,
            "add":add
        }
        
        response = requests.post(url, json=data)
        self.nodes_infos[id_node]['remaining_space'] = response.json()['remaining_space']
        #print(response.json()["response"])
        return response.json()
    
    def deleteAndSendOnThread(self, id_src_node, id_dst_node, id_dataset, ds_size):
        
        sending_process = threading.Thread(target=self.deleteAndSend, args=(id_src_node,id_dst_node, id_dataset, id_dataset,ds_size))
        #sending_process = multiprocessing.Process(target=self.sendDataSet, args=[ip_node, id_dataset, ds_size])
        sending_process.start()

        return sending_process
        

    def isOnNeighbords(self,node,id_ds):
        n = []
        for id_node, infos in self.nodes_infos.items():

            if self.graphe_infos[int(id_node)][int(node)] > 0 and id_ds in infos["keys"]:
                n.append(id_node)
        
        return n
    

    def dataOnNode(self,id_node, id_dataset):
        if id_node in self.local_execution[id_dataset]:
            return True
        
        return False

    def sendDataSetOnThread(self, id_node,ip_node, id_dataset, ds_size, process:Optional[threading.Thread]):
        
        sending_process = threading.Thread(target=self.sendDataSet, args=(id_node,ip_node, id_dataset, ds_size))
        #sending_process = multiprocessing.Process(target=self.sendDataSet, args=[ip_node, id_dataset, ds_size])
        sending_process.start()

        return sending_process
    
    #Note used
    def getEmptyNode(self):
        nodes = []
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == node["storage_space"]:
                nodes.append(i)
        return nodes   
         
    #Note used
    def selectNodeBySpace(self, task:Task):
        nodes = []
        if self.datasetOnNeigbors(task.id_node, task.id_dataset):
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
    
    #Note used
    def choseNode(self, task:Task):
        if self.datasetOnNeigbors(task.id_node, task.id_dataset):
            return 
        for i, node in enumerate(self.nodes_infos):
            if node["remaining_space"] == 0 or node["remaining_space"] < task.ds_size:
                continue
            else:
                return i
        return False
    
    def getDataSetLocation(self,id_ds):
        return self.location_table[id_ds] if id_ds in self.location_table.keys() else []
    
    def startFlaskServer(self):
        pass
        self.api_server = ReplicaManagerAPI( 
            host=self.ip,
            port=self.port,
            replica_manager=self
            )
        server_is_running = self.api_server.run()


    def addToLocationTable(self, id_node, id_dataset):
        if id_dataset in self.location_table.keys():
            if id_node not in self.location_table[id_dataset]:
                self.location_table[id_dataset].append(id_node)
            return True
            
        else:
            self.location_table[id_dataset] = [id_node]
            return True
        
        return False
    
    def isNeighbors(self, id_node):
        if self.graphe_infos[int(self.id)][int(id_node)] > 0:
            return True
        else: 
            return False
    
    def startThread(self):
        flask_process = multiprocessing.Process(target=self.startFlaskServer)
        flask_process.start()
        time.sleep(0.2)
        return flask_process

    def transfertCost(self, latency, data_size):
        bandwith_in_bits = BANDWIDTH*1024*1024*8
        size_in_bits = data_size*1024*8
        latency_in_s = latency/1000

        return latency_in_s + (size_in_bits/bandwith_in_bits)

    def writeOutput(self, str):
        self.output = open(f"/tmp/log_M.txt",'a')
        self.output.write(str)
        self.output.close()
    
    def writeTransfert(self,str):
        path = "/tmp/transfert_M.txt" if ENABEL_MIGRATION else "/tmp/transfert.txt"
        self.transfert = open(path,'a')
        self.transfert.write(str)
        self.transfert.close()

if __name__ == "__main__":

    data = recieveObject()

    
    task_manager = ReplicaManager(
        nb_nodes = NB_NODES,
        traces_path=PATH_TO_TASKS,
        graphe= data["graphe_infos"],
        ip=data["IP_ADDRESS"],
        local_execution=False
    )
    task_manager.writeOutput(f"{data}")
    
    
    task_manager.nodes_infos = data["infos"]
    task_manager.start()