#here i have to manage replica
import copy
from gc import enable
from math import cos
from platform import node
from urllib import response
import redis
from jinja2 import pass_environment
from exp.params import  (
    NB_NODES, 
    SERVER_REPLICA_MANAGER_PORT, 
    PATH_TO_TASKS, 
    SERVER_REPLICA_MANAGER_PORT, 
    MEMCACHED_LISTENING_PORT,
    BANDWIDTH,
    ENABEL_MIGRATION,
    TTL_MIN,
    EXECUTION_LOCAL
)

from communication.send_data import recieveObject
from communication.messages import Task
from communication.replicaManagerServer import ReplicaManagerServer
from functions.costs import transefrtWithGain
from classes.data import Data
from classes.djikstra import dijkstra
from typing import Optional, Dict
import multiprocessing 
import pandas as pd
import numpy as np
import time
import requests
if not EXECUTION_LOCAL: import pylibmc
import os
import threading

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
            return False

        traces = pd.read_csv(self.traces_path)

        for index, row in traces.iterrows():
            b, self.nodes_infos = self.collecteData()
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response, latency = self.sendTask(task,node_port, node_ip)

            if response["sendData"]:
                #is_eviction = True if self.nodes_infos[task.id_node]["remaining_space"] < (task.ds_size*1024*1024+65) else False
                if ENABEL_MIGRATION and response["eviction"]:
                    #self.writeOutput(f"Eviction demandée {response}\n")
                    for condidate in reversed(self.nodes_infos[task.id_node]["keys"]): #enlever reversed pour que l'exp soit la meme avec celle de hier
                        ##self.writeOutput(f"condidate {condidate}\n")
                        #f ((task.ds_size+5120)*1024)> self.nodes_infos[task.id_node]["remaining_space"]:
                        if ((task.ds_size)*1024)> self.nodes_infos[task.id_node]["remaining_space"]:
                            r_eviction = self.serachReplicaDistination(task.id_node, condidate, self.data_sizes[condidate])
                            #self.writeOutput(f"{r_eviction}\n")
                            #TODO erreur sponed with dataset
                            if r_eviction["send"]: 
                                id_dst_node = r_eviction["id_dst_node"]
                                self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data_sizes[condidate])
                                self.writeOutput(f"delete {condidate} from {task.id_node} la\n")
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                                #self.deleteDataFromTable(task.id_node, condidate)
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                                #self.addDataToTable(id_dst_node, condidate)
                                #if r2 : self.notifyNode(self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , condidate)
                            else:
                                self.writeOutput(f"delete {condidate} from {task.id_node} ici\n")
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                                self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                                #self.deleteDataFromTable(task.id_node, condidate)
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                                #self.data[condidate].updateNbReplica(add=False)
                                
                elif not ENABEL_MIGRATION and response["eviction"]:
                        for data in reversed(self.nodes_infos[task.id_node]["keys"]):
                            if ((task.ds_size+5120)*1024) > self.nodes_infos[task.id_node]["remaining_space"]:
                                self.writeOutput(f"delete {data} from {task.id_node}\n")
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                                self.deleteFromCache(task.id_node, node_ip, node_port, data)
                                self.deleteDataFromTable(task.id_node, data)
                                self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
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
                        cost = self.transfertCost(latency, task.ds_size)
                        #self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                        ##self.addDataToTable(task.id_node, task.id_dataset)
                        
                        self.nb_data_trasnfert +=1
                        self.writeTransfert(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost},transfert2\n")
                        #print(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost}\n")
                        

                if not l or not t:
                    #if with eviction change here add the condition to send the data somewhere
                    self.sendDataSet(id_node=task.id_node,ip_node=node_ip, id_ds=task.id_dataset, ds_size=task.ds_size)
                    self.data[task.id_dataset].updateNbReplica(add=True)
                    #self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                    #self.addDataToTable(task.id_node, task.id_dataset)
                    self.nb_data_trasnfert +=1
                    cost = self.transfertCost(latency, task.ds_size)
                    self.writeTransfert(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost},transfert1\n")
                    #print(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost}\n")

            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.ds_size},{task.id_node},0,NoTransfert\n")
                self.nb_data_trasnfert_avoided+=1
                

            
            self.accessData(task.id_node,task.id_dataset)
            self.data[task.id_dataset].addPopularityPeerNode(task.id_node)

        #process.terminate()
        #process.join()
        self.writeOutput(f"nb_transfert {self.nb_data_trasnfert}\n")
        #self.writeOutput(f"nb transfert avoided: {self.nb_data_trasnfert_avoided}")
        print(sum_cost)
        return True
    

    def startV2(self):
        if not self.nodes_infos:
            return False
        traces = pd.read_csv(self.traces_path)
        
        for index, row in traces.iterrows():
            #self.writeOutput(f"{str(self.nodes_infos)}\n")
            b, self.nodes_infos = self.collecteData()
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response, latency = self.sendTask(task,node_port, node_ip)
            eviction = True  

            if response["sendData"]:
                eviction = self.sendDataToTask(task=task, latency=latency)
                if eviction and ENABEL_MIGRATION:
                    i = 0
                    if 'keys' in self.nodes_infos[task.id_node].keys():
                        condidates = copy.deepcopy(reversed(self.nodes_infos[task.id_node]["keys"]))
                    else:
                        condidates = []
                    
                    while not eviction and len(condidates) > 0:
                        condidate = condidates[i] 
                        r_eviction = self.serachReplicaDistination(task.id_node, condidate, self.data_sizes[condidate])
                        if r_eviction["send"]: 
                            id_dst_node = r_eviction["id_dst_node"]
                            self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data_sizes[condidate])
                            self.writeOutput(f"delete {condidate} from {task.id_node} and sended to {id_dst_node}\n")
                            self.deleteDataFromTable(task.id_node, condidate)
                            #self.addDataToTable(id_dst_node, condidate)
                            self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                        else:
                            self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                            self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                            self.deleteDataFromTable(task.id_node, condidate)
                            self.data[condidate].updateNbReplica(add=False)
                            self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                        eviction = self.sendDataToTask(task=task, latency=latency)

                elif eviction and not ENABEL_MIGRATION:
                    i = 0
                    condidates = copy.deepcopy(reversed(self.nodes_infos[task.id_node]["keys"]))
                    while eviction:
                        condidate = condidates[i] 

            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.ds_size},{task.id_node},0,NoTransfert\n")       

        """itteration sur ca jusqua ce que la donnée pass_environment
        donc on auras une double 
        while evition:
            senddata()
        
        je sauvegarde dans mes fichier"""

    def startV3(self):
        traces = pd.read_csv(self.traces_path)

        for index, row in traces.iterrows():
            b, self.nodes_infos = self.collecteData()
            
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])
            
            self.writeOutput(f"node:{task.id_node}\n")
            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response, latency = self.sendTask(task,node_port, node_ip)
            self.writeOutput(f"{response}\n")


            if response["sendData"]:
                if ENABEL_MIGRATION and response["eviction"]:
                    for condidate in self.nodes_infos[task.id_node]["keys"]: #enlever reversed pour que l'exp soit la meme avec celle de hier
                        if ((task.ds_size)*1024)> self.nodes_infos[task.id_node]["remaining_space"]:
                            r_eviction = self.serachReplicaDistination(task.id_node, condidate, self.data_sizes[condidate])
                            if r_eviction["send"]: 
                                self.deleteAndSend(id_src_node=task.id_node,id_dst_node=r_eviction["id_dst_node"], id_dataset=condidate, ds_size=self.data_sizes[condidate])
                            else:
                                self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                    
                    
                    
                elif not ENABEL_MIGRATION and response["eviction"]:
                        for data in self.nodes_infos[task.id_node]["keys"]:
                            if ((task.ds_size)*1024) > self.nodes_infos[task.id_node]["remaining_space"]:
                                self.deleteFromCache(task.id_node, node_ip, node_port, data)
                                #self.deleteDataFromTable(task.id_node, data)
                                self.data[data].updateNbReplica(add=False)       
                else:
                    pass

                b, self.nodes_infos = self.collecteData()
                
                _,l = self.searchForDataOnNeighbors(id_node=task.id_node, dataset=task.id_dataset)
                t = False
                if l:
                    t = self.askForATransfert(
                        src= l,
                        dst=task.id_node,
                        id_dataset=task.id_dataset,
                        size_ds=task.ds_size
                    )
                    self.writeOutput(f"Resultat du transfert {t}")
                    if t: 
                        self.data[task.id_dataset].updateNbReplica(add=True)
                        cost = self.transfertCost(latency, task.ds_size)
                        #self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                        self.writeTransfert(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost},transfert2\n")
    
                if not l or not t:
                    r = self.sendDataSet(id_node=task.id_node,ip_node=node_ip, id_ds=task.id_dataset, ds_size=task.ds_size)
                    self.writeOutput(f"Resultat du transfert {r}")
                    self.data[task.id_dataset].updateNbReplica(add=True)
                    #self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                    cost = self.transfertCost(latency, task.ds_size)
                    self.writeTransfert(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost},transfert1\n")
                   
            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.ds_size},{task.id_node},0,NoTransfert\n")
                self.nb_data_trasnfert_avoided+=1

            #self.accessData(task.id_node,task.id_dataset) #a ajouter juste quand je veux utiliser le cache hit et le cache miss
            self.data[task.id_dataset].addPopularityPeerNode(task.id_node)


        self.writeOutput(f"nb_transfert {self.nb_data_trasnfert}\n")
        return True
    
    def startV4(self):
        if not self.nodes_infos:
            return False
        traces = pd.read_csv(self.traces_path)
        
        for index, row in traces.iterrows():
            #self.writeOutput(f"{str(self.nodes_infos)}\n")
            b, self.nodes_infos = self.collecteData()
            task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
            task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

            self.data_sizes[task.id_dataset] = task.ds_size
            
            if task.id_dataset not in self.data.keys(): 
                self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None)
            
            node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
            node_port = self.nodes_infos[int(task.id_node)]["node_port"]
            response, latency = self.sendTask(task,node_port, node_ip)
            eviction = True  

            if response["sendData"]:
                eviction = self.sendDataToTask(task=task, latency=latency)
                
                if eviction and ENABEL_MIGRATION:
                    i = 0
                    if 'keys' in self.nodes_infos[task.id_node].keys():
                        condidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                    else:
                        condidates = []
                    
                    while eviction and len(condidates) > 0:
                        condidate = condidates[i] 
                        r_eviction = self.serachReplicaDistination(task.id_node, condidate, self.data_sizes[condidate])
                        if r_eviction["send"]: 
                            id_dst_node = r_eviction["id_dst_node"]
                            self.writeOutput(f"send {task.id_node} from {task.id_node} and send it to {id_dst_node}\n")
                            self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data_sizes[condidate])
                            
                            #self.writeOutput(f"Resultat du transfert {r}")
                            #self.writeOutput(f"delete {condidate} from {task.id_node} and sended to {id_dst_node}\n")
                            #self.deleteDataFromTable(task.id_node, condidate)
                            #self.addDataToTable(id_dst_node, condidate)
                            #self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                        else:
                            self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                            self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                            #self.deleteDataFromTable(task.id_node, condidate)
                            self.data[condidate].updateNbReplica(add=False)
                            ##self.writeOutput(f"{self.nodes_infos[task.id_node]['keys']}\n")
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        i+=1
                    self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}")   
            else:
                self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.ds_size},{task.id_node},0,NoTransfert\n")

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
            self.nodes_infos[key]["popularities"] = response["popularities"]

            """if 'keys' in self.nodes_infos[key].keys():
                for id_ds in self.nodes_infos[key]["keys"]:
                    self.addToLocationTable(key, id_ds)"""
            #print(f"received data from {key}, {self.nodes_infos[key]}")
        ##self.writeOutput(f"{self.nodes_infos}")
        ##self.writeOutput("finishing collecting data from actors\n")
        
        return True, self.nodes_infos
    
    #used
    #TODO aussi
    def searchForDataOnNeighbors(self, id_node, dataset):
        """
            need to change this to select from where to get th data
        """
        locations = []
        latency = []
        #dataset_location = self.getDataSetLocation(id_ds=dataset)
        for node, c in enumerate(self.graphe_infos[id_node]):
            #if c > 0 and c < self.graphe_infos[self.id][node] and node in dataset_location:
            if c > 0 and c < self.graphe_infos[self.id][node] and dataset in self.nodes_infos[node]["keys"]:
                locations.append(node)                
                latency.append(c)
            
        if len(locations) == 0:
            return None, None
        
        i_min = np.argmin(latency)

        return latency[i_min], locations[i_min]
    
    
    def sendDataToTask(self, task, latency):
        node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
        node_port = self.nodes_infos[int(task.id_node)]["node_port"]
        _,l = self.searchForDataOnNeighbors(id_node=task.id_node, dataset=task.id_dataset)
        
        t = False
        if l:
            #TODO verifier ca je dois gere l'eviction la
            eviction = self.askForATransfert( 
                src= l,
                dst=task.id_node,
                id_dataset=task.id_dataset,
                size_ds=task.ds_size
            )
            if eviction: 
                self.data[task.id_dataset].updateNbReplica(add=True)
                cost = self.transfertCost(latency, task.ds_size)
                self.nb_data_trasnfert +=1
                self.writeTransfert(f"{task.id_task},{task.id_dataset},{l},{task.ds_size},{task.id_node},{cost},transfert2\n")
                
        if not l or not t:
            eviction = self.sendDataSet(id_node=task.id_node,ip_node=node_ip, id_ds=task.id_dataset, ds_size=task.ds_size) 
            
            if eviction:
                self.data[task.id_dataset].updateNbReplica(add=True)
                #self.addToLocationTable(id_dataset=task.id_dataset,id_node=task.id_node)
                #self.addDataToTable(task.id_node, task.id_dataset)
                self.nb_data_trasnfert +=1
                cost = self.transfertCost(latency, task.ds_size)
                self.writeTransfert(f"{task.id_task},{task.id_dataset},{self.id},{task.ds_size},{task.id_node},{cost},transfert1\n")
        
        return not eviction
    
    #used a copie
    def sendTask(self, task:Task, port, ip="localhost"):
        data = {"task": task.to_json(), "type":"task"}
        url = f'http://{ip}:{port}/execut'
        
        if self.isNeighbors(task.id_node):
            
            response = requests.post(url, json=data)
            ##self.writeOutput(f"task {task.id_task} sended to {task.id_node}\n")

            return response.json(), self.graphe_infos[self.id][task.id_node]
        
        else:
            path, cost =  dijkstra(self.graphe_infos, self.id, task.id_node)
            
            ##self.writeOutput(f'{path}')
            n = path.pop(0)
            next_dst = path.pop(0)
            ip_n = self.nodes_infos[int(next_dst)]["node_ip"]
            port_n = self.nodes_infos[int(next_dst)]["node_port"] 

            data_to_send = {
                    'data' : data,
                    'url':url,
                    'methode':'POST',
                    'path': path,
                    'target':task.id_node, 
                    'id_ds':task.id_dataset
                }
             
            url2 = f'http://{ip_n}:{port_n}/process'
            ##self.writeOutput(url2)
            response = requests.post(url2, json=data_to_send)
            #print(response.text)
            return response.json(), cost

    #used a copie
    def manageEviction(self, id_node, id_ds, ds_size):
        """
            ici je supprimer direct si ka données et dans les voisie
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
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > (((ds_size+5120)*1024)):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

    def manageEvictionV2(self, id_node, id_ds, ds_size, space_availabel):
        """
            here i will use the TTL to decide if a had to migrate or send 
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
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and space_availabel > (((ds_size+5120)*1024)):
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
        

        n = self.isOnNeighbords(id_node, id_ds)
        if len(n) != 0:
            return {"delete":True, "send":False} #demander au noeud de juste supprimer la données

        else:
            min_access_and_transfet_time = float('inf')
            node = None

            for id_neighbors in range(self.nb_nodes):
                if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and self.nodes_infos[id_neighbors]["remaining_space"] > (((ds_size+5120)*1024)):
                    cost = self.transfertCost(self.graphe_infos[int(id_node)][id_neighbors], ds_size) 
                    if cost <= min_access_and_transfet_time:
                        min_access_and_transfet_time = cost
                        node = id_neighbors

            return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}
        
    def sendDataSet(self,id_node, ip_node, id_ds,ds_size):
        if EXECUTION_LOCAL:
            return True
        
        if self.local_execution:
            return True
        file_name = '/tmp/tmp.bin'
        file_size_octet = int(ds_size)*1024
        with open(file_name, "wb") as p:
            p.write(os.urandom(file_size_octet))
        with open(file_name, "rb") as p:
            content = p.read()
        servers = [f"{ip_node}:{MEMCACHED_LISTENING_PORT}"]  # Adresse du serveur Memcached
        
        #client = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
        client = redis.Redis(host=ip_node, port=MEMCACHED_LISTENING_PORT, db=0, decode_responses=True)
        self.last_node_recieved = ip_node

        #TODO Check if the data is sended and ask the client to access id to set the LRU
        try:
            r = client.set(id_ds, content)
            return r
        except:
            return False
        #if r: self.location_table[id_ds].append()
        self.last_node_recieved = None

        return not r 
    
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
        return not response.json()["response"]
    
    def deleteAndSend(self, id_src_node, id_dst_node, id_dataset, ds_size):
        url = f'http://{self.nodes_infos[id_src_node]["node_ip"]}:{self.nodes_infos[id_src_node]["node_port"]}/send-and-delete'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
            'ds_size': ds_size,
            'ip_dst_node': self.nodes_infos[id_dst_node]["node_ip"],
            'port_dst_node':self.nodes_infos[id_dst_node]["node_port"]
        })
        ##self.writeOutput("migration declanchée\n")
        ##self.writeOutput(f"{response.text}\n")
        if response.json()["sended"]:
            cost = self.transfertCost(self.graphe_infos[int(id_src_node)][int(id_dst_node)],ds_size)
            self.writeTransfert(f"null,{id_dataset},{id_src_node},{ds_size},{id_dst_node},{cost},migration\n")
            self.nodes_infos[id_src_node]['remaining_space'] = response.json()['remaining_space']
            #self.location_table[id_dataset].append(id_dst_node)
            #self.location_table[id_dataset].remove(id_src_node)

            self.notifyNode(id_dst_node,self.nodes_infos[id_dst_node]['node_ip'],self.nodes_infos[id_dst_node]['node_port'] , id_dataset, add=True)
            #self.accessData(id_src_node,id_dataset)
        self.writeOutput(f"resultat du transfert {response.json()}\n")
        return response.json()
    
    def deleteFromCache(self,node_id, node_ip, node_port, id_dataset):
        url = f'http://{node_ip}:{node_port}/delete-data'

        response = requests.get(url,params={
            'id_dataset':id_dataset,
        })
        #print(response.text)
        ##self.writeOutput(f"{response.text}")
        self.nodes_infos[node_id]["remaining_space"] = response.json()["remaining_space"]
        #while node_id in self.location_table[id_dataset]: self.location_table[id_dataset].remove(node_id)
        #self.deleteFromLocationTable(node_id, id_dataset)
        self.notifyNode(node_id,node_ip,node_port , id_dataset, add=False)
        if response.json()['reponse']:
            self.writeOutput(f"{id_dataset} deleted from {node_id}\n")
            #self.notifyNode(node_id,node_ip,node_port , id_dataset, add=False)
        self.writeOutput(f"resulta de sup de {id_dataset} = {response.json()}\n")
        return response.json()

    def notifyNode(self, id_node,ip_node, port_node, id_dataset, add):
        url = f'http://{ip_node}:{port_node}/notify'
        
        data = { 
            "id_dataset": id_dataset,
            "add":1 if add else 2
        }
        
        response = requests.post(url, json=data)
        self.nodes_infos[id_node]['remaining_space'] = response.json()['remaining_space']
        #print(response.json()["response"]),
        self.writeOutput(f"resultat du transfert {response.json()}\n")
        return response.json()
    
    def deleteAndSendOnThread(self, id_src_node, id_dst_node, id_dataset, ds_size):
        
        sending_process = threading.Thread(target=self.deleteAndSend, args=(id_src_node,id_dst_node, id_dataset, id_dataset,ds_size))
        #sending_process = multiprocessing.Process(target=self.sendDaaSet, args=[ip_node, id_dataset, ds_size])
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

    def serachReplicaDistination(self,id_node,id_ds, ds_size):
        """
            here i will use the TTL to decide if a had to migrate or send 
        """
        data = self.data[id_ds]

        if self.data[id_ds].nb_replica >= TTL_MIN : return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra

        if len(self.isOnNeighbords(id_node, id_ds)) != 0: return {"delete":True, "send":False} #demander au noeud de juste supprimer la données
        
        min_access_and_transfet_time = -1
        node = None

        for id_neighbors in range(self.nb_nodes):
            space_availabel = self.nodes_infos[id_neighbors]["remaining_space"]
            if  self.graphe_infos[int(id_node)][id_neighbors] > 0 and (space_availabel > (((ds_size)*1024))):
                popularity = 0 if id_ds not in self.nodes_infos[id_neighbors]['popularities'].keys() else self.nodes_infos[id_neighbors]['popularities'][id_ds]
                cost =  transefrtWithGain(
                    b=BANDWIDTH,
                    l=self.graphe_infos[int(id_node)][id_neighbors],
                    s=ds_size,
                    n=popularity, 
                )
                if cost > min_access_and_transfet_time:
                    min_access_and_transfet_time = cost
                    node = id_neighbors

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

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
        self.api_server = ReplicaManagerServer( 
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

    def addDataToTable(self,id_node, id_dataset):
        if 'keys' not in self.nodes_infos[id_node].keys():
            self.nodes_infos[id_node]['keys'] = []
        if id_dataset not in self.nodes_infos[id_node]['keys']:
            self.nodes_infos[id_node]['keys'].append(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
        else:
            self.nodes_infos[id_node]['keys'].remove(id_dataset)
            self.nodes_infos[id_node]['keys'].append(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
        return False

    def deleteDataFromTable(self, id_node, id_dataset):
        if id_dataset in self.nodes_infos[id_node]['keys']:
            self.nodes_infos[id_node]['keys'].remove(id_dataset)
            #self.writeOutput(f'{self.nodes_infos[id_node]['keys']}')
            return True
        
        

    def deleteFromLocationTable(self,id_node, id_dataset):
        if id_dataset in self.location_table.keys():
            while id_node in self.location_table[id_dataset]:
                self.location_table[id_dataset].remove(id_node)
        return True

    def isNeighbors(self, id_node):
        if self.graphe_infos[self.id][int(id_node)] > 0:
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
    task_manager.startV4()