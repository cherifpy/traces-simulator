import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#here i have to manage replica
import copy
from exp.params import  (
    NB_NODES, 
    SERVER_REPLICA_MANAGER_PORT, 
    PATH_TO_TASKS, 
    SERVER_REPLICA_MANAGER_PORT, 
    MEMCACHED_LISTENING_PORT,
    BANDWIDTH,
    ENABEL_MIGRATION,
    TIME_SLOT,
    TTL_MIN,
    EXECUTION_LOCAL,
    MAX_MIGRATIONS,
    MIN_REQUESTS,
    WINDOW_SIZE,
    THRESHOLD
)

from communication.messages import Task
from communication.replicaManagerServer import ReplicaManagerServer
from functions.costs import (
    nodeImportanceV2, 
    transefrtWithGain, 
    transfertTime,
    nodeImportance,
    minimizingTimeTransfert
    )
from classes.data import Data
from classes.replica import Replica
from classes.djikstra import djikstra

import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import random


def startV4(self):
    previous_stats = []
    if not self.nodes_infos:
        return False
    traces = pd.read_csv(self.traces_path)

    popularities = self.getPopularities(traces)
    time = 0
    for index, row in traces.iterrows():
        
        #self.writeOutput(f"{str(self.nodes_infos)}\n")
        b, self.nodes_infos = self.collecteData()
        task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
        task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

        self.data_sizes[task.id_dataset] = task.ds_size
        
        if task.id_dataset not in self.data.keys(): 
            self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
            if task.id_dataset not in self.previous_stats:
                self.previous_stats[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
        
        self.data[task.id_dataset].updateDataState(task.id_node)
        popularities[task.id_dataset] -=1
        node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
        node_port = self.nodes_infos[int(task.id_node)]["node_port"]
        response, latency = self.sendTask(task,node_port, node_ip)
        eviction = True  
        self.writeOutput(f"==============================Task {index} {task.id_task}\n")
        
        if response["sendData"]:
            eviction = self.sendDataToTask(task=task, latency=latency)
            
            if eviction and ENABEL_MIGRATION:
                i = 0
                if 'keys' in self.nodes_infos[task.id_node].keys():
                    candidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                else:
                    candidates = []

                ##
                # 
                # This part is juste added to simulate the optimale execution so it must be deleted     
                data_to_delete = copy.deepcopy(candidates)
                for ds in data_to_delete:
                    data = self.data[ds]
                    if data.nb_requests_on_traces == 0 and eviction:
                        self.writeOutput(f"delete {candidate} from {task.id_node}\n")
                        b = self.deleteFromCache(node_id=task.id_node,node_ip=node_ip, node_port=node_port, id_dataset=ds)
                        if b:
                            candidates.remove(ds)
                        self.data[candidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        if not eviction:
                            break
                #######
                
                while eviction and len(candidates) > 0:
                    candidate = candidates[i] 
                    r_eviction = self.managerAvectionM1(task.id_node, candidate)#, self.data[candidate].size)
                    if r_eviction["send"]: 
                        id_dst_node = r_eviction["id_dst_node"]
                        self.writeOutput(f"send {candidate} from {task.id_node} and send it to {id_dst_node}\n")
                        r = False
                        r = self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=candidate, ds_size=self.data[candidate].size)

                    if not r_eviction["send"] or not r:
                        self.writeOutput(f"delete {candidate} from {task.id_node}\n")
                        self.deleteFromCache(task.id_node, node_ip, node_port, candidate)
                        self.data[candidate].updateNbReplica(add=False)
                    b, self.nodes_infos = self.collecteData()
                    eviction = self.sendDataToTask(task=task, latency=latency)
                    i+=1
                self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}\n")   
                
            if eviction and not ENABEL_MIGRATION:
                i = 0

                if 'keys' in self.nodes_infos[task.id_node].keys(): candidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                else: candidates = []
                print(f"task {task.id_task}")
                while eviction and len(candidates) > 0:
                    candidate = candidates[i] 
                    self.writeOutput(f"delete {candidate} from {task.id_node}\n") 
                    d = self.deleteFromCache(task.id_node, node_ip, node_port, candidate)
                    print(f"delete {d}\n")
                    #self.deleteDataFromTable(task.id_node, candidate)
                    self.data[candidate].updateNbReplica(add=False)
                    b, self.nodes_infos = self.collecteData()
                    eviction = self.sendDataToTask(task=task, latency=latency)
                    print(eviction)
                    i+=1
                self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}")  
        else:
            self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.id_node},{task.ds_size},0,NoTransfert\n")
        if time == TIME_SLOT:
            self.data, self.previous_stats = Data.iniTDataTTL(self.data)
            self.initNodeImportance()
            time = 0
        else:
            time+=1
    return True

def bestMigration(self):
    previous_stats = []
    if not self.nodes_infos:
        return False
    traces = pd.read_csv(self.traces_path)

    popularities = self.getPopularities(traces)
    time = 0
    for index, row in traces.iterrows():
        
        #self.writeOutput(f"{str(self.nodes_infos)}\n")
        b, self.nodes_infos = self.collecteData()
        task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
        task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

        self.data_sizes[task.id_dataset] = task.ds_size
        
        if task.id_dataset not in self.data.keys(): 
            self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
            if task.id_dataset not in self.previous_stats:
                self.previous_stats[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
        
        self.data[task.id_dataset].updateDataState(task.id_node)
        
        node_ip = self.nodes_infos[int(task.id_node)]["node_ip"]
        node_port = self.nodes_infos[int(task.id_node)]["node_port"]
        response, latency = self.sendTask(task,node_port, node_ip)
        eviction = True  
        self.writeOutput(f"==============================Task {index} {task.id_task}\n")
        
        if response["sendData"]:
            eviction = self.sendDataToTask(task=task, latency=latency)
            
            if eviction and ENABEL_MIGRATION:
                
                if 'keys' in self.nodes_infos[task.id_node].keys():
                    candidates = copy.deepcopy(self.nodes_infos[task.id_node]["keys"])
                else:
                    candidates = []

                ##
                # 
                # This part is juste added to simulate the optimale execution so it must be deleted     
                data_to_delete = copy.deepcopy(candidates)
                for ds in data_to_delete:
                    data = self.data[ds]
                    if data.nb_requests_on_traces == 0 and eviction:
                        self.writeOutput(f"delete {candidate} from {task.id_node}\n")
                        b = self.deleteFromCache(node_id=task.id_node,node_ip=node_ip, node_port=node_port, id_dataset=ds)
                        del self.replicas[(ds, task.id_node)]
                        candidates.remove(ds)
                        self.data[candidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        if not eviction:
                            break
                #######
                i = 0
                while eviction and len(candidates) > 0:
                    
                    candidate = candidates[i] 
                    if not predictNextUssage(traces,index, candidate):
                        self.writeOutput(f"delete {candidate} from {task.id_node}\n")
                        self.deleteFromCache(task.id_node, node_ip, node_port, candidate)
                        del self.replicas[(candidate, task.id_node)]
                        self.data[candidate].updateNbReplica(add=False)
                        
                        
                    else:

                        r_eviction = manageEvictionForBest(self, task.id_node, candidate,popularities)#, self.data[candidate].size)
                        
                        if r_eviction["send"]: 
                            id_dst_node = r_eviction["id_dst_node"]
                            self.writeOutput(f"send {candidate} from {task.id_node} and send it to {id_dst_node}\n")
                            r = False
                            r = self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=candidate, ds_size=self.data[candidate].size)

                        if not r_eviction["send"] or not r:
                            self.writeOutput(f"delete {candidate} from {task.id_node}\n")
                            self.deleteFromCache(task.id_node, node_ip, node_port, candidate)
                            del self.replicas[(candidate, task.id_node)]
                            self.data[candidate].updateNbReplica(add=False)

                    b, self.nodes_infos = self.collecteData()
                    eviction = self.sendDataToTask(task=task, latency=latency)
                    i+=1
                self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}\n")   
                
            if not eviction:
                if (task.id_dataset, task.id_node) not in self.replicas.keys(): self.replicas[(task.id_dataset, task.id_node)] = Replica(task.id_dataset, task.id_node)
        else:
            self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.id_node},{task.ds_size},0,NoTransfert\n")
        if time == TIME_SLOT:
            self.data, self.previous_stats = Data.iniTDataTTL(self.data)
            self.initNodeImportance()
            time = 0
        else:
            time+=1
    return True

def predictNextUssage(traces,index, id_ds, window=20):
    bool = False
    if index+window > traces.shape[0]:
        for i in range(traces.shape[0] - index - 1):
            if traces.iloc[index+i+1]["dataset"] == id_ds:
                bool = True
                break
    else:
        for i in range(window-1):
            if traces.iloc[index+i+1]["dataset"] == id_ds:
                bool = True
                break
    return bool

def manageEvictionForBest(self,id_node,id_ds, popularities):
    """
        i use this function to impose limites on data migration 
        in this function i will use the class replica 
    """
    data_item = self.data[id_ds]
    
    if popularities[id_ds] == 0 or self.replicas[(id_ds, id_node)].nb_migrations > MAX_MIGRATIONS:
        print("deleted cause of TTL or max migration for the replica\n")
        return {"delete":True, "send":False}
    
    neighbors = []
    storage_on_node = []
    for n in range(len(self.graphe_infos)-1):
        if self.graphe_infos[id_node][n] > 0 and self.nodes_infos[n]["remaining_space"] > (((data_item.size+100)*1024)) and id_ds not in self.nodes_infos[n]['keys']:
            neighbors.append((n, self.nodes_infos[n]["remaining_space"]))
    
    sorted_neighbors_by_space = sorted(neighbors, key=lambda x: x[1], reverse=True)
    optimal_cost = 0#float('inf')
    node = None

    keys_peer_node = {}
    for n in self.nodes_infos.keys():
        keys_peer_node[n] = copy.deepcopy(self.nodes_infos[n]['keys'])

    for id_n, _ in sorted_neighbors_by_space:
        space_availabel = self.nodes_infos[id_n]["remaining_space"]
        if  self.graphe_infos[int(id_node)][id_n] > 0 and (space_availabel > (((data_item.size+100)*1024))):
            self.writeOutput(f"why not to send {id_n} from {id_node} to {id_n} {self.graphe_infos[int(id_node)][id_n]}\n")
            
            cost = minimizingTimeTransfert(
                    dataset=id_ds,
                    ds_size=data_item.size,
                    id_src=id_node,
                    id_dst=id_n,
                    key_peer_node=keys_peer_node,
                    graphe_infos=self.graphe_infos
                )

            if cost > optimal_cost:
                optimal_cost = cost
                node = id_n

    return {"delete":True, "send": True if not node is None else False, "id_dst_node":node} 

    #je suis arrivé la je continu le choix du noeud comme dicuté

def managerAvectionM1(self,id_node,id_ds):

        #partie TTL
        data_item = self.data[id_ds]
        
        p = self.data[id_ds].TTL
        if p == -1:
            self.writeOutput("deleted cause of TTL\n")
            return {"delete":True, "send":False} #supp si le TTL l'exige => bcp de donnée dans l'infra
        
        self.writeOutput("TTL esquivé \n")
        if self.replicas[(id_ds, id_node)].nb_migrations > MAX_MIGRATIONS:
            self.writeOutput("deleted cause of Migration limite\n")
            return {"delete":True, "send":False}
        
        neighbors = []
        storage_on_node = []
        for n in range(len(self.graphe_infos)-1):
            if self.graphe_infos[id_node][n] > 0 and self.nodes_infos[n]["remaining_space"] > (((data_item.size+100)*1024)) and id_ds not in self.nodes_infos[n]['keys']:
                neighbors.append((n, self.nodes_infos[n]["remaining_space"]))
        
        sorted_neighbors_by_space = sorted(neighbors, key=lambda x: x[1], reverse=True)
        optimal_cost = float('inf')
        node = None

        keys_peer_node = {}
        for n in self.nodes_infos.keys():
            keys_peer_node[n] = copy.deepcopy(self.nodes_infos[n]['keys'])

        for id_n, _ in sorted_neighbors_by_space:
            space_availabel = self.nodes_infos[id_n]["remaining_space"]
            if  self.graphe_infos[int(id_node)][id_n] > 0 and (space_availabel > (((data_item.size+100)*1024))):
                self.writeOutput(f"why not to send {id_n} from {id_node} to {id_n} {self.graphe_infos[int(id_node)][id_n]}\n")
                #p = 0 if id_node not in self.data[id_ds].popularity_peer_node.keys() else self.data[id_ds].popularity_peer_node[id_n]

                """cost =  nodeImportanceV2(
                    b=BANDWIDTH,
                    graphe_infos=self.graphe_infos,
                    s=data_item.size,
                    id_node=id_n,
                    nb_requests= 1 if id_n not in self.requests_processed.keys() else self.requests_processed[id_n]
                )"""

                
                cost = transfertTime(
                    b=BANDWIDTH,
                    l=self.graphe_infos[int(id_node)][id_n],
                    s=data_item.size,
                )
                """
                
                cost = minimizingTimeTransfert(
                    dataset=id_ds,
                    ds_size=data_item.size,
                    id_src=id_node,
                    id_dst=id_n,
                    key_peer_node=keys_peer_node,
                    graphe_infos=self.graphe_infos
                )"""

                if cost < optimal_cost:
                    optimal_cost = cost
                    node = id_n

        return {"delete":True, "send": True if not node is None else False, "id_dst_node":node}

