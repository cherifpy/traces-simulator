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


def manageUsingKNN(self):

    if not self.nodes_infos:
        return False
    
    traces = pd.read_csv(self.traces_path)

    popularities = self.getPopularities(traces)

    data_for_knn = {
        'id_dataset':[],
        'time':[],
        'popularity_on_node' : [],
        'popularity_on_neighbors' :[],
        'softwar_classe':[],
        'last_time_used':[],
        'decision':[]
    }

    for index, row in traces.iterrows():

        b, self.nodes_infos = self.collecteData()
        task_infos = {'time' : row["time_compute (s)"],  'application_type': row["application_type"]}
        task = Task(id_task=row["id_task"],id_node= row["node_id"],infos= task_infos,id_dataset= row["dataset"],ds_size=row["dataset_size"])

        self.data_sizes[task.id_dataset] = task.ds_size
        
        if task.id_dataset not in self.data.keys(): 
            self.data[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
            if task.id_dataset not in self.previous_stats:
                self.previous_stats[task.id_dataset] = Data(id_dataset=task.id_dataset, size=task.ds_size, replicas_location=None,nb_requests_on_traces=popularities[task.id_dataset])
        
        self.data[task.id_dataset].updateDataState(task.id_node)

        data_for_knn = updateDataset(dataset=data_for_knn, id_dataset=task.id_dataset, time=index, window_size=WINDOW_SIZE)
        
        model_ready, accuracy, model = updateKNNModel(data_for_knn)

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

                while eviction and len(candidates) > 0:

                    condidate = candidates[i] 
                    
                    p_node, p_neighbors, p_software, last_used = getStat(traces,condidate,task.id_node,index)
                    
                    data_for_knn = saveData(
                        dataset=data_for_knn,
                        id_ds=condidate,
                        time=index,
                        p_neighbors=p_neighbors,
                        p_node=p_node,
                        last_time_used=last_used,
                        s_classe=p_software
                    )
                    
                    r_eviction = evictionUsingKNN(self,traces, condidate, task.id_node, index,model)
                    
                    if r_eviction["send"]: 
                        id_dst_node = r_eviction["id_dst_node"]
                        self.writeOutput(f"send {condidate} from {task.id_node} and send it to {id_dst_node}\n")
                        r = False
                        r = self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data[condidate].size)

                    if not r_eviction["send"] or not r:
                        self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                        self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                        del self.replicas[(condidate, task.id_node)]
                        self.data[condidate].updateNbReplica(add=False)
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
                    condidate = candidates[i] 
                    self.writeOutput(f"delete {condidate} from {task.id_node}\n") 
                    d = self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
                    print(f"delete {d}\n")
                    self.data[condidate].updateNbReplica(add=False)
                    b, self.nodes_infos = self.collecteData()
                    eviction = self.sendDataToTask(task=task, latency=latency)
                    
                    i+=1
                self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}")  
            if not eviction:
                if (task.id_dataset, task.id_node) not in self.replicas.keys(): self.replicas[(task.id_dataset, task.id_node)] = Replica(task.id_dataset, task.id_node)
        else:
            self.writeTransfert(f"{task.id_task},{task.id_dataset},-1,{task.id_node},{task.ds_size},0,NoTransfert\n")
    df = pd.DataFrame(data_for_knn)
    df.to_csv("/tmp/data_used_for_KNN.csv")
    return True


def getStat(dataset,id_ds, id_node, index):
    w_size = 20
    p_node = 0
    p_neighbors = 0
    last_used = w_size
    p_software = []

    if index == 0 :
        return 0,0,0, last_used
    
    if index < w_size:
        for i in range(index-1):
            if dataset.iloc[index-i]["dataset"] == id_ds:
                if dataset.iloc[index-i]["node_id"] == id_node:
                    p_node += 1
                    if last_used == w_size:last_used = i
                else:p_neighbors += 1
                p_software.append(dataset.iloc[index-i]["node_id"])
    else:
        for i in range(w_size):
            if dataset.iloc[index-i-1]["dataset"] == id_ds:
                if dataset.iloc[index-i-1]["node_id"] == id_node:
                    p_node += 1
                    if last_used == w_size: last_used = i
                else: p_neighbors += 1
                p_software.append(dataset.iloc[index-i]["node_id"])

    return p_node,p_neighbors,p_software,last_used

def saveData(dataset, id_ds, time,p_node, p_neighbors, s_classe, last_time_used):
    """
        function to save data on the dataset

    """
    dataset['id_dataset'].append(id_ds)
    dataset['time'].append(time)
    dataset['popularity_on_node' ].append(p_node)
    dataset['popularity_on_neighbors'].append(p_neighbors)
    dataset['softwar_classe'].append(s_classe)
    dataset['last_time_used'].append(last_time_used)
    dataset['decision'].append(None)

    return dataset

def updateDataset(dataset, id_dataset, time, window_size):
    """
        here i update the dataset according to recieved data 
    """
    for i in range(len(dataset["id_dataset"])):
        if dataset['decision'][i] is  None and dataset['id_dataset'][i] == id_dataset:
            if dataset['time'][i] + window_size < time:
                dataset['decision'][i] = 1
            elif dataset['time'][i] + window_size > time:
                dataset['decision'][i] = 0
    return dataset

def updateKNNModel(dataset, min_traces=100,k=3):
    previous_data = copy.deepcopy(dataset)
    data = pd.DataFrame(dataset)
    data = data[data["decision"] is not None]
    if data.shape[0] < 10:
        #print("Not enough data points for prediction.")
        return False, None, None
    
    X = np.array(data[['popularity_on_node','popularity_on_neighbors','last_time_used']])
    y = np.array(data['decision'])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)   
    
    knn = KNeighborsClassifier(n_neighbors=k)
    
    knn.fit(X_train, y_train)
    y_pred = knn.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    print(f'Accuracy: {accuracy*100:.2f}%')
    
    return True, accuracy, knn



def decideOnMigrationUsingKNN(traces,id_ds, id_node, index,model,model_ready=True):
    if model is None:
        if random.random() > THRESHOLD:
            return True
        else:
            return False
    
    p_node, p_neighbors, p_software, last_used = getStat(traces ,id_ds,id_node,index)
    prediction  = model.predict([[p_node, p_neighbors,last_used]])
    print(f"prediction {prediction[0]}")
    if prediction[0] > 0.5:
        return True
    else:
        return False


def evictionUsingKNN(self,traces, id_ds, id_node,index, model ):#self,condidate, task.id_node, index,model
    #partie TTL
    data_item = self.data[id_ds]
    
    p = self.data[id_ds].TTL
    if not decideOnMigrationUsingKNN(traces, id_ds, id_node,index, model,):
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
    optimal_cost = 0
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