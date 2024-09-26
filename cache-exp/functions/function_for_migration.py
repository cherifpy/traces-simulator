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
                        self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                        b = self.deleteFromCache(node_id=task.id_node,node_ip=node_ip, node_port=node_port, id_dataset=ds)
                        if b:
                            candidates.remove(ds)
                        self.data[condidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        if not eviction:
                            break
                #######
                
                while eviction and len(candidates) > 0:
                    condidate = candidates[i] 
                    r_eviction = self.managerAvectionM1(task.id_node, condidate)#, self.data[condidate].size)
                    if r_eviction["send"]: 
                        id_dst_node = r_eviction["id_dst_node"]
                        self.writeOutput(f"send {condidate} from {task.id_node} and send it to {id_dst_node}\n")
                        r = False
                        r = self.deleteAndSend(id_src_node=task.id_node,id_dst_node=id_dst_node, id_dataset=condidate, ds_size=self.data[condidate].size)

                    if not r_eviction["send"] or not r:
                        self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                        self.deleteFromCache(task.id_node, node_ip, node_port, condidate)
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
                    #self.deleteDataFromTable(task.id_node, condidate)
                    self.data[condidate].updateNbReplica(add=False)
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
def startV5(self):
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
                            self.writeOutput(f"delete {condidate} from {task.id_node}\n")
                            b = self.deleteFromCache(node_id=task.id_node,node_ip=node_ip, node_port=node_port, id_dataset=ds)
                            if b:
                                del self.replicas[(condidate, task.id_node)]
                                candidates.remove(ds)
                            self.data[condidate].updateNbReplica(add=False)
                            b, self.nodes_infos = self.collecteData()
                            eviction = self.sendDataToTask(task=task, latency=latency)
                            if not eviction:
                                break
                    #######
                    
                    while eviction and len(candidates) > 0:
                        condidate = candidates[i] 
                        r_eviction = self.managerAvectionWithLimite(task.id_node, condidate)#, self.data[condidate].size)
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
                        #self.deleteDataFromTable(task.id_node, condidate)
                        self.data[condidate].updateNbReplica(add=False)
                        b, self.nodes_infos = self.collecteData()
                        eviction = self.sendDataToTask(task=task, latency=latency)
                        
                        i+=1
                    self.writeOutput(f"resultats de l'envoi de la donnée {not eviction}")  
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