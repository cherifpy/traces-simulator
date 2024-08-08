from client import CacheManager
import subprocess
import time
from configuration.configuration import Configuration
from communication.send_data import sendObject
from exp.params import REP_PORT

def run_command(command):
    
    result = subprocess.run(command.split(), capture_output=True, text=True)
    print(result)

def InfosToSend(id_peer:int,graphe_info,ip_address, rep_port, cache_size):
    data = {}
    data["CACHE_SIZE"] = cache_size
    data["infos"] = []
    data["SITE_ID"] = id_peer
    data["REP_PORT"] = rep_port + id_peer
    data["IP_ADDRESS"] = str(ip_address[id_peer])
    
    for i in range(len(graphe_info)):
        if graphe_info[id_peer,i] > 0:
            peer = {
                'latency' : graphe_info[id_peer,i],
                "id": i,
                "ip" : ip_address[i], 
                "rep_port" : rep_port+i,
                "storage_space":cache_size 
            } 

            data["infos"].append(peer)
    return data

def SendInfoToManager(config,ips_address, rep_port):
    data = {}
    data['graphe_info'] = config
    data["IPs_ADDRESS"] = ips_address
    data["infos"] = []
    
    for i in range(len(config)):
        if config[0,i] > 0:
            peer = {
                'latency' : config[0,i],
                "id": i,
                "ip" : ips_address[i], 
                "rep_port" : rep_port+i,
            } 
            data["infos"].append(peer)
    return data

###### Start a reservation
PATH_TO_CONFIG_FILE = "/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/configuration/conf.yaml"
  
port_rep = 8780

if __name__ == "__main__":

    config = Configuration(
        config_file_path = PATH_TO_CONFIG_FILE,
    )
    
    NB_NODES = config.nb_sites
    CONFIG_GRAPHE = config.getGraphe()
    IPS_ADDRESS = config.getAllIPs()

    provider = config.setReservation()
    netem = config.setNetworkConstraintes()

    ## deplot memcached
    config.deployMemcached()

    infos_nodes= []
    
    if config.execution_local:
        import threading


        for i, machine in enumerate(config.machines):
            if i == 0:

                data = SendInfoToManager(CONFIG_GRAPHE,IPS_ADDRESS, REP_PORT)
                
                thread = threading.Thread(
                    target=run_command, 
                    args=(f"python cache-exp/replicaManager.py ",))
                
                thread.start()
                
                print(f"adresse IP du node {i} : {IPS_ADDRESS[i]}")
                
                sendObject(data, IPS_ADDRESS[i])
                infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":port_rep})
                port_rep += 1
                time.sleep(1)

            else:
            

                data = InfosToSend(i,CONFIG_GRAPHE, IPS_ADDRESS,REP_PORT,config.storage_capacities[i])
                
                thread = threading.Thread(
                    target=run_command, 
                    args=(f"python cache-exp/main.py ",))
                
                thread.start()
                
                print(f"adresse IP du node {i} : {IPS_ADDRESS[i]}")
                
                sendObject(data, IPS_ADDRESS[i])
                infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":port_rep})
                port_rep += 1
                time.sleep(1)
                
        
 
    else:
        
        for i, machine in enumerate(config.machines):

            data = InfosToSend(i,CONFIG_GRAPHE, IPS_ADDRESS,8780,machine["storage"])
            
            print(f"node {i} ========")
            print(data)
            
            #config.enoslib.ensure_python3(True,roles=config.roles[machine["roles"][0]])
            with config.enoslib.actions(roles=config.roles[machine["roles"][0]]) as p:
                #p.ensure_python()
                p.apt(name=["git","python3-pip"], state="present")
                p.command(
                    task_name = "Delete the last version of the repo",
                    cmd = "rm -rf /home/csimohammed/as-cast-implementation"
                )
                p.git(repo="https://github.com/cherifpy/as-cast-implementation.git", dest="/home/csimohammed/as-cast-implementation")

                p.command(
                    task_name = "installing python libs",
                    cmd = "pip3 install pyzmq eclipse-zenoh numpy sockets"
                )
                p.command(
                    task_name = "Executing the code on a site",
                    cmd = f"python3 ./main.py > /tmp/strout_{i}.out >> /tmp/strerr_{i}.err",
                    background=True
                )
            infos_nodes.append({"node_ip":IPS_ADDRESS[i], "node_port":port_rep})
            port_rep += 1
            
            print(f"adresse IP du node {i} : {IPS_ADDRESS[i]}")
            sendObject(data, IPS_ADDRESS[i])

        print("Waiting for Outputs:")
        count = 0
        time.sleep(30)
        while True:
            if count == config.nb_sites:
                break

            for i, machine in enumerate(config.machines): 
                
                try:
                    with config.enoslib.actions(roles=config.roles[machine["roles"][0]]) as p:
                        p.fetch(src=f"/tmp/log_{i}.txt", dest="~")  
                        #p.fetch(src=f"/tmp/log_{i}.err", dest="~")
                        #p.fetch(src=f"/tmp/log_{i}.out", dest="~")    
                    print("Output fetched")
                    count +=1                    
                except:
                    continue