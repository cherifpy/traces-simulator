from client import CacheManager
import subprocess
import time
from configuration.configuration import Configuration
from communication.send_data import sendObject
from communication.communication import CommunicationREQREP, CommunicationDelear
from replicaManager import ReplicaManager

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
    
    """
    SITE_ID = sys.argv[1] 

    PORT_PUB = sys.argv[2] 
    PORT_SUB = sys.argv[3]
    IP_ADDRESS = str(sys.argv[4])
    """
    
    for i in range(len(graphe_info)):
        if graphe_info[id_peer,i] > 0:
            peer = {
                'latency' : graphe_info[id_peer,i],
                "id": i,
                "ip" : ip_address[i], 
                "rep_port" : rep_port+i, 
            } 

            data["infos"].append(peer)
    return data

###### Start a reservation
PATH_TO_CONFIG_FILE = "/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/configuration/conf.yaml"
PATH_TO_TASKS ="/Users/cherif/Documents/Traveaux/traces-simulator/cache-exp/exp/traces/traces_with_datasets.csv" 
      
port_rep = 8780

if __name__ == "__main__":

    config = Configuration(
        config_file_path = PATH_TO_CONFIG_FILE,
    )
    
    NB_SITE = config.nb_sites
    MAT_GRAPHE = config.getGraphe()
    
    provider = config.setReservation()
    netem = config.setNetworkConstraintes()

    ## deplot memcached
    config.deployMemcached()

    task_manager = ReplicaManager(
        traces_path=PATH_TO_TASKS,
        connection = CommunicationDelear(
            3030,
            nieghbors=None,
            
        ),
        graphes= config.getGraphe()
    )

    ## get the data needed by the actors
    # list of all sites ip
    ips_address = config.getAllIPs()
    graphe = config.getGraphe()
    infos_nodes= []
    if config.execution_local:
        import threading
        for i, machine in enumerate(config.machines):
            data = InfosToSend(i,graphe, ips_address,8780,config.storage_capacities[i])
            
            thread = threading.Thread(
                target=run_command, 
                args=(f"python cache-exp/main.py ",))
            
            thread.start()
            
            print(f"adresse IP du node {i} : {ips_address[i]}")
            
            sendObject(data, ips_address[i])
            infos_nodes.append({"node_ip":ips_address[i], "node_port":port_rep})
            port_rep += 1
            time.sleep(1)
            
        task_manager.connection.nieghbors = infos_nodes
        task_manager.nodes_infos = infos_nodes
        task_manager.nb_nodes = config.nb_sites
        task_manager.start()
 
    else:
        
        for i, machine in enumerate(config.machines):

            data = InfosToSend(i,graphe, ips_address,8780,5555)
            
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
            infos_nodes.append({"node_ip":ips_address[i], "node_port":port_rep})
            port_rep += 1
            
            print(f"adresse IP du node {i} : {ips_address[i]}")
            sendObject(data, ips_address[i])

        #lunching the tasks manager
        task_manager.nodes_infos = infos_nodes
        task_manager.nb_nodes = config.nb_sites
        task_manager.start()

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