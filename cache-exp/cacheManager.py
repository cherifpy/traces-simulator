from client import CacheManager
from communication.send_data import recieveObject
from exp.params import SERVER_REPLICA_MANAGER_PORT
"""
    meme se fichier est n'est supprimer
"""
    
if __name__ == "__main__":

    #her, this function is used to recieve data from the site manager (where the enoslib script is executed)
    DATAS_RECIEVED = recieveObject()
    #get the ID and IP of the actual site 
    SITE_ID = DATAS_RECIEVED["SITE_ID"] 
    MANAGER_IP = DATAS_RECIEVED["MANAGER_IP"]
    CACHE_SIZE = DATAS_RECIEVED["CACHE_SIZE"] 
    lister_port = DATAS_RECIEVED["REP_PORT"]
    IP_ADDRESS = DATAS_RECIEVED["IP_ADDRESS"]
    
    costs = []
    neighbors = []

    for peer in DATAS_RECIEVED['infos']:
        costs.append(peer["latency"])
        neighbors.append({
            "id":peer["id"],
            "ip":peer["ip"],
            "rep_port": peer["rep_port"],
        })
    #TODO

    f = open("/tmp/je.txt")
    f.write("hello")
    f.close()
    cm = CacheManager(
        id=int(SITE_ID),
        storage_space = CACHE_SIZE,
        listner_port=lister_port,
        neighbors=neighbors,
        data_manager_ip=MANAGER_IP,
        data_manager_port=SERVER_REPLICA_MANAGER_PORT,
        host=IP_ADDRESS,
        
    )

    f = open("/tmp/tu.txt")
    f.write("hello")
    f.close()
    cm.cache_server.output.write(f"{SITE_ID} {lister_port} {DATAS_RECIEVED}")
    cm.cache_server.output.close()
    cm.cache_server.output = open(f"/tmp/log_{int(SITE_ID)}.txt",'a')
    cm.start()
          

        