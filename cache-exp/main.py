from client import CacheManager
from communication.send_data import recieveObject
import sys
import zmq
import time
import threading 
"""
    meme se fichier est n'est supprimer
"""
    
if __name__ == "__main__":

    """
    SITE_ID = sys.argv[1] 
    PORT_PUB = sys.argv[2] 
    IP_ADDRESS = str(sys.argv[3])
    """


    #her, this function is used to recieve data from the site manager (where the enoslib script is executed)
    DATAS_RECIEVED = recieveObject()

    #get the ID and IP of the actual site 
    SITE_ID = DATAS_RECIEVED["SITE_ID"] 
    CACHE_SIZE = DATAS_RECIEVED["CACHE_SIZE"] 
    REP_PORT = DATAS_RECIEVED["REP_PORT"]
    IP_ADDRESS = DATAS_RECIEVED["IP_ADDRESS"]
    
    costs = []
    neighbors = []

    for peer in DATAS_RECIEVED['infos']:
        costs.append(peer["latency"])
        neighbors.append({
            "id":peer["id"],
            "ip":peer["ip"],
            "rep_port": peer["rep_port"]
        })

    cm = CacheManager(
        id=int(SITE_ID),
        storage_space = CACHE_SIZE,
        listner_port=REP_PORT,
        neighbors=neighbors,
    )
    cm.output.write(f"{SITE_ID} {REP_PORT} {DATAS_RECIEVED}")

    cm.start()
            
    """poller = zmq.Poller()
    poller.register(cvm.sub_socket, zmq.POLLIN)"""
    
    print(type(cm.id))

    if cm.id == 0:
        
        time.sleep(3)
        cm.site = "A"
        new_data = Data(
            id_data = 0,
            size=5
        )
        cm.addData(id_data=0, data=new_data)
    
    if cm.id == 1:
        time.sleep(2)
        cm.site = "B"

    if cm.id == 2:
        time.sleep(1)
        cm.site = "C"

    if cm.id == 3:
        time.sleep(1)
        cm.site = "D"
    
    """if events:
        for socket, event in events.items():
            if socket == sub and event == zmq.POLLIN:"""
    
    print()
    while DATAS_RECIEVED:
        
        message = cm.connection.recv()
        
        cm.output.write(f"\nreceived {message.type} message from {message.id_sender} source:{message.id_source}")
        print(f"\nreceived {message.type} message from {message.id_sender} source:{message.id_source}")
        cm.processMessage(message)
        
        #thread = threading.Thread(target=cm.processMessage, args=(message,))
        #thread.start()
        break 
    
    cm.stop()

        