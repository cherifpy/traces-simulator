import zmq
import time

class RequestObject(object):
    def __init__(self, dist, sender,object_key) -> None:
        self.dist = dist
        self.object = object_key
        self.sender = sender


class SendObject:
    def __init__(self, dist, object_key, object) -> None:
        self.dist = dist
        self.key = object_key
        self.object = object

class Task:
    def __init__(self,id_task, id_node, infos, id_dataset, ds_size) -> None:
        self.id_task = id_task
        self.id_node = id_node
        self.infos = infos
        self.id_dataset = id_dataset
        self.ds_size = ds_size

class Communication(object):

    def __init__(self,ip_address, pub_port, sub_port):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.sub_socket = self.context.socket(zmq.SUB)

        #declaration des ports
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.ip_address = ip_address
    
    def connect(self, neighbords, output):
        """
            Starts the server by creating a socket and listening for connections.
        """
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"")

        for peer in neighbords:
            self.sub_socket.connect(f"tcp://{peer['ip']}:{peer['pub_port']}")
            output.write(f"\nsub connected to tcp://{peer['ip']}:{peer['pub_port']}\n")
            
        pub_address = f"tcp://*:{self.pub_port}"        
        self.pub_socket.bind(pub_address)
        output.write(f"Pub binded on {pub_address}\n")

        
    def send(self,data):
        time.sleep(0.2)
        self.pub_socket.send_pyobj(data) 
        
    
    def recv(self):
        return self.sub_socket.recv_pyobj()

    def stop(self):
        """
            stop all the connexion with the other peers
        """
        #close pub sub socket
        self.sub_socket.close() 
        self.pub_socket.close()
        self.context.term()

class CommunicationREQREP(object):
    def __init__(self, listner_port, nieghbors = {}) -> None:
        self.listner_port = listner_port
        self.nieghbors = nieghbors
        self.context = zmq.Context()
        self.rep_socket = self.context.socket(zmq.REP)

    def send(self,ip,port,data):
        socket = self.context.socket(zmq.REQ)
        socket.connect(f"tcp://{ip}:{port}")
        self.req_socket.send_pyobj(data) 
        time.time(0.02)
        self.req_socket.close()


    def connect(self):
        pub_address = f"tcp://*:{self.rep_socket}"        
        self.pub_socket.bind(pub_address)

    def recv(self) -> SendObject:
        return self.rep_socket.recv_pyobj()

    def stop(self):
        """
            stop all the connexion with the other peers
        """
        #close pub sub socket
        self.sub_socket.close() 
        self.rep_socket.close()
        self.context.term()
    


