import zmq
import time
import socket
import json
"""
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
        self.dist = id_node
        self.id_task = id_task
        self.id_node = id_node
        self.infos = infos
        self.id_dataset = id_dataset
        self.ds_size = ds_size
    
    def to_dict(self):
        return {
            'id_task': self.id_task,
            'id_node': self.id_node,
            'infos': self.infos,
            'id_dataset': self.id_dataset,
            'ds_size': self.ds_size
        }

    @staticmethod
    def from_dict(data):
        return Task(data['id_task'], data['id_node'], data['infos'], data['id_dataset'], data['ds_size'])

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(data):
        return Task.from_dict(json.loads(data))
    
    def __str__(self):
        return f"Task(id_task={self.id_task}, id_node={self.id_node}, infos={self.infos}, id_dataset={self.id_dataset}, ds_size={self.ds_size})"

class endMessage:
    def __init__(self,id_node, end_message) -> None:
        self.dist = id_node
        self.end_message = end_message
"""
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
        self.dealer_socket = self.context.socket(zmq.DEALER)
    
    def send(self,ip,port,data):
        with zmq.Context() as context:
            with context.socket(zmq.REQ) as socket:    
                socket.connect(f"tcp://{ip}:{port}")
                socket.send_pyobj(data) 

    def connect(self):
        
        self.dealer_socket = self.context.socket(zmq.REQ)
        pub_address = f"tcp://localhost:{self.listner_port}"       
        self.rep_socket.bind(pub_address)

    def recv(self):
        return self.rep_socket.recv_pyobj()

    def stop(self):
        """
            stop all the connexion with the other peers
        """
        #close pub sub socket
        self.sub_socket.close() 
        self.rep_socket.close()
        self.context.term()
    


class CommunicationDelear(object):
    def __init__(self, listner_port, nieghbors = []) -> None:
        self.listner_port = listner_port
        self.nieghbors = nieghbors
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
    
    def send(self,ip,port,data):
        return self.socket.send_pyobj(data) 


    def connect(self):
        for n in self.nieghbors:
            node_ip = n["node_ip"]
            node_port = n["node_port"]
            self.socket.connect(f"tcp://{node_ip}:{node_port}")
            print(f"tcp://{node_ip}:{node_port}")

    def recv(self):
        return self.rep_socket.recv_pyobj()

    def stop(self):
        """
            stop all the connexion with the other peers
        """
        #close pub sub socket
        self.sub_socket.close() 
        self.rep_socket.close()
        self.context.term()


