from concurrent.futures import thread
from crypt import methods
from multiprocessing import process
import sys
import os
import threading
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask, request, jsonify
from communication.messages import Task
from queue import Queue
import multiprocessing
import importlib.util
from cache import Cache
from exp.params import EXECUTION_LOCAL

class CacheManagerServer:
    def __init__(self,storage_space, id_node,neighbors:dict, host='localhost', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.cache = Cache(storage_space, id_node)
        self.neighbors = neighbors
        self.nb_requests_processed = {}
        
        
        self.client = self.cache.connectToMemcache()
        if self.client != None:
            self.writeOutput("connected to memecached\n")
        else:
            self.writeOutput("not connected to memecached\n")
    
    def setup_routes(self):
        
        @self.app.route("/process", methods=["POST"])
        def process():

            data_r = request.json
            self.writeOutput(f"{data_r} {self.neighbors.keys()}\n")
            if data_r["target"] in self.neighbors.keys():
                self.writeOutput(f"neu\n")
                if data_r["methode"] == "POST":
                    reponse = requests.post(url=data_r["url"],json=data_r["data"])
                else:
                    reponse = requests.get(url=data_r["url"], params=data_r["data"])
            
            else:
                new_target = data_r['path'].pop(0)
                data_send = {
                    'data' : data_r["data"],
                    'url':data_r["url"],
                    'methode':data_r["methode"],
                    'path': data_r['path'],
                    'target':data_r['target'], 
                    'id_ds': data_r["id_ds"]
                }

                if data_r['id_ds'] in self.nb_requests_processed.keys():
                    self.nb_requests_processed[data_r["id_ds"]] += 1
                else:
                    self.nb_requests_processed[data_r["id_ds"]] = 1

                url = f"http://{self.neighbors[new_target]['ip']}:{self.neighbors[new_target]['rep_port']}/process"
                reponse = requests.post(url, json=data_send)

            
            return jsonify(reponse.json())

        
        #used
        @self.app.route('/execut', methods=['POST'])    
        def process_data():
            #print("recieved task")
            self.writeOutput("recieved task")
            data = request.json

            task = Task.from_json(data["task"])
            #{"task": data["task"], "type":data["type"] ,"status": "processed"}
            #self.recieved_task.put(task)
            b1 = self.cache.checkOnCacheMemorie(task.id_dataset)
            b2, condidates = self.cache.predictEviction(task.ds_size)
            if b1:
                processed_data = {"sendData":False, "eviction":False}
            else:
                
                processed_data = {"sendData":True, "eviction":b2, "condidates":condidates}

                #TODO:this function have a probleme
                self.cache.addData(task.id_dataset, task.ds_size)
                self.cache.memory_used += (task.ds_size*1024 + 100)

            self.writeOutput(f"task recieved {str(task)} asking th controller to send the data:{not b1}\n")

            return jsonify(processed_data)
        
        #used
        @self.app.route('/infos', methods=['GET'])
        def get_info():
            stats = self.cache.getStats()[0][1]
            
            if stats:
                data = {
                    "id_node": self.cache.id_node,
                    "storage_space": int(stats["limit_maxbytes"].decode()),
                    "remaining_space":int(stats["limit_maxbytes"].decode()) - (int(stats["bytes"].decode()))- (10*1024),
                    #'keys': self.cache.last_recently_used_item, #self.cache.getKeys()
                    'popularities':self.nb_requests_processed
                }
                self.writeOutput("here\n")
                
                self.cache.cache_size = int(stats["limit_maxbytes"].decode())
                self.cache.memory_used  = int(stats["bytes"].decode())
            else:
                data = {
                    "id_node": self.cache.id_node,
                    "storage_space": self.cache.cache_size,
                    "remaining_space":self.cache.cache_size - self.cache.memory_used,
                    #'keys': self.cache.ids_data #self.cache.getKeys()
                }
                self.writeOutput("not here\n")
            self.writeOutput(f"{data}")
            self.writeOutput("info sended\n")
            return jsonify(data)
        
        #used
        @self.app.route("/access-data", methods=['GET'])
        def ckeckData():
            stats = self.cache.getStats()[0][1]
            self.cache.memory_used = int(stats["bytes"].decode())
            id_ds = request.args.get("id_dataset")
            b = self.cache.accessData(id_ds)

            return jsonify({
                "reponse":b,
                "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode())- (10*1024)
                })
        
        @self.app.route("/get-infos-for-evection", methods=["GET"])
        def infoForEvection():

            stats = self.cache.getStats()[0][1]
            self.cache.memory_used = int(stats["bytes"].decode())
            return jsonify({
                "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode())- (10*1024),
                'last_recently_used': self.cache.last_recently_used_item
            })

        #used
        @self.app.route('/transfert', methods=['POST'])
        def transfert():
            data = request.json
            
            r = self.cache.sendDataSetTo(
                ip_dst=data["dst_ip"],
                id_dataset=data["id_dataset"],
                size_ds=data["size_ds"]
            ) 
            processed_data = {"response":r}
            
            return jsonify(processed_data)
        
        #TODO
        @self.app.route('/send-and-delete', methods=["GET"])
        def sendAndDelete():
            id_ds = request.args.get("id_dataset")
            ip_dst_node = request.args.get("ip_dst_node")
            ds_size = request.args.get("ds_size")
            port_dst = request.args.get("port_dst_node")

            b = self.cache.deleteFromCache(id_ds, ds_size=ds_size)
            self.writeOutput(b)
            if b:
                t = self.cache.sendDataSetTo(
                    ip_dst=ip_dst_node,
                    id_dataset=id_ds,
                    size_ds=ds_size
                    )
                
                stats = self.cache.getStats()[0][1]
                self.cache.memory_used = int(stats["bytes"].decode())
                response = {"sended":t, "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode())- (10*1024)}
            else:
                response = {"sended":b}

            return jsonify(response)

        #TODO
        @self.app.route("/send-to", methods=["POST"])
        def transertTo():
            data = request.json
            path = data["path"]
            
            r = self.cache.sendDataSetTo(
                ip_dst=data["dst_ip"],
                id_dataset=data["id_dataset"],
                size_ds=data["size_ds"],
            ) 
            n = path.pop()
            if n != self.cache.id_node: return jsonify({"response": False})
            if n == self.cache.id_node and len(path) != 1:
                pass

            elif n == self.cache.id_node and len(path) == 1:
                #ici si il reste que le distinataire donc envoyer vers le memcached
                r = self.cache.sendDataSetTo(
                    ip_dst=self.neighbors[n]["ip"],
                    id_dataset=data["id_dataset"],
                    size_ds=data["size_ds"],
                ) 
                pass

            processed_data = {"response":r}
            
            return jsonify(processed_data)
        
        @self.app.route('/add-data', methods=['POST'])
        def add_data():
            data = request.json
            b = self.cache.checkOnCacheMemorie(data['id_dataset'])
            if b:
                processed_data = {"sendData":False}
            else:
                processed_data = {"sendData":True}

            return jsonify(processed_data) 
        
        @self.app.route('/delete-data', methods=['GET'])
        def delete_data():
            id_dataset = request.args.get("id_dataset")
            
            r = self.cache.deleteFromCache(id_dataset,ds_size=100)

            stats = self.cache.getStats()[0][1]
            self.cache.memory_used = int(stats["bytes"].decode())
            return jsonify({
                "reponse":r,
                "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode()) - (10*1024)
                })
        
        @self.app.route("/notify",methods=['POST'])
        def notify():
            data = request.json
            id_ds = data["id_dataset"]

            if int(data["add"]) == 1:
                while id_ds in self.cache.last_recently_used_item:
                    self.cache.last_recently_used_item.remove(id_ds)
                self.cache.last_recently_used_item.insert(0,id_ds)
                
                    
                if id_ds not in self.cache.ids_data: self.cache.ids_data.append(id_ds)
                self.writeOutput(f"{id_ds} added\n")
                
            else:
                while id_ds in self.cache.last_recently_used_item: self.cache.last_recently_used_item.remove(id_ds)
                while id_ds in self.cache.ids_data:self.cache.ids_data.remove(id_ds)
                self.writeOutput(f"{id_ds} removed\n")

            stats = self.cache.getStats()[0][1]
            return jsonify({"added":True,
                            "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode()) - (10*1024)
            })
            
        @self.app.route("/operations", methods=['POST'])
        def operation():
            data = request.json
            operations = data["operations"]
            threads = []
            for opt in operations:
                if opt[0] == 'migrate':
                    ip_dst_node = self.neighbors[opt[3]]['ip']
                    thread = threading.Thread(target=self.cache.migrateData, args=(opt[1], opt[2], ip_dst_node))
                    threads.append(thread)
                    thread.start()
            # Attendre la fin de tous les threads
            for thread in threads:
                thread.join()
            
            stats = self.cache.getStats()[0][1]
            return jsonify({"added":True,
                            "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode())- (10*1024)
            })

        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            shutdown_server = request.environ.get('werkzeug.server.shutdown')
            if shutdown_server is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            shutdown_server()
            return 'Server shutting down...'
        


        @self.app.route('/say', methods=['GET'])
        def say():
            param1 = request.args.get('num', 'Guest')
            #print(param1)
            processed_data = {"response":"good"}
            
            return jsonify(processed_data)
    
    def addRequest(self,id_dataset):
        if id_dataset in self.nb_requests_processed.keys():
            self.nb_requests_processed[id_dataset] +=1
        else:
            self.nb_requests_processed[id_dataset] =1
    def run(self):
        try:
            self.app.run(host="0.0.0.0", port=self.port)
            return True
        except :
            return False  

        
    def writeOutput(self, str):
        out = open(f"/tmp/log_{self.cache.id_node}.txt",'a')
        out.write(f"{str}")
        out.close()
          
        
    
"""
import threading
import time

def run_flask_app():
    cache_server = CacheManagerServer(None,port=8888)
    server_is_running = cache_server.run()

def main_algorithm():
    for i in range(5):
        import requests
        retur = requests.get('http://127.0.0.1:8888/say', params={"num":i})
        print(f"Running algorithm step {i}")
        time.sleep(1)

if __name__ == '__main__':
    flask_process = multiprocessing.Process(target=run_flask_app)
    flask_process.start()
    time.sleep(1)

    # Trigger shutdown
    """
    

