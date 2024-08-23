from crypt import methods
import sys
import os
from urllib import response
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask, request, jsonify
from communication.messages import Task
from queue import Queue
import multiprocessing
import importlib.util
from cache import Cache

class CacheManagerServer:
    def __init__(self,storage_space, id_node,neighbors, host='localhost', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.cache = Cache(storage_space, id_node)
        self.neighbors = neighbors
        
        
        self.client = self.cache.connectToMemcache()
        if self.client != None:
            self.writeOutput("connected to memecached\n")
        else:
            self.writeOutput("not connected to memecached\n")

    def setup_routes(self):
        #used
        @self.app.route('/execut', methods=['POST'])    
        def process_data():
            print("recieved task")
            self.writeOutput("recieved task")
            data = request.json

            task = Task.from_json(data["task"])
            #{"task": data["task"], "type":data["type"] ,"status": "processed"}
            self.recieved_task.put(task)
            
            b1 = self.cache.checkOnCacheMemorie(task.id_dataset)
            b2, condidates = self.cache.predictEviction(task.ds_size)

            if b1:
                processed_data = {"sendData":False, "eviction":False}
            else:
                
                processed_data = {"sendData":True, "eviction":b2, "condidates":condidates}

                #TODO:this function have a probleme
                self.cache.addData(task.id_dataset, task.ds_size)

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
                    "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode()),
                    'keys': self.cache.getKeys()
                }
                self.cache.memory_used  = int(stats["bytes"].decode())
            else:
                data = {
                    "id_node": self.cache.id_node,
                    "storage_space": self.cache.cache_size,
                    "remaining_space":self.cache.cache_size - self.cache.memory_used,
                    'keys': self.cache.getKeys()
                }
            self.writeOutput("info sended\n")
            return jsonify(data)
        
        #used
        @self.app.route("/access-data", methods=['GET'])
        def ckeckData():
            stats = self.cache.getStats()[0][1]
            id_ds = request.args.get("id_dataset")
            b = self.cache.accessData(id_ds)

            return jsonify({
                "reponse":b,
                "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode())
                })
        
        @self.app.route("/get-infos-for-evection", methods=["GET"])
        def infoForEvection():

            stats = self.cache.getStats()[0][1]
            return jsonify({
                "remaining_space":int(stats["limit_maxbytes"].decode()) - int(stats["bytes"].decode()),
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
        #TODO
        @self.app.route('/send-and-delete', methods=["GET"])
        def sendAndDelete():
            id_ds = request.args.get("id_dataset")
            ip_dst_node = request.args.get("ip_dst_node")
            ds_size = request.args.get("ds_size")

            b = self.cache.deleteFromCache(id_ds)
            
            if b:
                t = self.cache.sendDataSetTo(
                    ip_dst=ip_dst_node,
                    id_dataset=id_ds,
                    size_ds=ds_size
                    )
                response = {"sended":t}
            else:
                response = {"sended":b}

            return jsonify(response)


        @self.app.route('/add-data', methods=['POST'])
        def add_data():
            data = request.json
            b = self.cache.checkOnCacheMemorie(data['id_dataset'])
            if b:
                processed_data = {"sendData":False}
            else:
                processed_data = {"sendData":True}

            return jsonify(processed_data) 
        
        @self.app.route('/delete-data', methods=['PULL'])
        def delete_data():
            data = request.json
            self.cache.deleteFromCache(data["id_dataset"])
            processed_data = {"response":"good"}
            return jsonify(processed_data) 
        
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
            print(param1)
            processed_data = {"response":"good"}
            
            return jsonify(processed_data)
        
    def run(self):
        try:
            self.app.run(host="0.0.0.0", port=self.port)
            return True
        except :
            return False  

        
    def writeOutput(self, str):
        out = open(f"/tmp/log_{self.cache.id_node}.txt",'a')
        out.write(str)
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
    

