import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask, request, jsonify
from communication.messages import Task
from queue import Queue
import multiprocessing
import importlib.util
from cache import Cache

class CacheManagerServer:
    def __init__(self,storage_space, id_node, host='localhost', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.cache = Cache(storage_space, id_node)
        
        
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
            b2 = True if (self.cache.memory_used + int(task.ds_size) > self.cache.cache_size) else False
            if b1:
                processed_data = {"sendData":False, "eviction":False}
            else:
                #TODO:need to know wich data will be evicted
                processed_data = {"sendData":True, "eviction":b2}
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
                    "storage_space": stats["limit_maxbytes"],
                    "remaining_space":stats["limit_maxbytes"] - stats["bytes"],
                }
                self.cache.memory_used  = stats["bytes"]
            else:
                data = {
                    "id_node": self.cache.id_node,
                    "storage_space": self.cache.cache_size,
                    "remaining_space":self.cache.cache_size - self.cache.memory_used,
                }
            self.writeOutput("info sended\n")
            return jsonify(data)
        
        #used
        @self.app.route("/access-data", methods=['GET'])
        def ckeckData():
            id_ds = request.args.get("id_dataset")
            b = self.cache.accessData(id_ds)

            return jsonify({"reponse":b})
        
        
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
            self.cache.deleteFromCache(data["key"])
            
            processed_data = {"response":"good"}
            
            return jsonify(processed_data) 
        
        @self.app.route('/say', methods=['GET'])
        def say():
            param1 = request.args.get('num', 'Guest')
            print(param1)
            processed_data = {"response":"good"}
            
            return jsonify(processed_data) 
        
        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            shutdown_server = request.environ.get('werkzeug.server.shutdown')
            if shutdown_server is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            shutdown_server()
            return 'Server shutting down...'
        
    def run(self):
        try:
            self.app.run(host="0.0.0.0", port=self.port)
            return True
        except :
            return False  

    def run_on_thread(self):
        try:
            self.app.run(host=self.host, port=self.port)
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
    

