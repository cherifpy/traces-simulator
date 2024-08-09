from flask import Flask, request, jsonify
from messages import Task
from queue import Queue
import multiprocessing
from ..cache import Cache

class CacheManagerServer:
    def __init__(self, cache:Cache,host='localhost', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.cache = None

    def setup_routes(self):

        @self.app.route('/execut', methods=['POST'])    
        def process_data():
            data = request.json

            task = Task.from_json(data["task"])
            #{"task": data["task"], "type":data["type"] ,"status": "processed"}
            self.recieved_task.put(task)
            b = self.cache.checkOnCacheMemorie(task.id_dataset)
            if b:
                processed_data = {"sendData":False}
            else:
                processed_data = {"sendData":True}
                
            return jsonify(processed_data)
        
        @self.app.route('/infos', methods=['GET'])
        def get_info():
            data = {
                "id_node": self.cache.id_node,
                "storage_space": self.cache.cache_size,
                "remaining_space":self.cache.cache_size - self.cache.memory_used,
            }
            return jsonify(data)

        @self.app.route('/send-data', methods=['GET'])
        def send_data():
            param1 = request.args.get('id-ds')
            param2 = request.args.get('id-node')
            
            self.cache.sendDataSetTo(param1, param2)

            processed_data = {"response":"good"}
            
            return jsonify(processed_data)
        
        @self.app.route('/add-data', methods=['POST'])
        def add_data():
            data = request.json
            self.cache.addToMemcache(data["key"], data["value"])
            
            processed_data = {"response":"good"}
            
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
            self.app.run(host=self.host, port=self.port)
            return True
        except :
            return False  

    def run_on_thread(self):
        try:
            self.app.run(host=self.host, port=self.port)
            return True
        except :
            return False  
          
        
    

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
    main_algorithm()

    # Trigger shutdown
    
    

