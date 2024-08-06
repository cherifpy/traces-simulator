from flask import Flask, request, jsonify
from ..messages import Task
from queue import Queue
from ...cache import Cache

class CacheManagerServer:
    def __init__(self, cache:Cache ,host='localhost', port=5000):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.recieved_task = Queue()
        self.setup_routes()
        self.cache = cache

    def setup_routes(self):

        @self.app.route('/execut', methods=['POST'])    
        def process_data():
            data = request.json

            processed_data = {"response":"good"}
            #{"task": data["task"], "type":data["type"] ,"status": "processed"}
            self.recieved_task.put(Task.from_json(data["task"]))

            return jsonify(processed_data)
        
        @self.app.route('/infos', methods=['GET'])
        def get_info():
            data = {
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

    def run(self):
        try:
            self.app.run(host=self.host, port=self.port)
            return True
        except :
            return False    


