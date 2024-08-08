from flask import Flask, request, jsonify
from ..replicaManager import ReplicaManager


class TaskManagerAPI:
    def __init__(self, host='localhost', port=5000, replica_manager : ReplicaManager = None):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.replica_manager = replica_manager
        self.setup_routes()

    def setup_routes(self):
        
        @self.app.route('/process', methods=['POST'])
        def process_data():
            data = request.json
            # Traiter les données reçues
            processed_data = {"received": data, "status": "processed"}
            return jsonify(processed_data)

        @self.app.route('/get-data', methods=['GET'])
        def get_data_set():
            param1 = request.args.get('id-ds')
            param2 = request.args.get('id-node')
            data = request.json
            # Traiter les données reçues
            self.replica_manager.send_data()

            processed_data = {"status": "Good"}

            return jsonify(processed_data)
        
        @self.app.route('/evect-data', methods=['GET'])
        def delete_data():
            data = request.json
            decision = self.replica_manager.evectData(data["id_node"], data["id_dataset"], data["dataset_size"])
            processed_data = {"decision": decision}
            return jsonify(processed_data)
        
        
    def run(self):
        self.app.run(host=self.host, port=self.port)


    def run(self):
        self.app.run(port=5000)

if __name__ == '__main__':
    service1 = TaskManagerAPI()
    service1.run()