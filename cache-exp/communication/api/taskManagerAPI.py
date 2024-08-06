from flask import Flask, request, jsonify

class TaskManagerAPI:
    def __init__(self, host='localhost', port=5000):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
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
            data = request.json
            # Traiter les données reçues
            processed_data = {"received": data, "status": "processed"}
            return jsonify(processed_data)
        
        

            

    def run(self):
        self.app.run(host=self.host, port=self.port)


    def run(self):
        self.app.run(port=5000)

if __name__ == '__main__':
    service1 = TaskManagerAPI()
    service1.run()
