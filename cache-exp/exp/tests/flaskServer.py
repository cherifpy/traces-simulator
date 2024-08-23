from flask import Flask, request, jsonify

class FlaskServer:
    def __init__(self, host='0.0.0.0', port=8888):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.setup_routes()

    def setup_routes(self):
        
        @self.app.route('/hello', methods=['GET'])
        def process_data():
           
            processed_data = {"response": "Hello World"}
            return jsonify(processed_data)
        
    def run(self):
        self.app.run(host=self.host, port=self.port)

if __name__ == '__main__':
    service1 = FlaskServer()
    service1.run()
import requests
url = f'http://172.16.96.71:8888/hello'

response = requests.get(url)

print(response.text)