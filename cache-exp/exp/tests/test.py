import redis
import os

file_name = '/tmp/tmp.bin'
file_size_octet = int(600)*1024
with open(file_name, "wb") as p:
    p.write(os.urandom(file_size_octet))
with open(file_name, "rb") as p:
    content = p.read()

servers = ["{ip_node}:{MEMCACHED_LISTENING_PORT}"]  # Adresse du serveur Memcached

#client = pylibmc.Client(servers, binary=True, behaviors={"tcp_nodelay": True})
client = redis.Redis(host='0.0.0.0', port=2222, db=0, decode_responses=True)  # Use a valid port like 6379


r = client.set('ds11', content)
print(f'zjouter {r}\n')

r = client.set('ds1', content)
print(f'zjouter {r}\n')
print( True)