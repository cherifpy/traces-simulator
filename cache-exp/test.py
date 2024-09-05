import redis

r = redis.Redis(host='0.0.0.0', port=5555)
x = r.set('h', 'hello')

keys = r.keys('*')
memory_info = r.info('memory')
print(keys)

r.delete("a")
keys = r.keys('*')
memory_info = r.info('memory')
print(keys)
"""with open("file.txt", "w") as f:
    f.write("A" * 500 * 1024) """
import os
file_name = '/tmp/tmp.bin'
file_size_octet = int(1024*100)*1024
with open(file_name, "wb") as p:
    p.write(os.urandom(file_size_octet))
with open(file_name, "rb") as p:
    content = p.read()
try:
    
    x = r.set('azgsdaz',content)
    print(x)
except Exception as e:
    print(e)