from pymemcache.client.base import Client
import copy
import re
from exp.params import MEMCACHED_LISTENING_PORT
# Initialize Memcached client using pymemcache$
client = Client(("127.0.0.1", MEMCACHED_LISTENING_PORT))

# Function to retrieve keys from Memcached
def get_memcached_keys():
    # Get item statistics from Memcached to identify slab IDs
    stats_items = client.stats('items')

    # List to store keys
    keys = []
    m = 0
    # Parse slab IDs from the 'stats items' command
    for stat_key, stat_value in stats_items.items():
        if stat_key.decode().startswith('items:'):
            parts = stat_key.decode().split(':')
            if len(parts) == 3 and parts[2] == 'number':
                slab_id = parts[1]

                # Get keys from the specified slab using 'stats cachedump' with _misc method
                
                    # Execute the cachedump command for this slab and limit it to 100 items
                cachedump = client.stats(f'cachedump',f'{slab_id}','100')
                print(cachedump)
                
                # Parse and add the retrieved keys to the list
                for key, infos in cachedump.items():
                    numbers = re.findall(r'\d+', infos.decode())
                    m += int(numbers[0])
                    #key = line.split(' ')[1]
                    keys.append(key.decode())
            
    print(m)
    # Deep copy the keys to ensure they are not modified
    ids_data = copy.deepcopy(keys)
    return ids_data




import threading

def test():
    print("hello")

r = threading.Thread(target=test)
print(r)