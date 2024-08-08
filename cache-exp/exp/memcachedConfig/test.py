"""from pymemcache.client import base
import sys

print("hello")
params = sys.argv[1]

f = open("/tmp/log.txt", 'w')

# Se connecter à Memcached sur l'adresse IP spécifiée
client = base.Client(params, 11211)

# Définir une clé/valeur
client.set('my_key', 'my_value')

# Récupérer une valeur
value = client.get('my_key')

f.write(f'La valeur pour "my_key" est : {value.decode()}')

# Fermer la connexion
client.close()"""

"""
import sys
import pickle
from pymemcache.client.base import Client

def pickle_serializer(key, value):
    return pickle.dumps(value), 1  # The flag '1' is just a custom flag to identify pickle serialized data

def pickle_deserializer(key, value, flags):
    if flags == 1:
        return pickle.loads(value)
    return value  # If the flag is not 1, return the value as is


params = sys.argv[1]
# Initialize the client with Pickle serialization
client = Client(("localhost", 11211))
client.set('cherif_key', "cherf")

retrieved_person = client.get('cherif_key')
print(retrieved_person)
# Define a sample class
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f'Person(name={self.name}, age={self.age})'

# Create an instance of Person
person = Person('Alice', 30)

# Serialize and set the object in Memcached
client.set('person_key', person)

# Retrieve and deserialize the object from Memcached
retrieved_person = client.get('cherif_key')
print(retrieved_person)
f = open("/tmp/log.txt", 'w')
f.write(retrieved_person)"""

import pylibmc
import sys
# List of Memcached servers
servers = f"{sys.argv[1]}:11211"

# Create a client
client = pylibmc.Client(servers, binary=True, behaviors={"consistent_hash": True})

# Set and get values
client.set("key", "value")
print(client.get("key"))  # Output: 'value'

