from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import sys
import datetime
import string
import random

# AwesomeAstra boilerplate connect with SCB
clientID=sys.argv[1]
secret=sys.argv[2]
secureBundleLocation=sys.argv[3]
cloud_config= {
    'secure_connect_bundle': secureBundleLocation
}
auth_provider = PlainTextAuthProvider(clientID, secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# generate data for a new record
v_id = uuid.uuid4()
v_created_at = datetime.datetime.now()
v_string = ''.join(random.choices(string.ascii_letters, k=10))
v_number = random.randint(0, 10000)
v_weights = [random.random(), random.random(), random.random()]

# prepared statement for insert
insert_stmt = f"INSERT INTO demo.demo_singleton (id, created_at, string, number, weights) VALUES (?, ?, ?, ?, ?)"
print(insert_stmt)
insert_prepared = session.prepare(insert_stmt)

# bind insert statement
print(f"Running statement bound with {v_id}, {v_created_at}, {v_string}, {v_number}, {v_weights}")
session.execute(insert_prepared, [v_id, v_created_at, v_string, v_number, v_weights])

# prepared statement for query
query_stmt = f"SELECT created_at, string, number, weights FROM demo.demo_singleton ORDER BY weights ANN OF ? limit 1 ALLOW FILTERING"
print(query_stmt)
query_prepared = session.prepare(query_stmt)

print(f"Running statement bound with {v_weights}")
result = session.execute(query_prepared, [v_weights])

print(f"Result: {result[0]}")