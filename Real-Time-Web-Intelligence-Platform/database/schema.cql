CREATE KEYSPACE realtime
WITH replication = {
'class': 'SimpleStrategy',
'replication_factor': 1
};