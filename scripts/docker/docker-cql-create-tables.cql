CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : '1' }
    AND durable_writes = true;

CREATE TABLE IF NOT EXISTS test.agg0 (
    continent text PRIMARY KEY,
    population_count int,
    population_sum int);
    
