import sys
import pandas as pd

# to get system (user input provided) arguments, use sys.argv -> useful to parameterize pipelines
# example 1:
month = int(sys.argv[1])
print(f'Hello pipeline, month={month}')

# example 2:

df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df["month"] = month
print(df.head())

# example 3: apache parquet is open-source, column-oriented data file format (unlike csv which stores data in rows) and is ideal for 
# large-scale datasets and distributed systems

df.to_parquet(f"output_{month}.parquet")

# example 4: postgreSQL is an open-source object-relational database system that supports both SQL and JSON
# in order to interact with a PostgreSQL database, useful to use a CLI tool like pgcli

'''
Run a containerized version of Postgres that doesn't require any installation steps

docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18

Connect to it using pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi

-- List tables
\dt

-- Create a test table
CREATE TABLE test (id INTEGER, name VARCHAR(50));

-- Insert data
INSERT INTO test VALUES (1, 'Hello Docker');

-- Query data
SELECT * FROM test;

-- Exit
\q
  
'''

