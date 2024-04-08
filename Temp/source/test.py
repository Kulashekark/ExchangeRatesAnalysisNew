import json
import duckdb
from avro.datafile import DataFileReader
from avro.io import DatumReader


# Step 1: Read Avro file and extract data
def read_avro_file(avro_filename):
    with open(avro_filename, "rb") as avro_file:
        reader = DataFileReader(avro_file, DatumReader())
        data = [record for record in reader]
    return data

# Step 2: Create or connect to DuckDB database
connection = duckdb.connect(database=':memory:', read_only=False)

# Step 3: Create DuckDB table
def create_duckdb_table(schema):
    print(type(schema))
    x={}
    x=schema
    create_table_query = "CREATE TABLE avro_data ("
    print("field: "+str(schema))
    print(x[0])
    # for field in (json.loads(str(schema)))["fields"]:
    #     print(type(field))
    #     create_table_query += f"{field['name']} {field['type'].lower()}, "
    # create_table_query = create_table_query[:-2] + ")"
    # print(create_table_query)
    # connection.execute(create_table_query)

# Step 4: Insert data into DuckDB table
def insert_data_into_duckdb_table(data):
    for record in data:
        values = ", ".join([f"'{record[field['name']]}'" if isinstance(record[field['name']], str) else str(record[field['name']]) for field in schema['fields']])
        insert_query = f"INSERT INTO avro_data VALUES ({values})"
        connection.execute(insert_query)

# Example usage
avro_filename = '.\\twitter.avro'
avro_data = read_avro_file(avro_filename)
print(type(avro_data))
#schema = avro_data[0].schema

with open(avro_filename, "rb") as avro_f:
    reader = DataFileReader(avro_f, DatumReader())
    schema = reader.meta['avro.schema']


create_duckdb_table(schema)
insert_data_into_duckdb_table(avro_data)

# Optionally, you can query the DuckDB table to verify the data is inserted correctly
result = connection.execute("SELECT * FROM avro_data").fetchall()
print(result)
