'''
we have source files with a certain format and each file has 200 columns and there is a process that takes the source
files and loads into hbase and moves it into sql data warehouse. We have to create automated test scripts that compares
with with is with hbase and sql data warehouse. load into hbase and query the flat file, query the hbase, and compare.
compare each row. load into hbase and query.

https://community.hortonworks.com/articles/4942/import-csv-data-into-hbase-using-importtsv.html
https://www.briandunning.com/sample-data/
http://python-phoenixdb.readthedocs.io/en/latest/
https://phoenix.apache.org/faq.html
https://phoenix.apache.org/bulk_dataload.html

hbase shell
create 'CUSTOMERS', 'cf'
count 'CUSTOMERS'
scan 'CUSTOMERS'
exit

hdfs dfs -put customers-with-out-header-500.csv
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv '-Dimporttsv.separator=|' -Dimporttsv.columns="HBASE_ROW_KEY,cf:first_name,cf:last_name,cf:company_name,cf:address,cf:city,cf:county,cf:state,cf:zip,cf:phone1,cf:phone2,cf:email,cf:web" CUSTOMERS customers-with-out-header-500.csv

sudo python3 -m pip install happybase
sudo python3 -m pip install pandas
sudo python3 -m pip install numpy
sudo python3 -m pip install ipython

list of hbase tables [b'customers']
len of hbase keys 501
hbase columns [b'cf:state', b'cf:phone2', b'cf:email', b'cf:zip', b'cf:last_name', b'cf:address', b'cf:city', b'cf:company_name', b'cf:phone1', b'cf:county', b'cf:first_name', b'cf:web']
hbase columns len 12
csv file shape (500, 13)
csv columns ['index', 'first_name', 'last_name', 'company_name', 'address', 'city', 'county', 'state', 'zip', 'phone1', 'phone2', 'email', 'web']

phoenix steps
python /usr/lib/phoenix/bin/sqlline.py
CREATE TABLE "CUSTOMERSPHOENIX" (pk VARCHAR PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, company_name VARCHAR, address VARCHAR, city VARCHAR, county VARCHAR, state VARCHAR, zip VARCHAR, phone1 VARCHAR, phone2 VARCHAR, email VARCHAR, web VARCHAR)
python /usr/lib/phoenix/bin/psql.py -t CUSTOMERSPHOENIX -d "|" localhost customers-with-out-header-500.csv
SELECT A.*, B.* FROM CUSTOMERS AS A FULL JOIN CUSTOMERSPHOENIX AS B ON (A.PK = B.PK) WHERE A.PK IS NULL OR B.PK IS NULL

hive steps

CREATE EXTERNAL TABLE customers_hive(key string, first_name string, last_name string, company_name string, address string, city string, county string, state string, zip string, phone1 string, phone2 string, email string, web string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, cf:first_name, cf:last_name, cf:company_name, cf:address, cf:city, cf:county, cf:state, cf:zip, cf:phone1, cf:phone2, cf:email, cf:web")
TBLPROPERTIES ("hbase.table.name"="CUSTOMERS");

SELECT yourcolumns
FROM tablenames
JOIN tablenames
WHERE condition
GROUP BY yourcolumns
HAVING aggregatecolumn condition
ORDER BY yourcolumns
'''

import pandas as pd
import happybase
import phoenixdb
from pyhive import hive


connection = happybase.Connection()
connection.open()

print('list of hbase tables {}'.format(connection.tables()))

customers = connection.table('CUSTOMERS')

keys = []
data_list = []

for key, data in customers.scan():
    keys.append(key)
    data_list.append(data)

hbase_columns = [x.decode('utf-8')[3:] for x in data_list[0].keys()]

print('len of hbase keys {}'.format(len(keys)))
print('hbase columns {}'.format(hbase_columns))
print('hbase columns len {}'.format(len(hbase_columns)))

df = pd.read_csv('customers-with-header-500.csv', delimiter='|', index_col='index')

df_columns = list(df.columns)
print('csv file shape {}'.format(df.shape))
print('csv columns {}'.format(df_columns))

print('hbase columns == csv columns: {}'.format(set(hbase_columns) == set(df_columns)))
print('hbase row count == csv row count: {}'.format(len(keys) == df.shape[0]))


url = 'http://localhost:8765/'
conn = phoenixdb.connect(url, autocommit=True)

cursor = conn.cursor()
query1 = 'DROP VIEW "CUSTOMERS"'
cursor.execute(query1)
query2 = 'CREATE VIEW "CUSTOMERS" (pk VARCHAR PRIMARY KEY, "cf"."first_name" VARCHAR, "cf"."last_name" VARCHAR, "cf"."company_name" VARCHAR, "cf"."address" VARCHAR, "cf"."city" VARCHAR, "cf"."county" VARCHAR, "cf"."state" VARCHAR, "cf"."zip" VARCHAR, "cf"."phone1" VARCHAR, "cf"."phone2" VARCHAR, "cf"."email" VARCHAR, "cf"."web" VARCHAR)'
cursor.execute(query2)
query3 = 'SELECT * FROM CUSTOMERS'
cursor.execute(query3)
data = cursor.fetchall()
print(data[:2])


from pyhive import hive  # or import hive
cursor = hive.connect('localhost').cursor()
cursor.execute('SELECT * FROM customers_hive LIMIT 10')
result = cursor.fetchall()
print(len(result))
print(result)







