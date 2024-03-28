from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
import shutil
import os


def delete_test_databases():
    directory_to_delete = 'Lineage_DB'  
    try:
        shutil.rmtree(directory_to_delete, ignore_errors=True) 
    except Exception as e:
        print(f"Error deleting {directory_to_delete}: {e}") 

delete_test_databases() # Ensures test is reset
total_time = process_time()

db = Database()
products_table = db.create_table('Products', 5, 0)
query = Query(products_table)
keys = []

insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(100000000 + i, randrange(1, 5), randrange(0, 10), randrange(1, 5), 0)
    keys.append(100000000 + i)
insert_time_1 = process_time()

print("Inserting 10k product records took:     \t\t\t", insert_time_1 - insert_time_0)


update_cols = [
    [None, None, None, randrange(5, 10000), None],
    [None, None, None, None, randrange(2, 8)],
    [None, randrange(250, 750), None, None, None],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k product records took:      \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys), 0, [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k product records took:      \t\t\t", select_time_1 - select_time_0)

# Measuring Performance on Prices
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 100000000 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(1, 3))
agg_time_1 = process_time()
print("Aggregate 10k of 100 product batch on prices/stock took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(100000000 + i)
delete_time_1 = process_time()
print("Deleting 10k product records took:       \t\t\t", delete_time_1 - delete_time_0)

db.close()

print("Main took: \t\t\t\t\t\t\t", process_time() - total_time)
