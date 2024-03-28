from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import randint, seed
from time import time 
import shutil
import os
stock_tracking_enabled = True
purchase_tracking_enabled = True

overall_score = 0
passed_tests = 0  # Counter for passed tests
failed_tests = 0  # Counter for failed tests

def verify_records(db, table, expected_records):

    query = Query(table)
    all_matched = True
    for key, expected in expected_records.items():
        result = query.select(key, 0, [1, 1, 1, 1, 1])
        if not result or result[0].columns != expected:
            print(f"Record mismatch or not found for key {key}: expected {expected}, got {result[0].columns if result else 'None'}")
            all_matched = False
    return all_matched



def delete_test_databases():
    directories_to_delete = ['./Tracking_Test_DB', './Purchase_Tracking_Test']
    for directory in directories_to_delete:
        try:
            if os.path.exists(directory):
                shutil.rmtree(directory) 
        except Exception as e:
            continue



def stock_test():
    global overall_score, passed_tests, failed_tests
    start_time = time() 

    print("Initiating stock tracking test")
    if stock_tracking_enabled:
        lineageDB = Database()
        lineageDB.open('./Tracking_Test_DB')

        # Creates table of stock info for test
        stock_table = lineageDB.create_table('Stocks', 5, 0)
        stock_query = Query(stock_table)
        seed(3562901)

        record_count = 1000 
        transaction_count = 100000
        worker_threads = 8

        stock_ids = []
        stock_records = {}

        insert_transactions = [Transaction() for _ in range(transaction_count)]

        for i in range(record_count):
            stock_id = 92106429 + i
            stock_ids.append(stock_id)
            stock_records[stock_id] = [stock_id, randint(i * 20, (i + 1) * 20), randint(100, 500), randint(10, 50), randint(1, 10)]
            current_transaction = insert_transactions[i % transaction_count]
            current_transaction.add_query(stock_query.insert, stock_table, *stock_records[stock_id])

        transaction_workers = [TransactionWorker() for _ in range(worker_threads)]
        for i, transaction in enumerate(insert_transactions):
            transaction_workers[i % worker_threads].add_transaction(transaction)

        for worker in transaction_workers:
            worker.run()
        for worker in transaction_workers:
            worker.join()

        
    if verify_records(lineageDB, stock_table, stock_records):
        print("Stock tracking test passed.")
        passed_tests += 1
    else:
        print("Stock tracking test failed.")
        failed_tests += 1


        end_time = time() 
        print(f"Stock insertion test completed in {end_time - start_time:.2f} seconds")

def purchase_test():
    global overall_score, passed_tests, failed_tests
    start_time = time()  

    print("Initiating purchase tracking test")
    if purchase_tracking_enabled:
        lineageDB = Database()
        lineageDB.open('./Purchase_Tracking_Test')

        purchases_table = lineageDB.create_table('Purchases', 5, 0)
        purchases_query = Query(purchases_table)
        seed(3562901)

        record_count = 10
        transaction_count = 10
        worker_threads = 8

        purchase_ids = []
        purchase_records = {}

        insert_transactions = [Transaction() for _ in range(transaction_count)]

        for i in range(record_count):
            purchase_id = randint(100000, 999999)
            purchase_ids.append(purchase_id)
            purchase_records[purchase_id] = [purchase_id, randint(1, record_count), randint(1, 100), randint(100, 500), randint(1, 10)]
            current_transaction = insert_transactions[i % transaction_count]
            current_transaction.add_query(purchases_query.insert, purchases_table, *purchase_records[purchase_id])

        transaction_workers = [TransactionWorker() for _ in range(worker_threads)]
        for i, transaction in enumerate(insert_transactions):
            transaction_workers[i % worker_threads].add_transaction(transaction)

        for worker in transaction_workers:
            worker.run()
        for worker in transaction_workers:
            worker.join()

        if verify_records(lineageDB, purchases_table, purchase_records):
            print("Purchase tracking test passed.")
            passed_tests += 1
        else:
            print("Purchase tracking test failed.")
            failed_tests += 1

        end_time = time() 
        print(f"Purchase insertion test completed in {end_time - start_time:.2f} seconds")

def run_tests():
    global stock_tracking_enabled, purchase_tracking_enabled, passed_tests, failed_tests
    if stock_tracking_enabled:
        stock_test()
    if purchase_tracking_enabled:
        purchase_test()

    # Final Report Print
    print("\n------------------------------------")
    print(f"Total Passed Tests: {passed_tests}")
    print(f"Total Failed Tests: {failed_tests}")
    total_tests = passed_tests + failed_tests
    if total_tests > 0:
        pass_rate = (passed_tests / total_tests) * 100
        print(f"Pass Rate: {pass_rate:.2f}%")
    else:
        print("No tests were executed.")
    print("--------------------------------------\n")
delete_test_databases() # if you want to look at the db created remove this line (Note that data is stored in bin files and will be unreadable)
run_tests()
