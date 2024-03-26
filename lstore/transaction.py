from lstore.table import Table, Record
from lstore.index import Index
import threading


class Transaction:


    def __init__(self):
        self.queries = []
        self.is_in_use = False
        self.lock = threading.Lock()


    def add_query(self, query, table, *args):
        self.queries.append((query, args))

        
    def run(self):
        for query, args in self.queries:
            while self.is_in_use:
                continue
            self.lock.acquire()
            result = query(*args)
            self.is_in_use = True
            self.lock.release()
            self.is_in_use = False
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        return False

    
    def commit(self):
        return True

