from lstore.table import Table, Record
from lstore.index import Index
import threading


class TransactionWorker:

    def __init__(self, transactions=[]):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = None
        self.lock = threading.Lock()
        self.is_in_use = False


    def add_transaction(self, t):
        self.transactions.append(t)



    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()


    def join(self):
        if self.thread == None:
            return
        self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            while self.is_in_use:
                continue
            self.lock.acquire()
            self.stats.append(transaction.run())
            self.is_in_use = True
            self.lock.release()
            self.is_in_use = False
        self.result = len(list(filter(lambda x: x, self.stats)))
