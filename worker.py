import xmlrpclib
import pickle
import time
import socket
import random
import multiprocessing

def call_function(function, *args, **kwargs):
    return function(*args, **kwargs)


def call_and_retry_on_network_error(function, retryCount, *args, **kwargs):
    ret = None
    while retryCount > 0:
        retryCount -= 1
        try:
            ret = call_function(function, *args, **kwargs)
            return ret
        except socket.error:
            time.sleep(random.randint(1, 3))
    ret = call_function(function, *args, **kwargs)
    return ret


class Worker(object):
    def __init__(self, address, port, workerName=None):
        url = "http://%s:%s/PyAlgoTradeRPC" % (address, port)
        self.__server = xmlrpclib.ServerProxy(url, allow_none=True)
        if workerName is None:
            self.__workerName = socket.gethostname()
        else:
            self.__workerName = workerName

    def getProd(self):
        ret = call_and_retry_on_network_error(self.__server.getProd, 10)
        return ret
        
    def getProdtype(self):
        ret = call_and_retry_on_network_error(self.__server.getProdtype, 10)
        return ret

    def getNextJob(self):
        ret = call_and_retry_on_network_error(self.__server.getNextJob, 10)
        ret = pickle.loads(ret)
        return ret

    def __processJob(self, job, prod, prodtype):
        parameters = job.getNextParameters()
        while parameters is not None:
            self.runStrategyandBacktest(prod, prodtype, *parameters)
            parameters = job.getNextParameters()


    # Run the strategy and return the result.
    def runStrategyandBacktest(self, prod, prodtype, parameters):
        raise Exception("Not implemented")

    def run(self):
        prod = self.getProd()
        prodtype = self.getProdtype()
        # Process jobs
        job = self.getNextJob()
        while job is not None:
            self.__processJob(job, prod, prodtype)
            job = self.getNextJob()



