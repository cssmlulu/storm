import SimpleXMLRPCServer
import threading
import time
import pickle


class AutoStopThread(threading.Thread):
    def __init__(self, server):
        threading.Thread.__init__(self)
        self.__server = server

    def run(self):
        while self.__server.jobsPending():
            time.sleep(1)
        self.__server.stop()


class Job(object):
    def __init__(self, strategyParameters):
        self.__strategyParameters = strategyParameters
        self.__id = id(self)

    def getId(self):
        return self.__id

    def getNextParameters(self):
        ret = None
        if len(self.__strategyParameters):
            ret = self.__strategyParameters.pop()
        return ret


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
    rpc_paths = ('/PyAlgoTradeRPC',)


class Server(SimpleXMLRPCServer.SimpleXMLRPCServer):
    defaultBatchSize = 1

    def __init__(self, address, port, autoStop=True):
        SimpleXMLRPCServer.SimpleXMLRPCServer.__init__(self, (address, port), requestHandler=RequestHandler, logRequests=False, allow_none=True)
        self.__prod = None
        self.__prodtype = None

        self.__activeJobs = {}
        self.__activeJobsLock = threading.Lock()
        self.__parametersLock = threading.Lock()
        self.__parametersIterator = None
        if autoStop:
            self.__autoStopThread = AutoStopThread(self)
        else:
            self.__autoStopThread = None
        self.register_introspection_functions()
        self.register_function(self.getProd, 'getProd')
        self.register_function(self.getProdtype, 'getProdtype')
        self.register_function(self.getNextJob, 'getNextJob')
        self.__forcedStop = False

    def __getNextParams(self):
        ret = []

        # Get the next set of parameters.
        with self.__parametersLock:
            if self.__parametersIterator is not None:
                try:
                    for i in xrange(Server.defaultBatchSize):
                        ret.append(self.__parametersIterator.next())
                except StopIteration:
                    self.__parametersIterator = None
        return ret

    def getProd(self):
        return self.__prod
    def getProdtype(self):
        return self.__prodtype

    def getNextJob(self):
        ret = None
        params = []

        # Get the next set of parameters.
        params = self.__getNextParams()

        # Map the active job
        if len(params):
            ret = Job(params)
            with self.__activeJobsLock:
                self.__activeJobs[ret.getId()] = ret

        return pickle.dumps(ret)

    def jobsPending(self):
        if self.__forcedStop:
            return False

        with self.__parametersLock:
            jobsPending = self.__parametersIterator is not None
        with self.__activeJobsLock:
            activeJobs = len(self.__activeJobs) > 0
        return jobsPending or activeJobs


    def stop(self):
        self.shutdown()

    def serve(self, prod, prodtype, strategyParameters):
        self.__prod = prod
        self.__prodtype = prodtype
        ret = None
        try:

            self.__parametersIterator = iter(strategyParameters)

            if self.__autoStopThread:
                self.__autoStopThread.start()

            self.serve_forever()

            if self.__autoStopThread:
                self.__autoStopThread.join()

        finally:
            self.__forcedStop = True
        return ret


