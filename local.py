import multiprocessing
import threading
import logging
import socket
import random
import os

import server
import worker


class ServerThread(threading.Thread):
    def __init__(self, server, prod, prodtype,strategyParameters):
        threading.Thread.__init__(self)
        self.__server = server
        self.__prod = prod
        self.__prodtype = prodtype
        self.__strategyParameters = strategyParameters

    def run(self):
        self.__results = self.__server.serve(self.__prod, self.__prodtype, self.__strategyParameters)


def worker_process(strategyClass, backtestClass, port):
    class Worker(worker.Worker):
        def runStrategyandBacktest(self, prod, prodtype, *args, **kwargs):
            st = strategyClass(prod, prodtype, *args, **kwargs)
            st.run()
            bt = backtestClass(prod, prodtype, *args, **kwargs)
            bt.run()

    # Create a worker and run it.
    name = "worker-%s" % (os.getpid())
    w = Worker("localhost", port, name)
    w.run()


def find_port():
    while True:
        ret = random.randint(1025, 65536)
        try:
            s = socket.socket()
            s.bind(("localhost", ret))
            s.close()
            return ret
        except socket.error:
            pass


def run(strategyClass, backtestClass, prod, prodtype,strategyParameters, workerCount=None):
    assert(workerCount is None or workerCount > 0)
    if workerCount is None:
        workerCount = multiprocessing.cpu_count()

    ret = None
    workers = []
    port = find_port()
    if port is None:
        raise Exception("Failed to find a port to listen")

    # Build and start the server thread before the worker processes. We'll manually stop the server once workers have finished.
    srv = server.Server("localhost", port, False)
    serverThread = ServerThread(srv, prod, prodtype,strategyParameters)
    serverThread.start()

    try:
        # Build the worker processes.
        for i in range(workerCount):
            workers.append(multiprocessing.Process(target=worker_process, args=(strategyClass, backtestClass, port)))

        # Start workers
        for process in workers:
            process.start()

        # Wait workers
        for process in workers:
            process.join()

    finally:
        # Stop and wait the server to finish.
        srv.stop()
        serverThread.join()


