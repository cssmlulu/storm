#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

import logging
import time
def setlog(name):
    logger=logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')
    logfile = BASE_DIR+"/log/storm"+time.strftime("%Y%m%d")+'.log'
    handler=logging.FileHandler(logfile)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger 
