import datetime
import logging
from logging import config
import yaml
import os

baseDir = os.path.dirname(os.path.realpath(__file__))


class Logger():
    def __init1__(self):
        path = os.path.join(baseDir, 'conf/logging.yaml')
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                conf = yaml.load(f)
                today = str(datetime.datetime.now()).split(".")[0].split()[0]
                conf['handlers']['file']['filename'] ="log/"+today+".log"
                logging.config.dictConfig(conf)
        else:
            logging.basicConfig(level=logging.INFO)
            print('logging配置文件不存在')

    def getLogger(self):
        return logging.getLogger()
