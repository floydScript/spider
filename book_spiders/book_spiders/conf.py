import yaml
import os

class Conf():
    baseDir = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(baseDir, 'conf/application-dev.yaml')
    config = {}
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            config = yaml.load(f)
    else:
        print('appplication配置文件不存在')