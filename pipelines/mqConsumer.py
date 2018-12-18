# -*-coding:utf-8-*-
import json
import stomp
import time
from conf import Conf
from mylogger import Logger
from pipelines import ElasticSearchPipelines, MySqlPipelines, SaveImgPipelines

config = Conf.config

_logger = Logger().getLogger()


# 消息监听器
class BookListener(object):
    mysql_pipe = MySqlPipelines()
    image_pipe = SaveImgPipelines()
    es_pipe = ElasticSearchPipelines()


    def on_message(self, headers, message):
        """
        处理接收到的消息
        :param headers:
        :param message:
        :return:
        """
        item = json.loads(message)
        # _logger.info('接收信息：' + item['isbn'])
        # 数据推送到mysql中
        self.mysql_pipe.process_item(item)
        # 下载图片
        self.image_pipe.process_item(item)
        if item['_entitycode'] =='web_page_p_book_info_09':
            # 去掉is_set字段
            item.pop('is_set')
        # 数据推送到es中
        self.es_pipe.process_item(item)



def receive_from_queue():
    """
    实例监听者，从队列接收消息
    :return:
    """
    conn = stomp.Connection10([(config['activemq']['host'], config['activemq']['port'])])
    conn.set_listener(config['activemq']['listener_name'], BookListener())
    conn.start()
    conn.connect()
    conn.subscribe(config['activemq']['queue_name'])
    time.sleep(1)  # secs
    conn.disconnect()


if __name__ == '__main__':
    while True:
        receive_from_queue()