# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

import stomp

from book_spiders.conf import Conf
from book_spiders.mylogger import Logger


class ActivemqPipeline(object):
    _logger = Logger().getLogger()
    def __init__(self):
        self.config = Conf.config
        self._logger.info("正在连接activemq...")
        self.conn = stomp.Connection10([(self.config['activemq']['host'], self.config['activemq']['port'])])
        self.conn.start()
        self.conn.connect()
        self._logger.info("activemq连接成功")



    def process_item(self, item, spider):
        if item['_entitycode'] == 'web_page_p_book_info_09':
            if item['is_set'] == '是':
                return item
        # 对象转字典
        dict_item = dict((name, getattr(item, name)) for name in dir(item) if not name.startswith('__')).get('_values')
        # 字典转string
        str_item = json.dumps(dict_item)#.encode('utf-8')
        self._logger.info('发送消息至ActiveMQ(09_p_spider):'+item['_entitycode']+'   ISBN:'+item['isbn'])
        try:
            self.conn.send(self.config['activemq']['queue_name'], str_item)
        except:
            # self.conn.disconnect()
            self._logger.error('activemq连接超时错误')
            spider.crawler.engine.close_spider(spider,'强制结束爬虫')
        return item


