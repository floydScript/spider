import configparser
import os
from elasticsearch import Elasticsearch
import hashlib

import pymysql

from conf import Conf
from mylogger import Logger
from pipelines import ElasticSearchPipelines


class ParseComment(object):
    _logger = Logger.getLogger(Logger())
    def __init__(self):
        #初始化
        config = Conf.config

        # 初始化es数据通道
        self.es_pipe = ElasticSearchPipelines()

        self.mysql_hosts = config['mysql']['host']
        self.mysql_port = config['mysql']['port']
        self.mysql_user = config['mysql']['username']
        self.mysql_password = config['mysql']['password']
        self.mysql_db = config['mysql']['dbname']
        self.mysql_charset = config['mysql']['charset']
        self.mysql_table = config['mysql']['comment_table']

        # 建立数据库连接
        self.conn = pymysql.connect(host=self.mysql_hosts,
                                    port=int(self.mysql_port),
                                    user=self.mysql_user,
                                    password=self.mysql_password,
                                    db=self.mysql_db,
                                    charset=self.mysql_charset,
                                    cursorclass = pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()




    # 将数据推送至ES
    def parse_item(self):
        self._logger.info("查询待推送的数据：" + self.mysql_table)
        self.cursor.execute("""SELECT * FROM %s limit 0,100000""" % self.mysql_table)

        result = self.cursor.fetchall()

        for row in result:
            # 解析数据库结果
            item = self.initItem(row)
            self.es_pipe.process_item(item)


    # 释放资源
    def close_spider(self, spider):
        self.conn.close()
        self.cursor.close()

    # 解析mysql数据库
    def initItem(self,row):

        item = {}
        item['isbn'] = row['isbn']
        item['uri'] = row['uri']
        skuid = row['uri'].split('/')[7]
        item['bookname'] = row['bookname']
        item['sourcetype'] = row['sourcetype']
        item['collectiontime'] = row['collectiontime']
        item['publishtime'] = row['publishtime']
        item['username'] = row['username']
        # 初始化int类型的数据为0
        hitcount = row['hitcount']
        if not hitcount:
            hitcount = '0'
        item['hitcount'] = hitcount
        # 初始化int类型的数据为0
        follownum = row['follownum']
        if not follownum:
            follownum = '0'
        item['follownum'] = follownum
        # 初始化int类型的数据为0
        suportnum = row['suportnum']
        if not suportnum:
            suportnum = '0'
        item['suportnum'] = suportnum
        # 初始化int类型的数据为0
        opposnum = row['opposnum']
        if not opposnum:
            opposnum = '0'
        item['opposnum'] = opposnum

        item['commentid'] = row['commentid']
        item['followcommentid'] = row['followcommentid']
        item['commenttitle'] = row['commenttitle']
        item['commenttype'] = row['commenttype']
        item['comment'] = row['comment']
        score = row['score']
        if not score:
            score = '5'
        item['score'] = score
        level = row['level']
        if not level:
            level = '0'
        item['level'] = level

        item['commpoint'] = row['commpoint']
        item['type'] = row['type']
        item['sitename'] = row['sitename']
        item['_entitycode'] = row['_entitycode']
        item['_row'] = row['_row']
        item['skuid'] = skuid
        return item

if __name__ == "__main__":
    print('1')
    aa = ParseComment()
    print('1')
    aa.parse_item()
    print('1')