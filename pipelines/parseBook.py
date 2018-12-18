
import pymysql

from categoryServer import CategoryServer
from conf import Conf
from mylogger import Logger
from orgcoderServer import OrgcoderServer
from pipelines import ElasticSearchPipelines


class ParseBook(object):
    _logger = Logger().getLogger()
    def __init__(self):
        # 初始化分类服务器，即只实例化一次数据库连接
        self.cat_server = CategoryServer()
        self.orgcode_server = OrgcoderServer()

        # 初始化es数据通道
        self.es_pipe = ElasticSearchPipelines()
        #初始化
        try:
            self.config = Conf.config

            self.mysql_hosts = self.config['mysql']['host']
            self.mysql_port = self.config['mysql']['port']
            self.mysql_user = self.config['mysql']['username']
            self.mysql_password = self.config['mysql']['password']
            self.mysql_db = self.config['mysql']['dbname']
            self.mysql_charset = self.config['mysql']['charset']
            self.mysql_table = self.config['mysql']['book_table']
        except Exception:
            # 读取文件异常
            self._logger.info("读取文件异常")
        #建立数据库连接
        self.conn = pymysql.connect(host=self.mysql_hosts,
                                    port=int(self.mysql_port),
                                    user=self.mysql_user,
                                    password=self.mysql_password,
                                    db=self.mysql_db,
                                    charset=self.mysql_charset,
                                    cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.conn.cursor()



    # 将数据推送至ES
    def parse_item(self):
        self._logger.info("查询待推送的数据："+self.mysql_table)
        self.cursor.execute("""SELECT * FROM %s limit 140000,120000""" % self.mysql_table)

        result = self.cursor.fetchall()

        for row in result:
            # 解析数据库结果
            item = ParseBook.initItem(self,row)

            item['collectiontime'] = item['collectiontime'].strftime("%Y-%m-%d %H:%M:%S")
            self.es_pipe.process_item(item)

    # 释放资源
    def close_spider(self, spider):
        self.conn.close()
        self.cursor.close()

    # 解析mysql数据库
    def initItem(self,row):
        item = {}

        item['bookname'] = row['bookname']
        item['subhead'] = row['subhead']
        item['publisher'] = row['publisher']
        item['orgpublisher'] = row['orgpublisher']
        item['contentsummary'] = row['contentsummary']
        item['sourcetype'] = row['sourcetype']
        item['author'] = row['author']
        item['translator'] = row['translator']
        item['isbn'] = row['isbn']
        item['orgisbn'] = row['orgisbn']
        item['salecategory'] = row['salecategory']
        item['category'] = row['category']
        item['orgcategory'] = row['orgcategory']
        item['contenttype'] = row['contenttype']
        item['issuearea'] = row['issuearea']
        item['type'] = row['type']
        item['edition'] = row['edition']
        item['impression'] = row['impression']
        item['words'] = row['words']
        item['pages'] = row['pages']
        item['language'] = row['language']
        item['price'] = row['price']
        item['printedtime'] = row['printedtime']
        item['format'] = row['format']
        item['papermeter'] = row['papermeter']
        item['packing'] = row['packing']
        item['coverurl'] = row['coverurl']
        item['coverpath'] = row['coverpath']
        item['seriename'] = row['seriename']
        item['catalog'] = row['catalog']
        item['editorsugest'] = row['editorsugest']
        item['usersugest'] = row['usersugest']
        item['preface'] = row['preface']
        item['summary'] = row['summary']
        item['epilogue'] = row['epilogue']
        item['publishdate'] = row['publishdate']
        item['collectiontime'] = row['collectiontime']
        item['orgcode'] = row['orgcode']
        item['skuid'] = row['skuid']
        item['commentcount'] = row['commentcount']
        item['_row'] = row['_row']
        item['ifimport'] = '0'
        item['_entitycode'] = row['_entitycode']
        item['url'] = row['url']
        item['commentpercent'] = row['commentpercent']
        item['commenttag'] = row['commenttag']
        item['authorintro'] = row['authorintro']
        item['sourceprice'] = row['sourceprice']
        if not item['printedtime']:
            item['printedtime'] = None
        if not item['publishdate']:
            item['publishdate'] = None
        return item

if __name__ == "__main__":
    aa = ParseBook()

    aa.parse_item()