import datetime
import json
import os

import pymysql
import requests
from kafka import KafkaConsumer
from categoryServer import CategoryServer
from conf import Conf
from mylogger import Logger
from orgcoderServer import OrgcoderServer

config = Conf.config
consumer = KafkaConsumer('09_p_spider',bootstrap_servers=[config['kafka']['host']])
# consumer = KafkaConsumer('test',bootstrap_servers=[hosts])
_logger = Logger().getLogger()



class MySqlPipelines(object):
    _logger = Logger().getLogger()

    def __init__(self):
        config = Conf.config
        self.db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'],
                                  user=config['mysql']['username'],
                                  passwd=config['mysql']['password'], db=config['mysql']['dbname'], charset='utf8')
        self.cursor = self.db.cursor()

    def process_item(self, item):

        if item['_entitycode'] == 'web_page_p_book_info_09':
            if item['is_set'] == '是':
                return item
            # 实例化分类服务器
            cate_server = CategoryServer()
            orgcode_server = OrgcoderServer()
            # 营销分类查询
            contenttype = item['contenttype'].split(',')
            if not contenttype:
                contenttype = ['']
            contenttype = contenttype[-1]
            salecategory = cate_server.query_sale_category(contenttype)
            item['salecategory'] = salecategory
            # 中图分类
            isbn = item['isbn']
            cate_code = cate_server.query_cate_server(isbn)
            item['category'] = cate_code
            item['orgcategory'] = cate_code

            # 根据出版社名字查询orgcode
            pub_name = item['publisher']
            orgcode = orgcode_server.query_orgcode(pub_name)
            item['orgcode'] = orgcode

            sql = '''insert into web_page_p_book_info_09_dangdang(bookname, subhead, publisher, orgpublisher, contentsummary, sourcetype,
                    author, translator, isbn, orgisbn, salecategory, category, orgcategory, contenttype, issuearea, type, edition, impression,
                    words, pages, language, price, printedtime, format, papermeter, packing, coverurl, coverpath, seriename, catalog, 
                    editorsugest, usersugest, preface, summary, epilogue, publishdate, collectiontime, orgcode, skuid, commentcount, _row,
                    ifimport, _entitycode, url, commentpercent, commenttag,authorintro,sourceprice)
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            parames = (
                item['bookname'], item['subhead'], item['publisher'], item['orgpublisher'], item['contentsummary'],
                item['sourcetype'],
                item['author'], item['translator'], item['isbn'], item['orgisbn'], item['salecategory'],
                item['category'], item['orgcategory'],
                item['contenttype'], item['issuearea'], item['type'], item['edition'], item['impression'],
                item['words'], item['pages'], item['language'],
                item['price'], item['printedtime'], item['format'], item['papermeter'], item['packing'],
                item['coverurl'], item['coverpath'],
                item['seriename'], item['catalog'], item['editorsugest'], item['usersugest'], item['preface'],
                item['summary'], item['epilogue'],
                item['publishdate'], item['collectiontime'], item['orgcode'], item['skuid'], item['commentcount'],
                item['_row'], item['ifimport'],
                item['_entitycode'], item['url'], item['commentpercent'], item['commenttag'], item['authorintro'],
                item['sourceprice']
            )
            try:
                self.cursor.execute(sql, parames)
                self.db.commit()
                self._logger.info('插入===book_info===数据：' + item['url'])
            except Exception:
                # 更改is_set为'是'：即若是存在相同的数据就不去下载图片了
                item['is_set'] = '是'
                self._logger.info('存在相同数据，插入===book_info===失败：' + item['url'])
        elif item['_entitycode'] == 'web_page_p_book_comment_09':
            sql = '''insert into web_page_p_book_comment_09_dangdang(_row, isbn, uri, bookname, sourcetype, collectiontime, publishtime, username, hitcount,
                    follownum, suportnum, opposnum, commentid, followcommentid, commenttitle, commenttype, comment, score, level, commpoint, type, sitename,
                    ifimport, _entitycode)
                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            parames = (
                item['_row'], item['isbn'], item['uri'], item['bookname'], item['sourcetype'], item['collectiontime'],
                item['publishtime'], item['username'],
                item['hitcount'], item['follownum'], item['suportnum'], item['opposnum'], item['commentid'],
                item['followcommentid'], item['commenttitle'],
                item['commenttype'], item['comment'], item['score'], item['level'], item['commpoint'], item['type'],
                item['sitename'], '0', item['_entitycode']
            )
            try:
                self.cursor.execute(sql, parames)
                self.db.commit()
                self._logger.info('插入~~~book_comment~~~数据：' + item['url'])
            except Exception:
                self._logger.info('存在相同数据，插入~~~book_comment~~~失败：' + item['url'])
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()


class SaveImgPipelines():
    _logger = Logger().getLogger()

    def __init__(self):
        self.config = Conf.config

    def process_item(self, item):

        if item['_entitycode'] == 'web_page_p_book_info_09':
            if item['is_set'] == '是':
                return item
            # 拼接图片路径 /opt/fhcb/fileserver/img + /book/20180909/2993702.jpg
            img_path = self.config['image']['path'] + item['coverpath']
            # 创建文件夹  /opt/fhcb/fileserver/img + /book/ + 20180909/
            dir_path = self.config['image']['path'] + '/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/'
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            # 从imgurl下载图片
            with open(img_path, 'wb') as f:
                resp = requests.get(item['coverurl'])
                f.write(resp.content)
                self._logger.info('下载图片到： ' + item['coverpath'])
        return item

if __name__ == '__main__':
    mysql_pipe = MySqlPipelines()
    image_pipe = SaveImgPipelines()
    for msg in consumer:
        try:
            item_str = msg.value.decode('utf-8')
            item = json.loads(item_str)
            _logger.info('接收信息：'+item['isbn'])
            mysql_pipe.process_item(item)
            image_pipe.process_item(item)
        except Exception as e:
            _logger.error(e)


