import os

import pymysql
import requests
import datetime

from elasticsearch import Elasticsearch

from categoryServer import CategoryServer
from conf import Conf
from mylogger import Logger
from orgcoderServer import OrgcoderServer

# 存储爬虫爬取到的原始数据到mysql
class MySqlPipelines(object):

    _logger = Logger().getLogger()

    def __init__(self):
        config = Conf.config
        # 创建数据库连接
        self.db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'],
                                  user=config['mysql']['username'],
                                  passwd=config['mysql']['password'], db=config['mysql']['dbname'], charset='utf8')
        self.cursor = self.db.cursor()
        # 实例化分类服务器
        self.cate_server = CategoryServer()


    def process_item(self, item):
        """
        查询营销分类，中图分类，出版社编号。将数据推送至mysql
        :param item:图书信息实体
        :return:item
        """
        if item['_entitycode'] == 'web_page_p_book_info_09':
            if item['is_set'] == '是':
                return item

            # 营销分类查询
            contenttype = item['contenttype'].split(',')
            if not contenttype:
                contenttype = ['']
            contenttype = contenttype[-1]
            salecategory = self.cate_server.query_sale_category(contenttype)
            item['salecategory'] = salecategory
            # 中图分类
            isbn = item['isbn']
            cate_code = self.cate_server.query_cate_server(isbn)
            item['category'] = cate_code
            item['orgcategory'] = cate_code
            if item['sourcetype'] == '01':
                table = 'web_page_p_book_info_09_jingdong'
            elif item['sourcetype'] == '02':
                table = 'web_page_p_book_info_09_dangdang'
            elif item['sourcetype'] == '03':
                table = 'web_page_p_book_info_09_douban'
            elif item['sourcetype'] == '04':
                table = 'web_page_p_book_info_09_xinhuashudian'
            elif item['sourcetype'] == '05':
                table = 'web_page_p_book_info_09_yamaxun'
            elif item['sourcetype'] == '06':
                table = 'web_page_p_book_info_09_tianmao'
            sql = 'insert into '+table+'(bookname, subhead, publisher, orgpublisher, contentsummary, sourcetype,author, translator, isbn, orgisbn, salecategory, category, orgcategory, contenttype, issuearea, type, edition, impression,words, pages, language, price, printedtime, format, papermeter, packing, coverurl, coverpath, seriename, catalog, editorsugest, usersugest, preface, summary, epilogue, publishdate, collectiontime, orgcode, skuid, commentcount, _row,ifimport, _entitycode, url, commentpercent, commenttag,authorintro,sourceprice) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
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
                self._logger.info('mysql插入===book_info===数据成功：' + item['url'])
            except Exception:
                item['is_set'] = '是'
                self._logger.info('mysql插入===book_info===失败：' + item['url'])
        elif item['_entitycode'] == 'web_page_p_book_comment_09':
            if item['sourcetype'] == '01':
                table = 'web_page_p_book_comment_09_jingdong'
            elif item['sourcetype'] == '02':
                table = 'web_page_p_book_comment_09_dangdang'
            elif item['sourcetype'] == '03':
                table = 'web_page_p_book_comment_09_douban'
            elif item['sourcetype'] == '04':
                table = 'web_page_p_book_comment_09_xinhuashudian'
            elif item['sourcetype'] == '05':
                table = 'web_page_p_book_comment_09_yamaxun'
            elif item['sourcetype'] == '06':
                table = 'web_page_p_book_comment_09_tianmao'

            sql = 'insert into '+table+'(_row, isbn, uri, bookname, sourcetype, collectiontime, publishtime, username, hitcount,follownum, suportnum, opposnum, commentid, followcommentid, commenttitle, commenttype, comment, score, level, commpoint, type, sitename,ifimport, _entitycode) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
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
                self._logger.info('mysqk插入~~~book_comment~~~数据成功：' + item['_row'])
            except Exception as e:
                self._logger.error(e)
                self._logger.info('mysqk插入~~~book_comment~~~失败：' + item['_row'])
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()

# 下载封面图片
class SaveImgPipelines():
    _logger = Logger().getLogger()

    def __init__(self):
        self.config = Conf.config
        self.es = Elasticsearch(self.config['elasticsearch']['hosts'])

    def process_item(self, item):
        """
        保存图片
        :param item:
        :return:
        """
        if item['_entitycode'] == 'web_page_p_book_info_09':
            if item['is_set'] == '是':
                return item
            # 查询es，如果es中有这个isbn的话就不存图片了
            body = {
                "query": {
                    "term": {
                        "isbn": item['isbn']
                    }
                }
            }
            result = self.es.search(index="web_page_p_book_info_09", doc_type="web_page_p_book_info_09", body=body)
            if result['hits']['hits']:
                self._logger.info('重复的图片，不再下载  ISBN:'+item['isbn'])
                return item
            # 拼接图片路径 /opt/fhcb/fileserver/img + /book/20180909/2993702.jpg
            img_path = self.config['image']['path'] + item['coverpath']
            # 创建文件夹  /opt/fhcb/fileserver/img + /book/ + 20180909/
            filename = img_path.split('/')[-1]
            dir_path = img_path.replace(filename,'')
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            # 从imgurl下载图片
            with open(img_path, 'wb') as f:
                resp = requests.get(item['coverurl'])
                f.write(resp.content)
                self._logger.info('下载图片到： ' + item['coverpath'])
        return item

# 存储清洗过后的数据到elasticsearch
class ElasticSearchPipelines():
    _logger = Logger().getLogger()

    info_mapping = {
    "mappings": {
        "web_page_p_book_info_09": {
            "properties": {
                "_entitycode": {
                "type": "string"
                },
                "_row": {
                "type": "string"
                },
                "author": {
                "type": "string",
                "index": "not_analyzed"
                },
                "authorintro": {
                "type": "string"
                },
                "bookname": {
                "type": "string"
                },
                "catalog": {
                "type": "string"
                },
                "category": {
                "type": "string"
                },
                "collectiontime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM"
                },
                "commentcount": {
                "type": "long"
                },
                "commentcount_jd": {
                "type": "long"
                },
                "commentcount_ymx": {
                "type": "long"
                },
                "commentpercent": {
                "type": "float"
                },
                "commenttag": {
                "type": "string"
                },
                "contentsummary": {
                "type": "string"
                },
                "contenttype": {
                "type": "string"
                },
                "coverpath": {
                "type": "string"
                },
                "coverurl": {
                "type": "string"
                },
                "edition": {
                "type": "string"
                },
                "editorsugest": {
                "type": "string"
                },
                "epilogue": {
                "type": "string"
                },
                "format": {
                "type": "string"
                },
                "ifimport": {
                "type": "string"
                },
                "impression": {
                "type": "string"
                },
                "isbn": {
                "type": "string"
                },
                "issuearea": {
                "type": "string"
                },
                "language": {
                "type": "string"
                },
                "orgcategory": {
                "type": "string"
                },
                "orgcode": {
                "type": "string"
                },
                "orgisbn": {
                "type": "string"
                },
                "orgpublisher": {
                "type": "string",
                "index": "not_analyzed"
                },
                "packing": {
                "type": "string"
                },
                "pages": {
                "type": "integer"
                },
                "papermeter": {
                "type": "string"
                },
                "preface": {
                "type": "string"
                },
                "price": {
                "type": "float"
                },
                "printedtime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM"
                },
                "publishdate": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM"
                },
                "publisher": {
                "type": "string",
                "index": "not_analyzed"
                },
                "row": {
                "type": "string"
                },
                "salecategory": {
                "type": "string"
                },
                "seriename": {
                "type": "string"
                },
                "skuid": {
                "type": "string"
                },
                "sourceprice": {
                "type": "float"
                },
                "sourceprice_jd": {
                "type": "float"
                },
                "sourceprice_ymx": {
                "type": "float"
                },
                "sourcetype": {
                "type": "string"
                },
                "subhead": {
                "type": "string"
                },
                "summary": {
                "type": "string"
                },
                "translator": {
                "type": "string"
                },
                "type": {
                "type": "string"
                },
                "url": {
                "type": "string"
                },
                "usersugest": {
                "type": "string"
                },
                "words": {
                "type": "integer"
                }
            }
        }
    }
}
    comment_mapping = {
    "mappings": {
        "web_page_p_book_comment_09": {
            "properties": {
                "_entitycode": {
                    "type": "string"
                },
                "_row": {
                    "type": "string"
                },
                "bookname": {
                    "type": "string"
                },
                "collectiontime": {
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd",
                    "type": "date"
                },
                "comment": {
                    "type": "string"
                },
                "commentid": {
                    "type": "string"
                },
                "commenttitle": {
                    "type": "string"
                },
                "commenttype": {
                    "type": "string"
                },
                "commpoint": {
                    "type": "string"
                },
                "followcommentid": {
                    "type": "string"
                },
                "follownum": {
                    "type": "integer"
                },
                "hitcount": {
                    "type": "integer"
                },
                "isbn": {
                    "type": "string"
                },
                "level": {
                    "type": "integer"
                },
                "opposnum": {
                    "type": "integer"
                },
                "publishtime": {
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd",
                    "type": "date"
                },
                "score": {
                    "type": "float"
                },
                "sitename": {
                    "type": "string"
                },
                "skuid": {
                    "type": "string"
                },
                "sourcetype": {
                    "type": "string"
                },
                "suportnum": {
                    "type": "integer"
                },
                "type": {
                    "type": "string"
                },
                "uri": {
                    "type": "string"
                },
                "username": {
                    "type": "string"
                }
            }
        }
    }
}

    def __init__(self):

        self.config = Conf.config
        self.orgcode_server = OrgcoderServer()
        # 建立es连接
        self.es = Elasticsearch(self.config['elasticsearch']['hosts'])
        # 图书信息索引，如果不存在则创建
        if self.es.indices.exists(index='web_page_p_book_info_09') is not True:
            self.es.indices.create(index='web_page_p_book_info_09', body=self.info_mapping)
        # 图书评论索引，如果不存在则创建
        if self.es.indices.exists(index='web_page_p_book_comment_09') is not True:
            self.es.indices.create(index='web_page_p_book_comment_09', body=self.comment_mapping)
        self.orgcode_server = OrgcoderServer()

    def process_item(self,item):
        """
        清洗数据，推送至elasticsearch
        :param item:
        :return:
        """
        if item['_entitycode'] == 'web_page_p_book_info_09':
            # 清洗数据,根据来源分类清洗
            flag = self.dd_washing_datas(item,self.orgcode_server)
            if not flag:
                return None

            id = item['isbn']
            #=================== 区分数据来源 ===================#
            tag = ''
            if item['sourcetype'] == '01':#京东
                tag = '_jd'
                sourceprice = item.pop('sourceprice')
                item['sourceprice_jd'] = sourceprice
                commentcount = item.pop('commentcount')
                item['commentcount_jd'] = commentcount

            elif item['sourcetype'] == '02':#当当
                tag = ''
                # 因为当当的字段就是原字段，无需更改

            elif item['sourcetype'] == '03':#豆瓣
                tag = '_db'
                sourceprice = item.pop('sourceprice')
                item['sourceprice_db'] = sourceprice
                commentcount = item.pop('commentcount')
                item['commentcount_db'] = commentcount

            elif item['sourcetype'] == '04':#新华书店
                tag = '_xhsd'
                sourceprice = item.pop('sourceprice')
                item['sourceprice_xhsd'] = sourceprice
                commentcount = item.pop('commentcount')
                item['commentcount_xhsd'] = commentcount

            elif item['sourcetype'] == '05':#亚马逊
                tag = '_ymx'
                sourceprice = item.pop('sourceprice')
                item['sourceprice_ymx'] = sourceprice
                commentcount = item.pop('commentcount')
                item['commentcount_ymx'] = commentcount

            elif item['sourcetype'] == '06':#天猫
                tag = '_tm'
                sourceprice = item.pop('sourceprice')
                item['sourceprice_tm'] = sourceprice
                commentcount = item.pop('commentcount')
                item['commentcount_tm'] = commentcount
            # =================== 区分数据来源 ===================#

            # 索引中无则新增，有则更新，还要将数据进行合并
            try:
                # 取到相同id的数据，如果取不到会报错，进入except
                resu = self.es.get(index=item['_entitycode'], doc_type=item['_entitycode'], id=id)
                resu_item = resu['_source']
                if tag:
                    resu_item['sourcetype'+tag] = ''
                    resu_item['commentcount'+tag] = ''
                # 取代原来为空的字段
                for key in resu_item:
                    if not resu_item[key]:
                        try:
                            resu_item[key] = item[key]
                        except:
                            pass
                # 取最大的评论数存储
                if int(resu_item['commentcount']) < int(item['commentcount']):
                    resu_item['commentcount'] = item['commentcount']
                self.es.index(index=item['_entitycode'], doc_type=item['_entitycode'], id=id, body=resu_item)
                self._logger.info('es更新数据成功:' + resu_item['url'])
            except:
                # 新增数据
                # item['price'] = str(item['price'], encoding="utf-8")

                # 判断图片是否存在
                path = self.config['image']['path'] + item['coverpath']
                if not os.path.exists(path):
                    self._logger.info('图片不存在：' + item['coverpath'])
                    return None
                # 往es中写入数据
                self.es.index(index=item['_entitycode'], doc_type=item['_entitycode'], id=id, body=item)
                self._logger.info('es新增数据成功:' + item['url'])
        elif item['_entitycode'] == 'web_page_p_book_comment_09':
            id = item['_row']
            self.es.index(index=item['_entitycode'], doc_type=item['_entitycode'], id=id, body=item)
            self._logger.info('es新增数据成功:'+item['uri'])

    def dd_washing_datas(self, item,orgcode_server):
        """
        数据清洗
        1.作者
        2.书名
        3.过滤没有图片的数据
        4.将评论数为空串的转换为0
        5.根据出版社名字查询orgcode
        6.判断时间不能大于当前时间
        7.时间字段如果是空串，改成None
        8.清洗html标签
        :param item: 图书信息实体
        :param orgcode_server: 出版社编号查询 实例
        :return:
        """
        # 判断图片是否存在
        if not item['coverpath']:
            return None

        # 清洗作者1：‘菲尔·比德尔 | 译者’
        item['author'] = item['author'].split('|')[0]

        # 清洗书名1，去除（和(后面的字段
        item['bookname'] = item['bookname'].split('(')[0]
        item['bookname'] = item['bookname'].split('（')[0]

        # 清洗书名2，去除 出版社，isbn，作者等字样
        item['bookname'] = item['bookname'].replace(item['publisher'], '').replace(item['isbn'], '').replace(item['author'], '')

        # 将评论数为空串的转换为0
        if not item['commentcount']:
            item['commentcount'] = 0

        # 根据出版社名字查询orgcode

        pub_name = item['publisher']
        orgcode = orgcode_server.query_orgcode(pub_name)
        item['orgcode'] = orgcode

        # 出版时间不能大于当前时间
        strftime = datetime.datetime.strptime(item['publishdate'], "%Y-%m")
        strftime2 = datetime.datetime.now()
        if strftime > strftime2:
            return None

        # 时间字段如果是空串，改成None
        if not item['printedtime']:
            item['printedtime'] = None
        if not item['publishdate']:
            item['publishdate'] = None

        # 清洗html标签
        item['preface'] = item['preface'].replace("<br>",'')
        item['catalog'] = item['catalog'].replace("<br>",'')
        item['editorsugest'] = item['editorsugest'].replace("<br>",'')
        item['summary'] = item['summary'].replace("<br>",'')
        item['usersugest'] = item['usersugest'].replace("<br>",'')
        item['contentsummary'] = item['contentsummary'].replace("<br>",'')
        item['authorintro'] = item['authorintro'].replace("<br>",'')
        return True