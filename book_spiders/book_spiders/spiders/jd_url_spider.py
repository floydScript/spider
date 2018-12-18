import json
import re
import time

import requests
import scrapy
from redis import StrictRedis

from book_spiders import settings
from book_spiders.items import BookItem
from book_spiders.items import CommentItem
from book_spiders.conf import Conf
from scrapy_splash import SplashRequest
from scrapy_redis.spiders import RedisSpider
from lxml import etree
import datetime
import binascii
import pymysql
import hashlib

from book_spiders.mylogger import Logger

citem_list = ['isbn','uri','bookname','sourcetype','collectiontime','publishtime','username','hitcount','follownum','suportnum','opposnum','commentid','followcommentid','commenttitle','commenttype','comment','score','level','commpoint','type','sitename','_row','_entitycode']
item_list = ['preface', 'catalog', 'translator', 'isbn', 'subhead', 'edition', 'language', 'orgcategory', 'type',
                 'packing', 'seriename', 'coverurl', 'coverpath', 'pages', 'epilogue', 'price', 'publishdate',
                 'sourcetype', 'editorsugest', 'papermeter', 'printedtime', 'summary', 'orgisbn', 'author',
                 'usersugest',
                 'orgpublisher', 'words', 'format', 'issuearea', 'contenttype', 'contentsummary',
                 'salecategory', 'publisher', 'impression', 'bookname', 'category', 'collectiontime', 'orgcode',
                 'skuid',
                 'commentcount', 'ifimport', '_row', '_entitycode', 'url','commentpercent','commenttag','authorintro','sourceprice']

class JD_Url_Spider(scrapy.Spider):
    name = 'jd_url_spider'

    allowed_domains = ['jd.com']

    start_urls = ["https://search.jd.com/search?keyword=%E5%9B%BE%E4%B9%A6&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E5%9B%BE%E4%B9%A6&ev=exbrand_%E8%AF%BB%E5%AE%A2%5E&uc=0#J_searchWrap"]
    # start_urls = []

    _logger = Logger().getLogger()


    def __init__(self):
        # 加载redis
        config = Conf.config
        self.redis = StrictRedis(host=config['redis']['host'], port=config['redis']['port'],
                                 password=config['redis']['password'], db=config['redis']['db'])
        #获得所有分类url，并添加到 start_urls 中
        # self.recursion_cate('https://search.jd.com/Search?keyword=%E5%9B%BE%E4%B9%A6&enc=utf-8&wq=%E5%9B%BE%E4%B9%A6&pvid=29e5fd017a7b48fd991885dc0e82c06d')
        resp = requests.get('https://search.jd.com/Search?keyword=%E5%9B%BE%E4%B9%A6&enc=utf-8&wq=%E5%9B%BE%E4%B9%A6&pvid=29e5fd017a7b48fd991885dc0e82c06d')
        html = etree.HTML(resp.text)
        cate_urls = html.xpath("//div[@id='J_selector']//li/a/@href")
        for url in cate_urls:
            start_url = 'https://search.jd.com/'+url
            self.start_urls.append(start_url)
        self._logger.info("start_urls的长度:"+str(len(self.start_urls)))

    def parse(self, response):
        # 遍历本页的商品
        good_list = response.xpath("//div[@id='J_goodsList']//div[@class='p-img']/a/@href").extract()
        if good_list:
            for good in good_list:
                if not good.startswith("https"):
                    good = 'https:'+good
                self.redis.lpush('toplist_jd:start_urls', good)
                self._logger.info("添加url："+good)
            # 请求下一页
            response_url = response.url.replace('&uc=0','').replace("#J_searchWrap",'')
            params = re.findall("&page=(\d+)&s=(\d+)&click=0",response_url)
            if not params:
                next_page = response_url+"&page=3&s=61&click=0"
            else:
                params = params[0]
                new_params = [str(int(params[0])+2),str(int(params[1])+60)]
                next_page = response_url.replace("&page="+params[0]+"&s="+params[1]+"&click=0","&page="+new_params[0]+"&s="+new_params[1]+"&click=0")
            yield scrapy.Request(url=next_page,callback=self.parse)
        else:
            self._logger.info('================翻页到头啦================')
            pass

    def recursion_cate(self,url):
        try:
            resp = requests.get(url)
            html = etree.HTML(resp.text)
            cate_urls = html.xpath("//div[@id='J_selector']//li/a/@href")
            if cate_urls:
                for link in cate_urls:
                    start_url = 'https://search.jd.com/' + link
                    self.recursion_cate(start_url)
            else:
                self.start_urls.append(url)
        except:
            pass
