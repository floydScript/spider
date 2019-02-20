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

class Toplist_jd(RedisSpider):
    name = 'toplist_jd'

    allowed_domains = ['jd.com']

    # start_urls = ['https://item.jd.com/12111106.html']

    redis_key = 'toplist_jd:start_urls'

    _logger = Logger().getLogger()

    def parse(self, response):
        item = BookItem()
        for item_key in item_list:
            item[item_key] = ''
        item['is_set'] = '否'
        is_set = '否'
        # 判断isbn是否满足要求
        isbn = self.get_basicinfo(response,'ISBN')
        if len(isbn) != 13:
            isbn = ''
            is_set = '是'
        if is_set == '否':
            skuid = response.url.split('/')[-1].replace('.html','')
            # 加载商品描述信息接口
            html = self.get_content_and_cate(skuid)
            # 加载商品价格接口
            sourceprice,price = self.get_price(skuid)
            # 加载商品评论、评论数、好评率接口
            comments ,commentcount ,commentpercent ,commenttag  = self.get_comment(skuid)
            bookname = response.xpath("//div[@class='sku-name']/text()").extract_first()
            bookname = bookname.strip()
            item['bookname'] = bookname
            item['subhead'] = ''
            item['publisher'] = self.get_basicinfo(response,'出版社')
            item['orgpublisher'] = self.get_basicinfo(response,'出版社')
            contentsummary = self.parse_desc(html,'内容简介')
            contentsummary = ''.join(contentsummary)
            item['contentsummary'] = contentsummary
            item['sourcetype'] = '01'
            author_list = response.xpath("//div[@class='p-author']/a/@data-name").extract()
            author = '#'.join(author_list)
            item['author'] = author
            item['translator'] = ''
            item['isbn'] = isbn
            item['orgisbn'] = isbn
            item['salecategory'] = ''
            item['category'] = ''
            item['orgcategory'] = ''
            brand = self.get_basicinfo(response,'品牌')
            contenttype_list = response.xpath("//div[@class='crumb fl clearfix']/div[@class='item']/a/text()").extract()
            try:
                contenttype_list.remove(brand)
            except:
                pass
            contenttype = ','.join(contenttype_list)
            item['contenttype'] = contenttype
            item['issuearea'] = ''
            item['type'] = '01'
            item['edition'] = self.get_basicinfo(response,'版次')
            item['impression'] = ''
            item['words'] = self.get_basicinfo(response,'字数')
            pages = re.findall('\d+', self.get_basicinfo(response,'页数'))
            if not pages:
                page = ['']
            pages = pages[0]
            item['pages'] = pages

            item['language'] = self.get_basicinfo(response,'正文语种')
            item['price'] = price

            item['format'] = self.get_basicinfo(response,'开本')
            item['papermeter'] = self.get_basicinfo(response,'用纸')
            item['packing'] = self.get_basicinfo(response,'包装')
            item['coverurl'] = 'http:'+response.xpath("//div[@id= 'spec-n1']/img/@src").extract_first()
            item['seriename'] = self.get_basicinfo(response,'丛书名')
            item['catalog'] = self.parse_desc(html,'目录')
            item['editorsugest'] = self.parse_desc(html,'编辑推荐')
            item['usersugest'] = self.parse_desc(html,'精彩书评')
            item['preface'] = self.parse_desc(html,'前言/序言')
            item['summary'] = self.parse_desc(html,'精彩书摘')
            item['epilogue'] = ''
            publishdate = self.get_basicinfo(response,'出版时间')
            if not publishdate:
                publishdate = ''
            if len(publishdate) > 7:
                index = publishdate.rfind('-')
                publishdate = publishdate[:index]

            item['publishdate'] = publishdate
            item['printedtime'] = publishdate
            item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['orgcode'] = ''
            item['skuid'] = skuid
            item['commentcount'] = str(commentcount)
            item['_row'] = skuid + '01'
            item['coverpath'] = '/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/' + item['_row'] + '.jpg'
            item['is_set'] = '否'
            item['ifimport'] = '0'
            item['url'] = response.url
            item['_entitycode'] = 'web_page_p_book_info_09'
            item['commentpercent'] = commentpercent
            item['commenttag'] = commenttag
            item['authorintro'] = self.parse_desc(html,'作者简介')
            item['sourceprice'] = sourceprice

            #遍历评论列表
            if comments:
                for comment in comments:
                    comment_item = CommentItem()
                    comment_item['isbn'] = isbn
                    comment_item['uri'] = response.url
                    comment_item['bookname'] = bookname
                    comment_item['sourcetype'] = '01'
                    comment_item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    comment_item['publishtime'] = comment['creationTime']
                    comment_item['username'] = '无昵称用户'
                    comment_item['hitcount'] = '0'
                    follownum = str(comment['replyCount'])
                    if not follownum:
                        follownum = '0'
                    comment_item['follownum'] = follownum
                    suportnum = str(comment['usefulVoteCount'])
                    if not suportnum:
                        suportnum = '0'
                    comment_item['suportnum'] = suportnum
                    comment_item['opposnum'] = '0'
                    comment_item['commentid'] = str(comment['id'])
                    comment_item['followcommentid'] = ''
                    comment_item['commenttitle'] = ''
                    comment_item['commenttype'] = '0'
                    comment_item['comment'] = comment['content']
                    score = str(comment['score'])
                    if not score:
                        score = '5'
                    comment_item['score'] =score
                    score = int(score)
                    if score < 2:
                        level = '2'
                    elif score < 4:
                        level = '1'
                    else:
                        level = '0'
                    comment_item['level'] = level
                    comment_item['commpoint'] = ''
                    comment_item['type'] = '01'
                    comment_item['sitename'] = '京东'
                    comment_item['_row'] = comment_item['isbn'] + comment_item['sourcetype'] + comment_item['publishtime'] + comment_item['username']
                    comment_item['_entitycode'] = 'web_page_p_book_comment_09'
                    comment_item['skuid'] = skuid
                    yield comment_item
            yield item





    def get_content_and_cate(self, jid):
        """
        获取图书的简介和目录信息
        :param jid: 图书的商品id
        :return: 返回图书的简介和目录信息
        """
        url = 'http://dx.3.cn/desc/' + jid
        data = requests.get(url)
        data = json.loads(data.text[9:-1])
        data = data['content']
        html = etree.HTML(data)
        return html

    def parse_desc(self,html,tag):
        desc = html.xpath("//div[@text='%s']//p/text()" %tag)
        desc = '<br>'.join(desc)
        return desc

    def get_price(self, jid):
        """
        获取图书的价格和折扣
        :param jid: 图书的商品id
        :return: 图书的价格和折扣
        """
        headers = {
            'Referer': 'http://book.jd.com/booktop/0-0-0.html?category=1713-0-0-0-10001-1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
        }
        url = 'http://p.3.cn/prices/mgets?skuIds=%s' % (jid)
        data = requests.get(url,headers=headers).json()
        sale_price = data[0]['p']
        org_price = data[0]['m']
        return sale_price, org_price

    def get_comment(self,jid):
        url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv31700&productId=%s&score=0&sortType=5&page=0&pageSize=10&isShadowSku=0&fold=1' % jid
        json_str = requests.get(url).text
        prefix = json_str.split('{')[0]
        json_str = json_str.replace(prefix, '')[:-2]
        data = json.loads(json_str)
        comments = data['comments']
        commentcount = data['productCommentSummary']['commentCount']
        commentpercent = float(data['productCommentSummary']['goodRate'])*100
        commenttags = data['hotCommentTagStatistics']
        commenttag = ''
        if len(commenttags)>0:
            tag_list = []
            for tag in commenttags:
                tag_str = tag['name']+'('+str(tag['count'])+')'
                tag_list.append(tag_str)
            commenttag = '#'.join(tag_list)
        return comments,commentcount,commentpercent,commenttag

    def get_basicinfo(self,response,tag):
        li_selector_list = response.xpath("//ul[@id='parameter2']/li")
        li_str_list = response.xpath("//ul[@id='parameter2']/li").extract()
        result = ''
        for index,li in enumerate(li_str_list):
            flag = li.find(tag)
            if not flag == -1:
                result = li_selector_list[index].xpath("./@title").extract_first()
                if not result:
                    result = ''
                break
        return result






        