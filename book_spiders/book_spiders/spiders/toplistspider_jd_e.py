import json
import re


import requests
import scrapy

from book_spiders.items import BookItem
from book_spiders.items import CommentItem

from scrapy_redis.spiders import RedisSpider
from lxml import etree
import datetime

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

class Toplist_jd_e(RedisSpider):
    name = 'toplist_jd_e'

    allowed_domains = ['jd.com']

    # start_urls = ['https://e.jd.com/30188297.html']

    redis_key = 'toplist_jd_e:start_urls'

    _logger = Logger().getLogger()

    def parse(self, response):
        item = BookItem()
        for item_key in item_list:
            item[item_key] = ''
        item['is_set'] = '否'
        is_set = '否'
        # 判断isbn是否满足要求
        isbn = self.get_basicinfo(response,'I S B N')
        if len(isbn) != 13:
            isbn = ''
            is_set = '是'
        if is_set == '否':
            skuid = response.url.split('/')[-1].replace('.html','')
            # 加载商品描述信息接口
            data = self.get_content_and_cate(skuid)
            # 加载商品价格接口
            sourceprice,price = self.get_price(skuid)
            # 加载商品评论、评论数、好评率接口
            comments ,commentcount ,commentpercent ,commenttag  = self.get_comment(skuid)
            bookname = response.xpath("//div[@class='sku-name']/text()").extract_first()
            bookname = bookname.strip()
            item['bookname'] = bookname
            item['subhead'] = ''
            item['publisher'] = response.xpath("//div[@class='publishing li']/div[@class='dd']/a/text()").extract_first()
            item['orgpublisher'] = item['publisher']
            item['contentsummary'] = data['contentInfo']
            item['sourcetype'] = '01'
            author_list = response.xpath("//div[@class='author']//a/text()").extract()
            author = '#'.join(author_list)
            item['author'] = author
            item['translator'] = ''
            item['isbn'] = isbn
            item['orgisbn'] = isbn
            item['salecategory'] = ''
            item['category'] = ''
            item['orgcategory'] = ''
            contenttype_list = response.xpath("//div[@class='category li']//div[@class='dd']/a/text()").extract()
            contenttype = ','.join(contenttype_list)
            item['contenttype'] = contenttype
            item['issuearea'] = ''
            item['type'] = '02'
            item['edition'] = self.get_basicinfo(response,'版　　次')
            item['impression'] = ''
            item['words'] = self.get_basicinfo(response,'字　　数')
            item['pages'] = ''

            item['language'] = self.get_basicinfo(response,'正文语种')
            item['price'] = price

            item['format'] = ''
            item['papermeter'] = ''
            item['packing'] = ''
            item['coverurl'] = 'http:'+response.xpath("//img[@id='spec-img']/@data-origin").extract_first()
            item['seriename'] = ''
            item['catalog'] = data['catalog']
            item['editorsugest'] = data['editorPick']
            item['usersugest'] = ''
            item['preface'] = data['preface']
            item['summary'] = ''
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
            item['_row'] = skuid + item['sourcetype']
            item['coverpath'] = '/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/' + item['_row'] + '.jpg'
            item['is_set'] = '否'
            item['ifimport'] = '0'
            item['url'] = response.url
            item['_entitycode'] = 'web_page_p_book_info_09'
            item['commentpercent'] = commentpercent
            item['commenttag'] = commenttag
            item['authorintro'] = data['authorInfo']
            item['sourceprice'] = sourceprice

            #遍历评论列表
            if comments:
                for comment in comments:
                    comment_item = CommentItem()
                    comment_item['isbn'] = isbn
                    comment_item['uri'] = response.url
                    comment_item['bookname'] = bookname
                    comment_item['sourcetype'] = item['sourcetype']
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
                    comment_item['commenttype'] = '1'
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
        return data

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
        url = 'https://p.3.cn/prices/mgets?skuids=J_%s&pduid=14991642880271760950521' % (jid)
        data = requests.get(url).json()
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
        tag_list = response.xpath("//div[@class='li']/div/div/div[@class='dt']/text()").extract()
        value_list = response.xpath("//div[@class='li']/div/div/div[@class='dd']/text()").extract()
        result = ''
        for index,tag_str in enumerate(tag_list):
            flag = tag_str.find(tag)
            if not flag == -1:
                result = value_list[index]
                break
        return result






        