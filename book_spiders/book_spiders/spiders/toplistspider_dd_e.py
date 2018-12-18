import json
import re
import time

import requests

from book_spiders.items import BookItem
from book_spiders.items import CommentItem

from scrapy_redis.spiders import RedisSpider
import datetime
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

class Toplist_dd_e(RedisSpider):
    name = 'toplist_dd_e'

    allowed_domains = ['dangdang.com']

    # start_urls = ['http://e.dangdang.com/products/1901077157.html']

    redis_key = 'toplist_dd_e:start_urls'

    _logger = Logger().getLogger()

    def parse(self, response):
        item = BookItem()
        for item_key in item_list:
            item[item_key] = ''
        is_set = '否'
        item['is_set'] = is_set
        skuid = response.url.split('/')[-1].replace('.html', '')

        bookname = response.xpath("//span[@class='title_words']/@title").extract_first()
        bookname = bookname.strip()
        item['bookname'] = bookname
        item['subhead'] = response.xpath("//p[@class='title_descript']/@title").extract_first()
        item['publisher'] = response.xpath("//p[@id='publisher']//a/text()").extract_first()
        item['orgpublisher'] = response.xpath("//p[@id='publisher']//a/text()").extract_first()
        contentsummary = response.xpath("//div[@class='newEdit_box']//text()").extract()
        contentsummary = '<br>'.join(contentsummary)
        item['contentsummary'] = contentsummary
        item['sourcetype'] = '02'
        authors = response.xpath("//p[@id='author']//a/text()").extract_first()
        if not authors:
            authors = ''
        authors = authors.replace('、',',')
        author_list = authors.split(',')
        authors = '#'.join(author_list)
        item['author'] = authors
        item['translator'] = ''
        item['isbn'] = ''
        item['orgisbn'] = ''
        item['salecategory'] = ''
        item['category'] = ''
        item['orgcategory'] = ''
        contenttype_list = response.xpath("//div[@id='crumb']/a/text()").extract()
        for index,ct in enumerate(contenttype_list):
            ct = ct.replace('>','')
            ct = ct.strip()
            contenttype_list[index] = ct
            if ct == bookname:
                contenttype_list.pop(index)
        contenttype = ','.join(contenttype_list)
        item['contenttype'] =contenttype
        item['issuearea'] = ''
        item['type'] = '02'
        item['edition'] = ''
        item['impression'] = ''
        basic_info_list = response.xpath("//div[@class='explain_box']/p").extract()
        basic_info_str = ''.join(basic_info_list)
        words = re.findall('数：(\d+[.]*\d+)', basic_info_str)
        suffix = 1
        if '万' in basic_info_str:
            suffix =10000
        if words:
            words = int(float(words[0]) * suffix)
        else:
            words = ''
        item['words'] = str(words)
        # 测试
        item['pages'] = ''
        item['language'] = ''
        price_str = response.xpath("//div[@class='cost_box']/p").extract_first()
        price = re.findall('\d+[.]*\d+', price_str)
        if not price:
            price = ['0']
        item['price'] = price[0]
        item['format'] = ''
        item['papermeter'] = ''
        item['packing'] = ''
        item['coverurl'] = response.xpath("//div[@class='bookCover_area']/img/@src").extract_first()
        item['seriename'] = ''
        catalog_list = response.xpath("//div[@id='catalog_title']//text()").extract()
        catalog = '<br>'.join(catalog_list)
        item['catalog'] = catalog
        item['editorsugest'] = ''
        item['usersugest'] = ''
        item['preface'] = ''
        item['summary'] = ''
        item['epilogue'] = ''
        publishdate = re.findall('出版时间：([\d]{4}-[\d]{2})', basic_info_str)
        if not publishdate:
            publishdate = ['']
        publishdate = publishdate[0]
        item['publishdate'] = publishdate
        item['printedtime'] = publishdate
        item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item['orgcode'] = ''
        item['skuid'] = skuid
        commentcount = response.xpath("//div[@class='count_per']/em/text()").extract_first()
        if not commentcount:
            commentcount = ''
        commentcount = re.findall('\d+',commentcount)
        if not commentcount:
            commentcount = ['']
        item['commentcount'] = commentcount[0]
        item['_row'] = skuid + item['sourcetype']
        item['coverpath'] = '/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/' + item['_row'] + '.jpg'
        item['is_set'] = '否'
        item['ifimport'] = '0'
        item['url'] = response.url
        item['_entitycode'] = 'web_page_p_book_info_09'
        item['commentpercent'] = ''
        item['commenttag'] = ''
        item['authorintro'] = ''
        item['sourceprice'] = ''
        # 获取评论列表

        comments = self.get_comments(skuid)
        # 遍历评论列表
        for comment in comments:
            comment_item = CommentItem()
            try:
                uri = 'http://e.dangdang.com/post_detail_page.html?barId='+str(comment['barId'])+'&digestId='+str(comment['mediaDigestId'])
                comment_item['isbn'] = ''
                comment_item['uri'] = uri
                comment_item['bookname'] = bookname
                comment_item['sourcetype'] = '02'
                comment_item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                publishdate_ts = comment['createDateLong']/1000
                publishdate_c = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(publishdate_ts))
                comment_item['publishtime'] = publishdate_c
                comment_item['username'] = comment['userBaseInfo']['nickName']
                comment_item['hitcount'] = '0'
                comment_item['follownum'] = comment['commentNum']
                comment_item['suportnum'] = comment['commentStar']
                comment_item['opposnum'] = '0'
                comment_item['commentid'] = comment['mediaDigestId']
                comment_item['followcommentid'] = ''
                comment_item['commenttitle'] = ''
                comment_item['commenttype'] = '1'
                comment_item['comment'] = comment['content']
                comment_item['score'] = '5'
                comment_item['level'] = '0'
                comment_item['commpoint'] = ''
                comment_item['type'] = '02'
                comment_item['sitename'] = '当当'
                comment_item['_row'] = skuid + comment_item['sourcetype'] + comment_item['publishtime'] + hashlib.md5(comment_item['username'].encode('utf-8')).hexdigest()[8:-8]
                comment_item['_entitycode'] = 'web_page_p_book_comment_09'
                comment_item['skuid'] = skuid
                yield comment_item
            except :
                continue
        for item_key in item_list:
            if not item[item_key]:
                item[item_key] = ''
        yield item

    def get_comments(self,skuid):
        url = 'http://e.dangdang.com/media/api.go?action=queryArticleListV2&deviceSerialNo=html5&macAddr=html5&channelType=html5&permanentId=20180803175812008595579769724942218&returnType=json&channelId=70000&clientVersionNo=5.8.4&platformSource=DDDS-P&fromPlatform=106&deviceType=pconline&token=&objectId=' + skuid
        response = requests.get(url)
        data_dict = json.loads(response.text)
        try:
            comments = data_dict['data']['articleList']
        except:
            comments = []
        return comments


        