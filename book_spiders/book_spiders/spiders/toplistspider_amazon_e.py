import json
import re
import time
from urllib import parse

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

class Toplist_amazon_e(RedisSpider):
    name = 'toplist_amazon_e'

    allowed_domains = ['amazon.com']

    # start_urls = ['https://www.amazon.cn/dp/B01ARNQJE0/ref=zg_bs_116169071_80/459-7867710-4711019?_encoding=UTF8&psc=1&refRID=FC4HJH0QXQ5AKTBDSJKG']

    redis_key = 'toplist_amazon_e:start_urls'

    _logger = Logger().getLogger()


    def parse(self, response):
        """
        :param response:
        :return:
        :author: eason
        """
        item = BookItem()
        for item_key in item_list:
            item[item_key] = ''
        item['is_set'] = '否'
        is_set = '否'
        # 判断isbn是否满足要求
        isbn = self.get_basicinfo(response,'ISBN')
        isbn_list = isbn.split(',')
        if len(isbn_list) == 1:
            isbn = isbn_list[0]
        elif len(isbn_list) > 1:
            for i in isbn_list:
                i = i.strip()
                if len(i) == 13:
                    isbn = i
        if not isbn:
            isbn = ''
        if is_set == '否':
            skuid = self.get_basicinfo(response,'ASIN')
            # 加载商品描述信息接口
            html = self.get_content_and_cate(skuid)
            bookname = response.xpath("//h1/span[@id='ebooksProductTitle']/text()").extract_first()
            bookname = bookname.strip()
            item['bookname'] = bookname
            item['subhead'] = ''
            publisher_str = self.get_basicinfo(response,'出版社')
            publisher = publisher_str.split(';')[0].strip()
            item['publisher'] = publisher
            item['orgpublisher'] = publisher
            try:
                contentsummary = re.findall('bookDescEncodedData = "(.*)"',response.text)[0]
                contentsummary = parse.unquote(contentsummary)
                contentsummary = self.unescape(contentsummary)
                contentsummary = etree.HTML(contentsummary)
                contentsummary = contentsummary.xpath("//body//text()")
                contentsummary = '<br>'.join(contentsummary)
            except:
                contentsummary = ''
            item['contentsummary'] = contentsummary
            item['sourcetype'] = '05'
            author_list = response.xpath("//div[@id='bylineInfo']/span[1]/a/text()").extract()
            author = '#'.join(author_list)
            item['author'] = author
            translator_list = response.xpath("//div[@id='bylineInfo']/span[2]/a/text()").extract()
            translator = '#'.join(translator_list)
            item['translator'] = translator
            item['isbn'] = isbn
            item['orgisbn'] = isbn
            item['salecategory'] = ''
            item['category'] = ''
            item['orgcategory'] = ''
            contenttype_list = response.xpath("//div[@id='wayfinding-breadcrumbs_feature_div']//span[@class='a-list-item']/a/text()").extract()
            for index,c in enumerate(contenttype_list):
                contenttype_list[index] = c.strip()
            contenttype = ','.join(contenttype_list)
            item['contenttype'] = contenttype
            item['issuearea'] = ''
            item['type'] = '02'
            packing = ''
            edition = re.findall('第(\d+)版',publisher_str)
            if not edition:
                edition = ['']
            item['edition'] = edition[0]
            item['impression'] = ''
            item['words'] = ''
            pages = re.findall('\d+', self.get_basicinfo(response,packing))
            if not pages:
                pages = ['']
            pages = pages[0]
            item['pages'] = pages

            item['language'] = self.get_basicinfo(response, '语种')
            price = response.xpath("//tr[@class='kindle-price']//text()").extract()
            price = ''.join(price)
            price = re.findall('\d+[.]*\d+', price)
            item['price'] = price[0]
            item['format'] = self.get_basicinfo(response, '开本')
            item['papermeter'] = ''
            item['packing'] = packing
            item['coverurl'] = response.xpath("//div[@id='ebooks-img-canvas']/img/@src").extract_first()
            item['seriename'] = ''
            item['catalog'] = ''
            item['editorsugest'] = ''
            item['usersugest'] = ''
            item['preface'] = ''
            item['summary'] = ''
            item['epilogue'] = ''

            pub_list = re.findall('(\d+)年(\d+)月',publisher_str)
            publishdate = '-'.join(pub_list[0])
            item['publishdate'] = publishdate
            item['printedtime'] = publishdate
            item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['orgcode'] = ''
            item['skuid'] = skuid
            commentcount = response.xpath("//span[@id='acrCustomerReviewText']/text()").extract_first()
            if not commentcount:
                commentcount = '0'
            commentcount = re.findall('(\d+)*', commentcount)
            commentcount = ''.join(commentcount)
            item['commentcount'] = commentcount
            item['_row'] = skuid + item['sourcetype']
            item['coverpath'] = '/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/' + item['_row'] + '.jpg'
            item['is_set'] = '否'
            item['ifimport'] = '0'
            item['url'] = response.url
            item['_entitycode'] = 'web_page_p_book_info_09'
            item['commentpercent'] = ''
            try:
                tag_resp = self.get_commenttag(skuid)
                commenttag = tag_resp.xpath("//span/@data-cr-trigger-on-view")
                commenttag = json.loads(commenttag[0])
                commenttag = commenttag['ajaxParamsMap']['lighthouseTerms'].replace('/', '#')
            except:
                commenttag = ''
            item['commenttag'] = commenttag
            item['authorintro'] = ''
            item['sourceprice'] = ''
            comments = response.xpath("//div[@id='cm-cr-dp-review-list']/div")
            #遍历评论列表
            if comments:
                for comment in comments:
                    comment_item = CommentItem()
                    comment_item['isbn'] = isbn
                    comment_item['uri'] = response.url
                    comment_item['bookname'] = bookname
                    comment_item['sourcetype'] = item['sourcetype']
                    comment_item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    publishdate_c = response.xpath("//span[@data-hook='review-date']/text()").extract_first()
                    if not publishdate_c:
                        publishdate_c = ''
                    else:
                        pub_list = re.findall('(\d+)年(\d+)月(\d+)日', publishdate_c)
                        publishdate_c = '-'.join(pub_list[0])
                    comment_item['publishtime'] = publishdate_c
                    username = comment.xpath("./div/div[1]/a/div/span/text()").extract_first()
                    if not username:
                        username = ''
                    comment_item['username'] = username
                    comment_item['hitcount'] = '0'
                    comment_item['follownum'] = '0'
                    suportnum = comment.xpath(".//span[@data-hook='helpful-vote-statement']/text()").extract_first()
                    if not suportnum:
                        suportnum = '0'
                    suportnum = re.findall('\d+',suportnum)[0]
                    comment_item['suportnum'] = suportnum
                    comment_item['opposnum'] = '0'
                    comment_item['commentid'] = comment.xpath("./@id").extract_first()
                    comment_item['followcommentid'] = ''
                    commenttitle = comment.xpath(".//a[@data-hook='review-title']/text()").extract_first()
                    if not commenttitle:
                        commenttitle = ''
                    comment_item['commenttitle'] = commenttitle
                    comment_item['commenttype'] = '0'
                    comment_strs = comment.xpath(".//div[@data-hook='review-collapsed']/text()").extract()
                    comment_strs = ''.join(comment_strs)
                    comment_item['comment'] = comment_strs
                    score = comment.xpath("//div[@id='cm-cr-dp-review-list']/div[1]/div[1]/div[2]/a/@title").extract_first()
                    if not score:
                        score = ['5.0']
                    score = re.findall('\d.\d', score)[0]
                    score = score[:1]
                    comment_item['score'] = score
                    score = float(score)
                    if score < 2:
                        level = '2'
                    elif score < 4:
                        level = '1'
                    else:
                        level = '0'
                    comment_item['level'] = level
                    comment_item['commpoint'] = ''
                    comment_item['type'] = '02'
                    comment_item['sitename'] = '亚马逊'
                    comment_item['_row'] = comment_item['isbn'] + comment_item['sourcetype'] + comment_item['publishtime'] + comment_item['commentid']
                    comment_item['_entitycode'] = 'web_page_p_book_comment_09'
                    comment_item['skuid'] = skuid
                    yield comment_item
            yield item

    def unescape(slef,s):
        """
        ascii码转为中文
        :return:
        """
        if '&' not in s:
            return s

        def replaceEntities(s):
            s = s.groups()[0]
            try:
                if s[0] == "#":
                    s = s[1:]
                    if s[0] in ['x', 'X']:
                        c = int(s[1:], 16)
                    else:
                        c = int(s)
                    return chr(c)
            except ValueError:
                return '&#' + s + ';'

        return re.sub(r"&(#?[xX]?(?:[0-9a-fA-F]+|\w{1,8}));", replaceEntities, s)


    def get_content_and_cate(self, jid):
        """
        获取图书的简介和目录信息
        :param jid: 图书的商品id
        :return: 返回图书的简介和目录信息
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/68.0.3440.106 Safari/537.36'
        }
        url = 'https://www.amazon.cn/gp/product-description/ajaxGetProuductDescription.html?&asin=%s&deviceType=web' %jid

        data = requests.get(url,headers = headers)
        html = etree.HTML(data.text)
        return html


    def get_basicinfo(self,response,tag):
        key_list = response.xpath("//div[@class='content']/ul/li/b/text()").extract()
        # li_str_list = response.xpath("//div[@class='content']/ul/li/text()").extract()
        result = ''
        for index,key in enumerate(key_list):
            flag = key.find(tag)
            if not flag == -1:
                result = response.xpath("//div[@class='content']/ul/li[%s]/text()" %(index+1)).extract_first()
                if not result:
                    result = ''
                break
        result = result.strip()
        return result

    def get_commenttag(self,skuid):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate, br',
            'Accept-Language':'zh-CN,zh;q=0.9',
            'Cache-Control':'no-cache',
            'Connection':'keep-alive',
            'Cookie':'at-main=Atza|IQEBLzAtAhRs_7fGRbboBRf-6t7hvvaDAtVLGgIVAI-0WfE2dLoP4QloNibAF8htiTCgB46Fy2JV02uFJH2PvrqrPKFOJzcqmgBfzCqtXEGFgLJVKEuBhNG6wrP1d4k59c_CmGFJlWvN1bAW0utxLbl_V8uLbHZskp9-F0FfHMcqYhubRYqkafvdhSy6TmEiFqEnC2qF7O0rVhGfA975DS5qDqG9K0LsCThZexpN1a_mAYpVOxngP0EM_hg9TCRM7VfQzEdHwAqs0fd9UlNtGCYPo-bU55t3nvmRGG_Yx86rxUi3OHZEVTZ6qhkSNIbf0pgN1quzMnBma0tePF4ZeyANYS22K8v-hczQly18LrLHLL2MXs-aiWyKsiZ1r-ByZebrSw; x-wl-uid=1kZ0g48khMa8wOTuoewoBNn4g7ncLMUl7VYmBr5N2FXjWj2tCFl5zpi9EvkFp0nYjHwpNeIUypMY=; session-id=462-4549085-3938830; ubid-acbcn=457-9140799-8946539; csm-hit=tb:s-8SRM60PBD5JFZWVR35NK|1540018787612&adb:adblk_no; session-token=8nxCDslV4rXknHWDD7NmMeeHTDGCkxIyASZqmx2iheBDSJS1Of/M3H/edanbiE/+DQn6yfoRsFs/DW4F//9tb2/BMeHC5R2lcxsFOkRaotzR/PuPVBGYPK81lgqvtPaS1lAUeaXRWBWTd5us6nmwKzu8e0BVUvRmLAluBiTsgcoi7256tckGgqIP7beRaJ9q; session-id-time=2082787201l',
            'Host':'www.amazon.cn',
            'Pragma':'no-cache',
            'Upgrade-Insecure-Requests':'1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
        }
        url = 'https://www.amazon.cn/dp/%s/hz/reviews-render/ajax/lighthut/report/' %skuid
        data = requests.get(url,headers = headers)
        html = etree.HTML(data.text)
        return html




        