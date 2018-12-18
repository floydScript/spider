# -*- coding: utf-8 -*-
import scrapy
from scrapy_redis.spiders import RedisSpider

from book_spiders.items import BookItem
from book_spiders.items import CommentItem
import datetime

import hashlib
import re

from book_spiders.mylogger import Logger


class Toplist_douban_e(scrapy.Spider):
    # 豆瓣图书爬虫
    name = 'toplist_douban'

    # 允许的域名
    allowed_domains = ['book.douban.com']

    # 起始url
    # start_urls = ['https://book.douban.com/tag/?view=type&icn=index-sorttags-all']
    # start_urls = ['https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4']
    # start_urls = ['https://book.douban.com/subject/4049269/']
    # start_urls = ['https://book.douban.com/subject/27609047/']
    # start_urls = ['https://book.douban.com/subject/3202924/']
    start_urls = ['https://book.douban.com/subject/1848256/']

    # redis_key = 'douban_spider:start_urls'

    _logger = Logger().getLogger()

    def parse(self,response):
        self._logger.info("解析url："+response.url)
        douban_item = BookItem()
        list = ['preface', 'catalog', 'translator', 'isbn', 'subhead', 'edition', 'language', 'orgcategory', 'type',
                 'packing', 'seriename', 'coverurl', 'coverpath', 'pages', 'epilogue', 'price', 'publishdate',
                 'sourcetype', 'editorsugest', 'papermeter', 'printedtime', 'summary', 'orgisbn', 'author','usersugest',
                 'orgpublisher', 'words', 'format', 'issuearea', 'contenttype', 'contentsummary',
                 'salecategory', 'publisher', 'impression', 'bookname', 'category', 'collectiontime', 'orgcode','skuid',
                 'commentcount', 'ifimport', '_row', '_entitycode', 'url','commentpercent','commenttag','authorintro','sourceprice']
        for item_key in list:
            douban_item[item_key] = ''
        douban_item['bookname'] = response.xpath("//h1/span/text()").extract_first()
        selector_info = response.xpath("//div[@id='info']")
        key_list = response.xpath("//div[@id='info']//span/text()").extract()
        info_list = selector_info.xpath('./text()|a/text()|span/a/text()').extract()
        key_list = remove_meaningless_str(key_list)
        key_list = merge_key(key_list)
        info_list = remove_meaningless_str(info_list)
        info_list = merge_info(info_list)
        if len(key_list)>11:
            self._logger.error('出现封装字段数大于11的图书：'+response.url)
            self._logger.error(key_list)
        if len(key_list) == len(info_list):
            douban_item = packing_info(douban_item,key_list,info_list)
        else:
            self._logger.error('基础信息封装代码出现BUG:'+response.url)
        # 内容简介
        is_set = '否'
        if not douban_item['isbn'] or len(douban_item['isbn']) != 13:
            is_set = '是'
        if is_set == '否':
            contentsummary_selector_list = response.xpath("//div[@id='link-report']//div[@class='intro']")
            douban_item['contentsummary'] = packing_content(contentsummary_selector_list,-1)
            douban_item['sourcetype'] = '03'
            douban_item['salecategory'] = ''
            douban_item['category'] = ''
            douban_item['orgcategory'] = ''
            contenttype = response.xpath("//a[@class='  tag']/text()").extract()
            contenttype = ','.join(contenttype)
            douban_item['contenttype'] = contenttype
            douban_item['issuearea'] = ''
            douban_item['type'] = '01'
            douban_item['edition'] = ''
            douban_item['impression'] = ''
            douban_item['words'] = ''
            douban_item['language'] = ''
            douban_item['printedtime'] = ''
            douban_item['format'] = ''
            douban_item['papermeter'] = ''
            # 封面url
            douban_item['coverurl'] = response.xpath("//a[@class='nbg']/@href").extract_first()
            # 保存图片地址
            today_str = str(datetime.datetime.now()).split(".")[0].split()[0].replace('-', '')
            sku_id = re.findall(r"\d+",response.url)[0]
            douban_item['coverpath'] = '/book/'+today_str+'/'+'03'+douban_item['isbn']+'.png'
            # 目录
            catalog_selector_list = response.xpath("//div[@id='dir_"+sku_id+"_full']")
            douban_item['catalog'] = packing_content(catalog_selector_list)

            douban_item['editorsugest'] = ''
            douban_item['usersugest'] = ''
            douban_item['preface'] = ''
            douban_item['summary'] = ''
            douban_item['epilogue'] = ''
            # 收集时间
            now_str = str(datetime.datetime.now()).split(".")[0]
            douban_item['collectiontime'] = now_str

            douban_item['orgcode'] = ''

            douban_item['skuid'] = sku_id
            comment_count_list = response.xpath("//h2/span[@class='pl']/a/text()").extract()
            comment_count = 0
            if comment_count_list != None:
                for i in comment_count_list:
                    i = re.findall(r"\d+",i)
                    if len(i)==0:
                        i.append('0')
                    comment_count = comment_count+int(i[0])
            douban_item['commentcount'] = str(comment_count)

            douban_item['ifimport'] = '0'
            douban_item['_row'] = douban_item['skuid']+'03'
            douban_item['_entitycode'] = 'web_page_p_book_info_09'
            douban_item['is_set'] = '否'
            douban_item['url'] = response.url
            douban_item['commentpercent'] = ''
            douban_item['commenttag'] = ''
            douban_item['authorintro'] = ''
            douban_item['sourceprice'] = ''



            # 书评列表地址
            long_comment_list = response.xpath("//div[@class='main-bd']/h2/a/@href").extract()
            if  len(long_comment_list) >0:
                for long_comment_link in long_comment_list:
                    yield scrapy.Request(long_comment_link, meta={'douban_item': douban_item}, callback=self.parse_long_comment)
            # 短评地址
            short_comment_link = response.xpath("//div[@class='related_info']/p/a/@href").extract_first()
            if short_comment_link:
                yield scrapy.Request(short_comment_link,meta={'douban_item':douban_item},callback=self.parse_short_comment)
            yield douban_item

            # 添加相关图书的链接
            similarity_urls = response.xpath("//dd//a/@href").extract()
            if len(similarity_urls)>0:
                for similarity_url in similarity_urls:
                    # 判断url是否已经爬取过
                    # sku_id = re.findall(r"\d+", similarity_url)[0]
                    # querysql = 'select * from web_page_book_info_09_douban where skuid = %s' %sku_id
                    # self.cursor.execute(querysql)
                    # result = self.cursor.fetchone()
                    # if result != None:
                    #     continue
                    yield scrapy.Request(similarity_url)

    def parse_short_comment(self,response):
        self._logger.info("请求短评列表页：" + response.url)
        douban_item = response.meta['douban_item']
        comment_list = response.xpath("//li[@class='comment-item']")
        for comment in comment_list:
            comment_item = CommentItem()
            comment_item['isbn'] = douban_item['isbn']
            comment_item['uri'] = response.url
            comment_item['bookname'] = douban_item['bookname']
            comment_item['sourcetype'] = '03'
            comment_item['collectiontime'] = douban_item['collectiontime']
            publishtime = comment.xpath("./div/h3/span[@class='comment-info']/span/text()").extract()[-1]
            publishtime = publishtime+' 00:00:00'
            comment_item['publishtime'] = publishtime
            comment_item['username'] = comment.xpath("./div[@class='avatar']/a/@title").extract_first()
            comment_item['hitcount'] = ''
            comment_item['follownum'] = ''
            comment_item['suportnum'] = comment.xpath("./div/h3/span[@class='comment-vote']/span/text()").extract_first()
            comment_item['opposnum'] = ''
            comment_item['commentid'] = str(comment.xpath("./@data-cid").extract_first())
            comment_item['followcommentid'] = ''
            comment_item['commenttitle'] = ''
            comment_item['commenttype'] = '1'
            comment_item['comment'] = comment.xpath(".//span[@class='short']/text()").extract_first()
            score_str = comment.xpath(".//span[@class='comment-info']/span[1]/@class").extract_first()
            if score_str == None:
                score_str = '30'
            score = int(re.findall(r"\d+", score_str)[0]) / 10
            comment_item['score'] = score
            level = 2
            if score>3:
                level = 0
            elif score == 2 or score == 3:
                level = 1
            comment_item['level'] = level
            comment_item['commpoint'] = ''
            comment_item['type'] = '01'
            comment_item['sitename'] = '豆瓣'
            comment_item['_row'] =hashlib.md5((response.url).encode('utf-8')).hexdigest()[8:-8]+'03'+publishtime+hashlib.md5(comment_item['username'].encode('utf-8')).hexdigest()[8:-8]
            comment_item['_entitycode'] = 'web_page_p_book_comment_09'
            comment_item['ifimport'] = '0'
            yield comment_item

    def parse_long_comment(self, response):
        self._logger.info("请求长评详情页：" + response.url)
        douban_item = response.meta['douban_item']
        comment_item = CommentItem()
        comment_item['isbn'] = douban_item['isbn']
        comment_item['uri'] = response.url
        comment_item['bookname'] = douban_item['bookname']
        comment_item['sourcetype'] = '03'
        comment_item['collectiontime'] = douban_item['collectiontime']
        comment_item['publishtime'] = response.xpath("//span[@class='main-meta']/text()").extract_first()
        comment_item['username'] = response.xpath("//header[@class='main-hd']/a[1]/span/text()").extract_first()
        comment_item['hitcount'] = ''
        comment_item['follownum'] = response.xpath("//span[@class='rec-num']/text()").extract_first()
        comment_item['suportnum'] = re.findall(r"\d+",response.xpath("//div[@class='main-panel-useful']/button[1]/text()").extract_first())[0]
        comment_item['opposnum'] = re.findall(r"\d+",response.xpath("//div[@class='main-panel-useful']/button[2]/text()").extract_first())[0]
        comment_item['commentid'] = re.findall(r"\d+",response.url)[0]
        comment_item['followcommentid'] = ''
        comment_item['commenttitle'] = response.xpath("//h1/span/text()").extract_first()
        comment_item['commenttype'] = '0'
        comment_item['comment'] = response.xpath("//div[@class='main-bd']").xpath("string(.)").extract()[0]
        score_list = re.findall(r"\d+", response.xpath("//header[@class='main-hd']/span[1]/@class").extract_first())
        score = 0
        if len(score_list) > 0:
            score = int(score_list[0]) / 10
        comment_item['score'] = score
        level = 2
        if score > 3:
            level = 0
        elif score == 2 or score == 3:
            level = 1
        comment_item['level'] = level
        comment_item['commpoint'] = ''
        comment_item['type'] = '01'
        comment_item['sitename'] = '豆瓣'
        comment_item['_row'] = hashlib.md5((response.url).encode('utf-8')).hexdigest()[8:-8] + '03' + comment_item['publishtime'] + hashlib.md5(comment_item['username'].encode('utf-8')).hexdigest()[8:-8]
        comment_item['_entitycode'] = 'web_page_p_book_comment_09'
        comment_item['ifimport'] = '0'
        yield comment_item


# 封装标签内的所有内容
def packing_content(list,index = 0 ):
    if len(list) == 0 :
        return ''
    return list[index].xpath("string(.)").extract()[0]

# 封装基础信息
def packing_info(douban_item,key_list,info_list):
    translator = None
    author = None
    for index, key in enumerate(key_list):
        if key == '作者':
            if author == None:
                author = info_list[index]
            else:
                author = author + '#' + info_list[index]
            douban_item['author'] = author.replace(';','#').replace('、','#').replace('，','#').replace(',','#')
        elif key == '出版社':
            douban_item['publisher'] = info_list[index]
            douban_item['orgpublisher'] = info_list[index]
        elif key == '副标题':
            douban_item['subhead'] = info_list[index]
        elif key == '译者':
            if translator == None:
                translator = info_list[index]
            else:
                translator = translator + '#' + info_list[index]
            douban_item['translator'] = translator
        elif key == '出版年':
            douban_item['publishdate'] = date_format(info_list[index])
        elif key == '页数':
            douban_item['pages'] = re.findall(r"\d+", info_list[index])[0]
        elif key == '定价':
            price = re.findall(r"\d+\.?\d*", info_list[index])[0]
            douban_item['price'] = price.split('.')[0]+'.00'
        elif key == '装帧':
            if info_list[index] == 'Hardcover':
                info_list[index] = '精装'
            elif info_list[index] == 'Paperback':
                info_list[index] = '平装'
            douban_item['packing'] = info_list[index]
        elif key == '丛书':
            douban_item['seriename'] = info_list[index]
        elif key == 'isbn' or key == 'ISBN':
            douban_item['isbn'] = info_list[index]
            douban_item['orgisbn'] = info_list[index]
        elif key == '统一书号':
            douban_item['orgisbn'] = info_list[index]
    return douban_item

# 合并info:合并作者
def merge_info(list):
    for index,name in enumerate(list):
        if name == '/':
            list[index-1] = list[index-1]+'#'+list[index+1]
            list.pop(index + 1)
            list.pop(index)
    return list

# 合并key:合并译者
def merge_key(list):
    for index,name in enumerate(list):
        if name == '/':
            list[index] = list[index-1]
    return list

# 去除无意义的字符
def remove_meaningless_str(list):
    i = len(list) - 1
    for info in reversed(list):
        info = info.replace(' ', '').replace('\n', '').replace('\xa0', '').replace(':','')
        if info == '':
            list.pop(i)
            i = i - 1
            continue
        list[i] = info
        i = i - 1
    return list

# 拼接url
def url_splice(url):
    if url.startswith('https://book.douban.com'):
        return url
    else:
        return 'https://book.douban.com'+url

# 转换日期格式
def date_format(time):
    if re.findall('[a-zA-Z]', time) or re.findall('^[34567890]', time):
        return ''
    publishdate = time.replace('年','-').replace('月','-').replace('日','').replace('.','-')
    publishdate_list = publishdate.split('-')
    if len(publishdate_list) ==1:
        if len(publishdate) == 6 or len(publishdate) == 8:
            sec_int = int(publishdate[4]+publishdate[5])
            if sec_int>12:
                publishdate = publishdate[0]+publishdate[1]+publishdate[2]+publishdate[3]+'0'+publishdate[4]+'0'+publishdate[5]
                publishdate = date_format(publishdate)
            elif sec_int<12:
                publishdate = publishdate[0]+publishdate[1]+publishdate[2]+publishdate[3]+'-'+publishdate[4]+publishdate[5]+'-'+publishdate[6]+publishdate[7]
        elif  len(publishdate) == 4:
            publishdate = publishdate+'-01-01'
    if len(publishdate_list) >1:
        if len(publishdate_list[1]) == 1:
            publishdate_list[1] = '0'+publishdate_list[1]
        if len(publishdate_list[-1]) == 1:
            publishdate_list[-1] = '0'+publishdate_list[-1]
        if len(publishdate_list) ==2:
            publishdate_list.append('01')
        publishdate = '-'.join(publishdate_list)
    return publishdate

