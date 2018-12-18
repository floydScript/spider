import json
import re
import time

import requests
import scrapy

from book_spiders.items import BookItem
from book_spiders.items import CommentItem
from book_spiders.conf import Conf
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

class BookSpider_dd(RedisSpider):
    name = 'dang_dang'

    allowed_domains = ['dangdang.com']

    redis_key = 'dang_dang:start_urls'

    _logger = Logger().getLogger()
    def __init__(self):
        config = Conf.config

        self.db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'], user=config['mysql']['username'],
                                passwd=config['mysql']['password'], db=config['mysql']['dbname'], charset='utf8')
        self.cursor = self.db.cursor()
        self._logger.info('数据库连接成功')

    def parse(self, response):
        item = BookItem()
        # 将所有字段设为空串
        for item_key in item_list:
            item[item_key] = ''
        item['is_set'] = '是'
        try:
            is_set = response.xpath('//div[@id="detail_describe"]/ul/li[4]/text()').extract_first()[-1:]
        except Exception as e:
            self._logger.error(e)
            is_set = '是'
        try:
            isbn = response.xpath('//div[@id="detail_describe"]/ul/li[5]/text()').extract_first()
            isbn = isbn.split('：')[1]
        except Exception as e:
            self._logger.error(e)
            isbn = ''
        item['orgisbn'] = isbn
        # 如果isbn长度不是13位的话，置为空，不存进数据库
        if len(isbn) != 13:
            isbn = ''
            is_set = '是'
        item['isbn'] = isbn
        if is_set == '否' :
            # 获得商品id和店铺id
            skuid = re.findall('\d+', response.url)[0]
            shopid = response.xpath("//p[@class='goto_shop']/a[1]/@href").extract_first().split('/')[-1]

            # 调用接口以获取动态加载的数据
            timemil_start = time.time()
            descrip_html = self.descrip_inter(skuid)
            comment_dict = self.comment_inter(skuid)
            price_dict = self.price_inter(skuid, shopid)
            tags = self.tag_inter(skuid)
            alsobuy_urls = self.alsobuy_inter(skuid, shopid)
            timemil_end = time.time()
            self._logger.info('解析url：'+response.url+'    ===>调取接口耗时:'+str(timemil_end-timemil_start)+' s')
            for url_item in alsobuy_urls:
                ab_url = 'http://product.dangdang.com/' + url_item['productId']+'.html'
                taskId = binascii.crc32((ab_url).encode())
                ab_taskname = url_item['productName']
                # 往site_book表中插入url任务
                sql = '''insert into site_book(siteId,taskId,taskName,taskCode,startUrl,requestTimes,pollPeriod,autorun,status,crawlTime,maxDepth,threadNum,sleepTime,saveTime,newsType,rollUnit) 
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                params = (530701699,taskId,ab_taskname,'20',ab_url,3,86400,1,2,'2016-01-01 00:00:00',3,10,100,datetime.datetime.now(),'0','1')
                try:
                    self.cursor.execute(sql, params)
                    self.db.commit()
                    self._logger.info('插入任务：taskId为 ' + str(taskId) + '  url为  ' + ab_url)
                    yield scrapy.Request(url=ab_url, callback=self.parse)
                except Exception as e:
                    pass
            item['is_set'] = '否'
            bookname = response.xpath('//div[@id="product_info"]/div[1]/h1/@title').extract_first()
            item['bookname'] = bookname
            subhead = response.xpath("//span[@class='head_title_name']/@title").extract_first()
            if not subhead:
                subhead = ''
            item['subhead'] = subhead
            publisher = response.xpath('//div[@id="product_info"]/div[2]/span[2]/a/text()').extract_first()
            item['publisher'] = publisher
            item['orgpublisher'] = publisher
            item['contentsummary'] = self.packing_descrip(descrip_html,'content')
            item['editorsugest'] = self.packing_descrip(descrip_html,'abstract')
            item['sourcetype'] = '02'
            try:
                author_klist = response.xpath('//span[@id="author"]/text()').extract()
                author_list = response.xpath('//a[@dd_name="作者"]/text()').extract()
                author = []
                translator = []
                flag = True
                for index,k in enumerate(author_klist):
                    if flag:
                        author.append(author_list[index])
                        next_index = index+1
                        if next_index == len(author_klist):
                            continue
                        if author_klist[next_index] != '，' and author_klist[next_index] != ',':
                            flag = False
                    else:
                        if index >= len(author_list):
                            break
                        translator.append(author_list[index])
                author = '#'.join(author)
                translator = '#'.join(translator)
            except Exception as e:
                self._logger.error('作者信息封装失败')
                author = item['publisher']
                translator = ''
            item['author'] = author
            item['translator'] = translator
            item['salecategory'] = ''
            item['category'] = ''
            item['orgcategory'] = ''
            contenttype = response.xpath('//li[@id="detail-category-path"]/span/a/text()').extract()
            contenttype = ','.join(contenttype)
            item['contenttype'] = contenttype
            item['issuearea'] = '0'
            item['type'] = '01'
            # 版次
            item['edition'] = ''
            # 印次
            item['impression'] = ''
            item['words'] = ''
            item['pages'] = ''
            item['language'] = ''
            item['price'] = price_dict['price']
            printedtime = response.xpath('//div[@id="product_info"]/div[2]/span[3]/text()').extract_first()
            if printedtime:
                printedtime = printedtime.strip()
                printedtime = printedtime[5:-1].replace('年', '-')
            else:
                printedtime = ''
            item['printedtime'] = printedtime
            format = response.xpath('//div[@id="detail_describe"]/ul/li[1]/text()').extract_first()[4:]
            item['format'] = format
            papermeter = response.xpath('//div[@id="detail_describe"]/ul/li[2]/text()').extract_first()[4:]
            item['papermeter'] = papermeter
            packing = response.xpath('//div[@id="detail_describe"]/ul/li[3]/text()').extract_first()[4:]
            item['packing'] = packing
            coverurl = response.xpath('//img[@id="largePic"]/@src').extract_first()
            item['coverurl'] = coverurl
            item['seriename'] = ''
            item['catalog'] = self.packing_descrip(descrip_html,'catalog')
            item['usersugest'] = self.packing_descrip(descrip_html,'mediaFeedback')
            item['preface'] = self.packing_descrip(descrip_html,'preface')
            item['summary'] = self.packing_descrip(descrip_html,'extract')
            item['epilogue'] = ''
            item['publishdate'] = printedtime
            item['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['orgcode'] = ''
            item['skuid'] = skuid
            item['_row'] =  skuid+'02'
            item['coverpath'] ='/book/' + datetime.datetime.now().strftime('%Y%m%d') + '/'+item['_row'] + '.jpg'
            item['commentcount'] = comment_dict['commentcount']
            item['ifimport'] = '0'
            item['url'] = response.url
            item['_entitycode'] = 'web_page_p_book_info_09'
            item['commentpercent'] = comment_dict['commentpercent']
            item['commenttag'] = tags
            item['authorintro'] = self.packing_descrip(descrip_html,'authorIntroduction')
            item['sourceprice'] = price_dict['sourceprice']

            comments = comment_dict['comments']
            if comments:
                for comment in comments:
                    try:
                        citem = CommentItem()
                        citem['isbn'] = isbn
                        uri = comment.xpath('./div[1]/div[2]//a/@href')
                        if not uri:
                            uri = [response.url]
                        uri = ''.join(uri)
                        citem['uri'] = uri
                        citem['bookname'] = bookname
                        citem['sourcetype'] = '02'
                        citem['collectiontime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        publishtime = comment.xpath('./div[1]/div[4]/span[1]/text()')
                        if not publishtime:
                            continue
                        publishtime = ''.join(publishtime)
                        citem['publishtime'] = publishtime
                        username = comment.xpath('./div[2]/span[1]/text()')
                        if not username:
                            username = []
                        username = ''.join(username)
                        citem['username'] = username
                        citem['hitcount'] = '0'
                        citem['follownum'] = '0'
                        suportnum = comment.xpath('./div[1]/div[5]/a[1]/text()')
                        suportnum = ''.join(suportnum)
                        if suportnum == '赞':
                            suportnum = '0'
                        citem['suportnum'] = suportnum
                        citem['opposnum'] = '0'
                        commentid = str(binascii.crc32((username + publishtime).encode()))
                        citem['commentid'] = commentid
                        citem['followcommentid'] = '-1'
                        citem['commenttitle'] = ''
                        citem['commenttype'] = '0'
                        commentcontent = comment.xpath('./div[1]/div[2]//a/text()')
                        commentcontent = ''.join(commentcontent)
                        citem['comment'] = commentcontent
                        score = comment.xpath('./div[1]/div[1]/em/text()')
                        score = ''.join(score)
                        if not score:
                            score = '5'
                        score = score[:-1]
                        score = int(score) / 2
                        citem['score'] = str(score)
                        if score < 2:
                            citem['level'] = '2'
                        elif score < 4:
                            citem['level'] = '1'
                        else:
                            citem['level'] = '0'
                        citem['commpoint'] = ''
                        citem['type'] = '01'
                        citem['sitename'] = '当当'
                        citem['_row'] = citem['isbn'] + citem['sourcetype'] + citem['publishtime'] + hashlib.md5(citem['username'].encode('utf-8')).hexdigest()[8:-8]
                        citem['_entitycode'] = 'web_page_p_book_comment_09'
                        citem['skuid'] = skuid
                        for citem_key in citem_list:
                            if not citem[citem_key]:
                                citem[citem_key] =''
                        yield citem
                    except Exception as e:
                        self._logger.error(e)
                        continue
        for item_key in item_list:
            if not item[item_key]:
                item[item_key] = ''
        yield item
    

    def replace_label(self, msg):
        if not msg:
            return ''
        msg = etree.HTML(msg)
        msg = msg.xpath('//*')[0].xpath('string(.)')
        return msg

    def packing_descrip(self,response,tag):
        """
        封装描述信息
        :param response: 响应的html
        :param tag:模块名
        :return:item 描述信息  String
        """
        try:
            item = response.xpath("//div[@id='%s']/div[@class='descrip']/textarea[@id='%s-textarea']//text()" %(tag,tag))
            if not item:
                item = response.xpath("//div[@id='%s']/div[@class='descrip']//text()" %tag)
                if not item:
                    item = ['']
            item = '<br>'.join(item)
            # item = self.replace_label(item)
        except Exception as e:
            self._logger.error(e)
            item =  ''
        return item


    def alsobuy_inter(self,productId,shopId):
        """
        “购买此商品的顾客还购买过” 接口
        :param productId:
        :param shopId:
        :return:
        """
        try:
            url = 'http://product.dangdang.com/index.php?r=callback%2Frecommend&productId=' + productId + '&shopId=' + shopId + '&pageType=publish&module=&isBroad=true'  # %(productId,shopId)
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Cookie': '__permanent_id=20180803175812008595579769724942218; NTKF_T2D_CLIENTID=guestD678BBB3-D9D6-902B-735B-FF393B7BB54D; gr_user_id=fbcff3b0-c64f-4f3e-a74d-2ee118edc15e; MDD_channelId=70000; MDD_fromPlatform=307; __ddc_15d_f=1537239145%7C!%7C_ddclickunion%3D419-955937%257C00Ih363c297840c85e7e; from=419-988140%7C00Iid1dfce02cb8c7242; order_follow_source=P-419-9881%7C%231%7C%23p.gouwubang.com%252Fl%253Fl%253Dclsp1k4u5nudnz446wbqnwvyrwwopn4vntpspnm2p7ptyq4du73qycdmpsawnsubpkzsyqbyrwwopn6x%7C%230%7C%23LjUAqOw63T81-%7C-; __ddc_24h=1537339302%7C!%7C_ddclickunion%3D419-988140%257C00Iid1dfce02cb8c7242; __ddc_15d=1537339302%7C!%7C_ddclickunion%3D419-988140%257C00Iid1dfce02cb8c7242; priceab=b; __visit_id=20180920093108125287580850932903509; __out_refer=; ddscreen=2; nTalk_CACHE_DATA={uid:dd_1000_ISME9754_guestD678BBB3-D9D6-90,tid:1537409563718690}; pos_9_end=1537412716465; pos_0_start=1537412716579; pos_0_end=1537412716584; dest_area=country_id%3D9000%26province_id%3D111%26city_id%3D1%26district_id%3D1110101%26town_id%3D-1; pos_6_start=1537412724333; pos_6_end=1537412724567; ad_ids=2755404%2C2755401%7C%231%2C1; __rpm=mix_317715.3208565%2C3208567..1537412706351%7Cs_112100.94003212839%2C94003212840.2.1537413001137; producthistoryid=' + productId + '%2C25245335%2C25159650%2C23427130%2C25324596%2C1901084091%2C25312560%2C1006539860%2C9239276%2C24516890; __trace_id=20180920111445289399703146326706692',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
            response = requests.get(url, headers=headers)
            html = json.loads(response.text)
            list = html['data']['alsoBuy']['list']
        except:
            list = []
        return list


    def price_inter(self,productId,shopId):
        """
        价格接口
        :param productId:
        :param shopId:
        :return:
        """
        try:
            url = 'http://product.dangdang.com/index.php?r=callback/product-info&productId=%s&isCatalog=0&shopId=%s&productType=0' % (productId, shopId)
            response = requests.get(url)
            html = json.loads(response.text)
            sourceprice = html['data']['spu']['price']['salePrice']
            price = html['data']['spu']['price']['originalPrice']
        except Exception as e:
            self._logger.error(e)
            sourceprice = ''
            price = ''
        return {'sourceprice':sourceprice,'price':price}



    def comment_inter(self,productId):
        """
        评论接口
        :param productId:
        :return:
        """
        try:
            url = 'http://product.dangdang.com/index.php?r=comment%2Flist&productId=' + productId + '&categoryPath=01.41.26.15.00.00&mainProductId=' + productId + \
                  '&mediumId=0&pageIndex=1&sortType=1&filterType=1&isSystem=1&tagId=0&tagFilterCount=0&template=publish'
            response = requests.get(url)
            resp_dict = json.loads(response.text)
            html = resp_dict['data']['list']['html']
            html_etree = etree.HTML(html)
            comment_list = html_etree.xpath("//div[@class='item_wrap']/div")
            try:
                for i in range(2, 10):
                    url = 'http://product.dangdang.com/index.php?r=comment%2Flist&productId=' + productId + '&categoryPath=01.41.26.15.00.00&mainProductId=' + productId + \
                          '&mediumId=0&pageIndex=' + str(i) + '&sortType=1&filterType=1&isSystem=1&tagId=0&tagFilterCount=0&template=publish'
                    response = requests.get(url)
                    resp_dict = json.loads(response.text)
                    html = resp_dict['data']['list']['html']
                    html_etree = etree.HTML(html)
                    comments = html_etree.xpath("//div[@class='item_wrap']/div")
                    comment_list.extend(comments)
            except:
                self._logger.error("=======调取评论接口发生错误  skuid:" + productId + "=======")
            commentcount = resp_dict['data']['list']['summary']['total_comment_num']
            commentpercent = resp_dict['data']['list']['summary']['goodRate']
        except Exception as e:
            self._logger.error("=======调取评论接口发生错误  skuid:"+productId+"=======")
            comment_list = None
            commentcount = ''
            commentpercent = ''
        return {'comments': comment_list, 'commentcount': commentcount, 'commentpercent': commentpercent}

    def tag_inter(self,productId):
        """
        “买过的人觉得” 接口
        :param productId:
        :return:
        """
        try:
            url = 'http://product.dangdang.com/index.php?r=comment%2Flabel&productId=' + productId + '&categoryPath=01.05.16.00.00.00'
            response = requests.get(url)
            resp_dict = json.loads(response.text)
            tag_list = resp_dict['data']['tags']
            list = []
            for tag in tag_list:
                t = tag['name'] + '(' + str(tag['num']) + ')'
                list.append(t)
            list_str = '#'.join(list)
        except Exception as e:
            self._logger.error(e)
            list_str=''
        return list_str



    def descrip_inter(self, productId):
        """
        商品描述信息 接口
        :param productId:
        :return:
        """
        try:
            url = 'http://product.dangdang.com/index.php?r=callback/detail&productId=%s&templateType=publish&describeMap=0100002899:1&shopId=0&categoryPath=01.03.35.02.00.00' % productId
            data = requests.get(url).json()
            html_str = data['data']['html']
            html = etree.HTML(html_str)
        except Exception as e:
            self._logger.error(e)
            html = ''
        return html