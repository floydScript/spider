import codecs
import requests
from lxml import etree
from redis import StrictRedis
from book_spiders.conf import Conf
from book_spiders.mylogger import Logger


class Pub_url_spider():

    _logger = Logger().getLogger()

    def __init__(self):
        config = Conf.config
        self.redis = StrictRedis(host=config['redis']['host'], port=config['redis']['port'],
                            password=config['redis']['password'], db=config['redis']['db'])

    def parse_url(self,start_url = 'http://search.dangdang.com/?key=机械工业出版社&act=input'):
        resp = requests.get(start_url)
        resp = etree.HTML(resp.text)
        url_list = resp.xpath("//div[@class='filtrate_box clearfix']/ul/li/div[@class='list_right']//span/a/@href")
        if url_list:
            for url in url_list:
                url_str = 'http://search.dangdang.com'+url
                self.parse_url(url_str)
        else:
            url_list = resp.xpath("//p[@class='name']/a/@href")
            for url in url_list:
                self.redis.rpush('publisher:start_urls', url)
                self._logger.info('publisher:start_urls 插入任务：'+url)
            next_url = resp.xpath("//li[@class='next']/a/@href")
            if next_url:
                next_url = 'http://search.dangdang.com' + next_url[0]
                self.parse_url(next_url)


if __name__ =='__main__':
    spider = Pub_url_spider()
    spider.parse_url()




        