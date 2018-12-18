import datetime
import time

import pymysql
import requests
import re

from conf import Conf
from mylogger import Logger


# 中图分类查询
# 营销分类查询
class CategoryServer():
    # 授权链接
    auth_url = ''
    _logger = Logger().getLogger()
    proxies = {
        "http": ''  # 代理ip
    }

    def __init__(self):
        # self.reload_authURL()
        config = Conf.config
        #初始化数据库连接
        self.db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'],user=config['mysql']['username'],
                                  passwd=config['mysql']['password'], db=config['mysql']['dbname_cate'])
        self.cursor = self.db.cursor()
        self.db_sale = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'],user=config['mysql']['username'],
                                  passwd=config['mysql']['password'], db=config['mysql']['dbname_sale_cate'])
        self.cursor_sale = self.db_sale.cursor()


    def reload_authURL(self):
        """
        更换中图网站的授权链接
        :return:
        """
        try:
            proxy_ip = requests.get('http://api.ip.data5u.com/dynamic/get.html?order=f6d9a18f02f520f2aaac6b249fd8689e').content.decode().strip()
            self.proxies['http'] = proxy_ip
            url = 'http://opac.nlc.cn/F?RN=989462048'
            response = requests.get(url,timeout=20,proxies=self.proxies)
            html = response.text
            self.auth_url = re.findall('tmp="([^"]+)"',html)[0]
        except:
            self._logger.error('更换中图授权链接的时候出错')
            self.auth_url = 'http://opac.nlc.cn:80/F/IYKXX91A5NCBPEQP1DQHLF471L8ANIEHXUMSUTI2HLRRXI77MF-10964'



    def query_cate_server(self,isbn):
        """
        中图查询入口：先查book_isbn_cate表，有则return，无则再查中图网站，查到的中图分类再存进mysql
        :param isbn:
        :return:
        """
        # 先查询mysql是否有此isbn
        cate_code = self.query_cate_mysql(isbn)
        if cate_code:
            return cate_code
        # 更换授权链接
        try:
            self.reload_authURL()
        except Exception as e:
            self._logger.error(e)
        url = self.auth_url+'?func=find-b&find_code=ISB&request=%s&local_base=NLC01&filter_code_1=WLN&filter_request_1=&filter_code_2=WYR&filter_request_2=&filter_code_3=WYR&filter_request_3=&filter_code_4=WFM&filter_request_4=&filter_code_5=WSL&filter_request_5=' %isbn
        try:
             # 请求中图网站获取isbn对应的网页，进行解析
            response = requests.get(url,timeout=10,proxies=self.proxies)
            html = response.text
        except Exception as e:
            self._logger.error(e)
            html = ''
        cate_code = re.findall('CALL-NO:\s*?([^\r\n]*)',html)
        if not cate_code :
            self._logger.info('中图服务器查询查无此isbn：' + isbn)
            return ''
        cate_code = cate_code[0].strip()
        if not cate_code:
            self._logger.info('中图服务器查询查无此isbn：' + isbn)
            return ''
        self._logger.info('中图服务器查询========>isbn：'+isbn+'  分类为：'+cate_code)
        # 往数据库中插入新的中图分类
        self.insert_cate_mysql(isbn,cate_code)
        return cate_code


    def query_cate_mysql(self, isbn):
        """
        从mysql中查询中图分类
        :param isbn:
        :return:
        """
        sql = 'select category from book_isbn_cate where isbn = "%s" ' %isbn
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if not result:
            self._logger.info('中图数据库查无此isbn：' + isbn +'转为中图服务器查询')
            return None
        self._logger.info('中图数据库查询========>isbn：' + isbn + '  分类为：' + result[0])
        return result[0]

    def insert_cate_mysql(self,isbn,cate_code):
        """
        往数据库中插入中图分类
        :param isbn:
        :param cate_code: 中图分类号
        :return:
        """
        sql = 'insert into book_isbn_cate(isbn,category,savetime) values(%s,%s,%s)'

        now = datetime.datetime.now()
        params = (isbn,cate_code,now)
        self.cursor.execute(sql,params)
        self.db.commit()
        pass

    def query_sale_category(self,salecategory_name):
        """
        从mysql中查询营销分类
        :param salecategory_name:
        :return:
        """
        sql = 'select id from book_category_cate where name like "%'+salecategory_name+'%"'
        self.cursor_sale.execute(sql)
        result = self.cursor_sale.fetchone()
        if not result:
            self._logger.info('查无此营销分类========>salecategory_name：'+salecategory_name)
            return ''
        self._logger.info('查询营销分类========>salecategory_name：' + salecategory_name + '  ID为：' + result[0])
        return result[0]



if __name__ == '__main__':
    cate = CategoryServer()
    # isbn = '978754842791'
    # cate_code = cate.query_cate_mysql(isbn)
    # # 如果结果集为空，则去中图服务器查询
    # if not cate_code:
    #     # 查询到cate_code,并往数据库中添加此分类
    #     cate_code = cate.query_cate_server(isbn)
    resu = cate.query_cate_server('9787115403179')
    # print(resu)




