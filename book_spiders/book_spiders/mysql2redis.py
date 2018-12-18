import pymysql
from redis import StrictRedis
import datetime

from conf import Conf

config = Conf.config
redis = StrictRedis(host=config['redis']['host'], port=config['redis']['port'], password=config['redis']['password'], db= config['redis']['db'])
# redis = StrictRedis(host='127.0.0.1', port=6379, db= 1)
# db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'], user=config['mysql']['username'],
#                                 passwd=config['mysql']['password'], db=config['mysql']['dbname'], charset='utf8')
db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'], user=config['mysql']['username'],
                                passwd=config['mysql']['password'], db=config['mysql']['dbname'], charset='utf8')
cursor = db.cursor()



if __name__ == '__main__':
    print('开始时间'+str(datetime.datetime.now()))
    # 从mysql中读取数据
    # q_sql = "select url from info_backup where publisher like '%四川%'"
    # q_sql = "select startUrl from site_book limit 1000000,4000000"
    q_sql = "select url from web_page_p_book_toplist_09 where booktype = '02' and sourcetype = '05'"
    cursor.execute(q_sql)
    results = cursor.fetchall()
    for start_url in results:
        print(start_url[0])
        # 插入到redis
        redis.rpush('toplist_amazon_e:start_urls', start_url[0])
    print('结束时间'+str(datetime.datetime.now()))
