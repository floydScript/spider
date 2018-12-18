import pymysql

from conf import Conf
from mylogger import Logger


class OrgcoderServer():

    _logger = Logger().getLogger()

    def __init__(self):
        config = Conf.config
        # 初始化数据库连接
        self.db = pymysql.connect(host=config['mysql']['host'], port=config['mysql']['port'],
                                  user=config['mysql']['username'],
                                  passwd=config['mysql']['password'], db=config['mysql']['dbname_org'])
        self.cursor = self.db.cursor()

    def query_orgcode(self,organization_name):
        """
        从mysql中查询出版社编码
        :param organization_name: 出版社名字
        :return:
        """
        sql = 'select organization_code from 09_org_collect_sys where organization_name = "'+organization_name+'"'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if not result:
            self._logger.info('查无此出版社========>publisher：' + organization_name)
            return ''
        self._logger.info('出版社查询========>publisher：' + organization_name + '  orgcode为：' + result[0])
        return result[0]

if __name__ == '__main__':
    result = OrgcoderServer().query_orgcode('四川人民出版社')
    print(result)