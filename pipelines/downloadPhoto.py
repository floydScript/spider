import os
import requests
from elasticsearch import Elasticsearch

from conf import Conf
from mylogger import Logger

_logger = Logger().getLogger()
config = Conf.config
# 从es中拿到路径coverpath和coverurl
es = Elasticsearch(config['elasticsearch']['hosts'])
for x in range(0,91):
    body = {
        "query":{
            "match_all":{}
        },
        "from": x * 10000,
        "size": 10000
    }
    result = es.search(index="web_page_p_book_info_09", doc_type="web_page_p_book_info_09", body=body)
    terms = result['hits']['hits']
    for t in terms:
        coverpath = t['_source']['coverpath']
        coverurl = t['_source']['coverurl']



def process_item(self, item):
    """
    保存图片
    :param item:
    :return:
    """
    if item['_entitycode'] == 'web_page_p_book_info_09':
        if item['is_set'] == '是':
            return item
        # 拼接图片路径 /opt/fhcb/fileserver/img + /book/20180909/2993702.jpg
        img_path = self.config['image']['path'] + item['coverpath']
        # 创建文件夹  /opt/fhcb/fileserver/img + /book/ + 20180909/
        filename = img_path.split('/')[-1]
        dir_path = img_path.replace(filename,'')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        # 从imgurl下载图片
        with open(img_path, 'wb') as f:
            resp = requests.get(item['coverurl'])
            f.write(resp.content)
            self._logger.info('下载图片到： ' + item['coverpath'])
    return item