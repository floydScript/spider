import os
import requests
from elasticsearch import Elasticsearch
from conf import Conf
from mylogger import Logger

_logger = Logger().getLogger()
config = Conf.config
es = Elasticsearch(config['elasticsearch']['hosts'])
date = '20180928'
body = {
    "query": {
        "term": {
            "coverpath": date
        }
    }
}
result = es.search(index="web_page_p_book_info_09", doc_type="web_page_p_book_info_09", body=body)
index = 0
terms = result['hits']['hits']
_logger.info('日期 '+date+' 的图书共有'+str(len(terms))+'本。')
for item in terms:
    term = terms[0]
    coverurl = term['_source']['coverurl']
    coverpath = term['_source']['coverpath']
    img_path = config['image']['path']+coverpath
    filename = img_path.split('/')[-1]
    dir_path = img_path.replace(filename, '')
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(img_path, 'wb') as f:
        resp = requests.get(coverurl)
        f.write(resp.content)
        index = index + 1
        _logger.info('================_row匹配成功下载第'+str(index)+'张图片到： ' + coverpath+ ' ================')


