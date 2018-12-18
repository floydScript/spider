import os
import re

import requests
from elasticsearch import Elasticsearch

from mylogger import Logger

_logger = Logger().getLogger()
es = Elasticsearch('10.13.11.21:9200')
index = 0
try:
    with open('zlog.text','r',encoding='utf-8') as file:
        lists = file.readlines()
        for l in lists:
            date = re.findall("个文件：(\d+)/\d+.jpg", l)
            _row = re.findall("个文件：\d+/(\d+).jpg", l)
            if _row:
                _row = _row[0]
                date = date[0]
                body = {
                    "query": {
                        "term": {
                            "_row": _row
                        }
                    }
                }
                result = es.search(index="web_page_p_book_info_09", doc_type="web_page_p_book_info_09", body=body)
                terms = result['hits']['hits']
                if terms:
                    term = terms[0]
                    coverurl = term['_source']['coverurl']
                    coverpath = term['_source']['coverpath']
                    if date in coverpath:
                        img_path = '/mount/fhcb/fileserver/img'+coverpath
                        filename = img_path.split('/')[-1]
                        dir_path = img_path.replace(filename, '')
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)
                        with open(img_path, 'wb') as f:
                            resp = requests.get(coverurl)
                            f.write(resp.content)
                            index = index + 1
                            _logger.info('================_row匹配成功下载第'+str(index)+'张图片到： ' + coverpath+ ' ================')
                    else:
                        _logger.info('_row匹配成功,但日期不对:'+coverpath)
                else:
                    _logger.info('es中没有找到此_row：' + _row)
                    continue
            else:
                continue
except:
    pass