# 此脚本可以删除重复下载的图片。（单文件夹版本）
import os

from elasticsearch import Elasticsearch
from conf import Conf
from mylogger import Logger



def photo_is_exit(es,filename,dirname):
    """
    判断图片是否存在
    :param es:
    :param filename:
    :param dirname:
    :return:
    """
    body = {
        "query": {
            "term": {
                # "coverpath":"/book/20180921/100194542302.jpg"
                "coverpath": filename
            }
        }
    }
    result = es.search(index="web_page_p_book_info_09", doc_type="web_page_p_book_info_09", body=body)
    terms = result['hits']['hits']
    if len(terms) > 0:
        for t in terms:
            if dirname in t['_source']['coverpath']:
                # 存在此图片，日期也正确，返回false
                return True,'存在此图片'
            else:
                # 存在此图片，但日期不对，返回false
                return False,'reason:es中已找到，但日期不对'
    else:
        # 不存在此图片
        return False,'reason:es中无法找到'


config = Conf.config
_logger = Logger().getLogger()

es = Elasticsearch(config['elasticsearch']['hosts'])

directory = config['image']['path']+'/book/20181119'
os.chdir(directory)
cwd = os.getcwd()
# 列出此日期文件夹下的所有文件
files = os.listdir(cwd)
count = 0
# 遍历文件夹
_logger.info('当前目录：'+directory)
for file in files :
    if os.path.getsize(file) == 0:  # 获取文件大小
        os.remove(file)
        count = count + 1
        _logger.info("删除第 "+str(count)+" 个文件："+directory+'/'+file+'   '+'reason:空文件')
        continue
    # 判断此文件是否存在es中
    flag,reason = photo_is_exit(es,file,dirname='/book/20181012/'+file)
    if not flag:
        os.remove(file)  # 删除文件
        count = count + 1
        _logger.info("删除第 "+str(count)+" 个文件："+directory+'/'+file+'   '+reason)
    else:
        _logger.info("=============图片路径匹配成功："+directory+'/'+file+"=============")





