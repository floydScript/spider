# 此脚本可以删除重复下载的图片。（遍历所有文件夹版本）
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
            p = '/book/'+dirname +'/'+ filename
            if p in t['_source']['coverpath']:
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

directory = config['image']['path']+'/book'
os.chdir(directory)
cwd = os.getcwd()
# 列出book中所有的文件夹
dirs = os.listdir(cwd)
count = 0
# 遍历文件夹
for dir in dirs :
    path = directory+'/'+dir
    if os.path.isfile(path):
        continue
    # 选择当前文件夹
    os.chdir(path)
    cwd = os.getcwd()
    # 列出当前文件夹下所有的文件
    files = os.listdir(cwd)
    _logger.info('当前目录：'+path)
    for file in files :
        # 删除空文件
        if os.path.getsize(file) == 0:  # 获取文件大小
            os.remove(file)
            count = count + 1
            _logger.info("删除第 "+str(count)+" 个文件："+dir+'/'+file+'   '+'reason:空文件')
            continue
        # 判断此文件是否存在es中
        flag,reason = photo_is_exit(es,file,dirname=dir)
        if not flag:
            os.remove(file)  # 删除文件
            count = count + 1
            _logger.info("删除第 "+str(count)+" 个文件："+dir+'/'+file+'   '+reason)
        else:
            _logger.info("=============图片路径匹配成功："+dir+'/'+file+"=============")





