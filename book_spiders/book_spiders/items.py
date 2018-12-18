# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BookItem(scrapy.Item):
    bookname = scrapy.Field()
    subhead = scrapy.Field()
    publisher = scrapy.Field()
    orgpublisher = scrapy.Field()
    contentsummary = scrapy.Field()
    sourcetype = scrapy.Field()
    author = scrapy.Field()
    translator = scrapy.Field()
    isbn = scrapy.Field()
    orgisbn = scrapy.Field()
    salecategory = scrapy.Field()
    category = scrapy.Field()
    orgcategory = scrapy.Field()
    contenttype = scrapy.Field()
    issuearea = scrapy.Field()
    type = scrapy.Field()
    edition = scrapy.Field()
    impression = scrapy.Field()
    words = scrapy.Field()
    pages = scrapy.Field()
    language = scrapy.Field()
    price = scrapy.Field()
    printedtime = scrapy.Field()
    format = scrapy.Field()
    papermeter = scrapy.Field()
    packing = scrapy.Field()
    coverurl = scrapy.Field()
    coverpath = scrapy.Field()
    seriename = scrapy.Field()
    catalog = scrapy.Field()
    editorsugest = scrapy.Field()
    usersugest = scrapy.Field()
    preface = scrapy.Field()
    summary = scrapy.Field()
    epilogue = scrapy.Field()
    publishdate = scrapy.Field()
    collectiontime = scrapy.Field()
    orgcode = scrapy.Field()
    skuid = scrapy.Field()
    commentcount = scrapy.Field()
    _row = scrapy.Field()
    is_set = scrapy.Field()
    ifimport = scrapy.Field()
    url = scrapy.Field()
    _entitycode = scrapy.Field()

    commentpercent = scrapy.Field()
    commenttag = scrapy.Field()
    authorintro = scrapy.Field()
    sourceprice = scrapy.Field()



class CommentItem(scrapy.Item):
    isbn = scrapy.Field()
    uri = scrapy.Field()
    bookname = scrapy.Field()
    sourcetype = scrapy.Field()
    collectiontime = scrapy.Field()
    publishtime = scrapy.Field()
    username = scrapy.Field()
    hitcount = scrapy.Field()
    follownum = scrapy.Field()
    suportnum = scrapy.Field()
    opposnum = scrapy.Field()
    commentid = scrapy.Field()
    followcommentid = scrapy.Field()
    commenttitle = scrapy.Field()
    commenttype = scrapy.Field()
    comment = scrapy.Field()
    score = scrapy.Field()
    level = scrapy.Field()
    commpoint = scrapy.Field()
    type = scrapy.Field()
    sitename = scrapy.Field()

    _row = scrapy.Field()
    _entitycode = scrapy.Field()
    skuid = scrapy.Field()


