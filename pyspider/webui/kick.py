#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-07-16 15:30:57

import pymongo, logging
from flask import request, json

from .app import app

myLogger = logging.getLogger('handler_screen')
__DB = pymongo.MongoClient("mongodb://root:8a2p9j3x9g@172.26.11.184:27017/")["resultdb"]["Django_Websites"]
__RESULTDB = pymongo.MongoClient("mongodb://root:8a2p9j3x9g@172.26.11.184:27017/")["resultdb"]["Brand_website_test"]


@app.route('/kick/keywords', methods=['GET', 'POST'])
def keywords():
    # GET查询!
    if request.method == 'GET':
        result = {}
        try:
            json_keywords = json.loads(request.args.get('keywords', '[]')) or []
            for keyword in json_keywords:
                keyword = keyword.strip()
                query_list = list(__DB.find({'keywords': keyword}))
                if len(query_list) == 0:
                    result[keyword] = "None"
                else:
                    status = query_list[0]['status']
                    result[keyword] = status
            return json.dumps(result), 200, {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Method': '*',
                'Access-Control-Allow-Headers': 'x-requested-with,content-type',
                'Content-Type': 'application/json'
            }
        except Exception as error:
            myLogger.info('=======================%s====================' % (error))
            return 'GET数据有误! 请提交正确的JSON格式 eg["关键词1","关键词2","关键词3"]'
    else:
        # POST提交!
        result = {}
        json_keywords = json.loads(request.data)['keywords'] or []
        try:
            for keyword in json_keywords:
                keyword = keyword.strip()
                query_list = list(__DB.find({'keywords': keyword}))
                if len(query_list) == 0:
                    print("===========%s 创建中!===========" % (keyword))
                    __DB.insert_one({'keywords': keyword, 'status': 0})
                    result[keyword] = 0
                else:
                    print("===========%s 已存在!===========" % (keyword))
                    status = query_list[0]['status']
                    result[keyword] = status
            return json.dumps(result), 200, {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Method': '*',
                'Access-Control-Allow-Headers': 'x-requested-with,content-type',
                'Content-Type': 'application/json'
            }
        except Exception as error:
            myLogger.info('=======================%s====================' % (error))
            return 'POST数据有误! 请提交正确的JSON格式 eg["关键词1","关键词2","关键词3"]'

@app.route('/kick/websites', methods=['GET', 'POST'])
def websites():
    # GET查询!
    result = {}
    if request.method == 'GET':
        try:
            json_keywords = json.loads(request.args.get('keywords', '[]')) or []
            for keyword in json_keywords:
                keyword = keyword.strip()
                websites = []
                for item in list(__RESULTDB.find({'result.keywords': keyword}, {"_id": 0, 'result.domain': 1})):
                    websites.append(item["result"]["domain"])
                result[keyword] = websites
            return json.dumps(result), 200, {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Method': '*',
                'Access-Control-Allow-Headers': 'x-requested-with,content-type',
                'Content-Type': 'application/json'
            }
        except Exception as error:
            myLogger.info('=======================%s====================' % (error))
            return 'GET数据有误! 请提交正确的JSON格式 eg["关键词1","关键词2","关键词3"]'

@app.route('/kick/stats', methods=['GET', 'POST'])
def stats():
    result = {}
    keywords = []
    if request.method == 'GET':
        try:
            limit = int(request.args.get('limit', 0)) or 0
            offset = int(request.args.get('offset', 0)) or 0
            status = int(request.args.get('status', 0)) or 0
            myLogger.info('%i, %i, %i' % (limit, offset, status))
            total = __DB.count_documents({})
            pending = __DB.count_documents({"status": 0})
            running = __DB.count_documents({"status": 1})
            done = __DB.count_documents({"status": 2})
            result['total'] = total
            result['pending'] = pending
            result['running'] = running
            result['done'] = done
            for item in __DB.find({"status": status}, skip=offset, limit=limit):
                keywords.append(item['keywords'])
            result["keywords"] = keywords
            return json.dumps(result), 200, {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Method': '*',
                'Access-Control-Allow-Headers': 'x-requested-with,content-type',
                'Content-Type': 'application/json'
            }
        except Exception as error:
            myLogger.info(error)
            return error
