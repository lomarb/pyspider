#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-02-22 23:20:39

import socket
import hashlib
import redis
import requests
import six
from six import iteritems, itervalues
from flask import render_template, request, json
from pyspider.within7.CopyProject import CopyProject, fetch_url, send_request
import time
import configparser

try:
    import flask_login as login
except ImportError:
    from flask.ext import login

from .app import app

index_fields = ['name', 'group', 'status', 'comments', 'rate', 'burst', 'updatetime']
md5string = lambda x: hashlib.md5(utf8(x)).hexdigest()

js_host = 'http://3.15.15.192:3000'

# # 创建一个配置文件解析器对象

# redis_client = redis.StrictRedis(host='172.26.7.16', port=6379, db=5)
REDIS_URL = f'redis://172.26.7.16:6379/5'
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, encoding='utf-8')


def utf8(string):
    """
    Make sure string is utf8 encoded bytes.

    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if isinstance(string, six.text_type):
        return string.encode('utf8')
    elif isinstance(string, six.binary_type):
        return string
    else:
        return six.text_type(string).encode('utf8')


@app.route('/')
def index():
    projectdb = app.config['projectdb']
    projects = sorted(projectdb.get_all(fields=index_fields),
                      key=lambda k: (0 if k['group'] else 1, k['group'] or '', k['name']))
    return render_template("index.html", projects=projects)


# @vue3_blueprint.route('/vue3')
# def admin():
#     return render_template("index.html")

@app.route('/admin')
def admin():
    return render_template("admin.html")


@app.route('/copy')
def copy():
    return render_template("copy.html")


# 获取user信息，outh2.0授权
def get_user_info(code):
    app_token = get_feishu_token(token_name='app_token')
    if app_token is None:
        return json.dumps({'err': '获取失败'})

    url = "https://open.feishu.cn/open-apis/authen/v1/access_token"

    payload = json.dumps({
        "code": code,
        "grant_type": "authorization_code"
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {app_token}',
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text


def get_feishu_key():
    feishu_resultdb = app.config['resultdb']
    feishu_results = list(feishu_resultdb.select('FeishuInfo'))
    app_id = feishu_results[0]['result']['app_id']
    app_secret = feishu_results[0]['result']['app_secret']

    return app_id, app_secret


# 获取飞书 app token 和 tenant token
def get_feishu_token(token_name='token'):
    old_token = redis_client.get(f'feishu:{token_name}')
    if old_token is not None:
        return old_token
    app_id = get_feishu_key()
    app_secret = get_feishu_key()
    payload = json.dumps({
        "app_id": app_id,
        "app_secret": app_secret,
    })
    return {
        "app_id": app_id,
        "app_secret": app_secret,
    }

    headers = {
        'Content-Type': 'application/json; charset=utf-8'
    }
    url = f"https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    response = requests.request("post", url, headers=headers, data=payload)
    # res = requests.request(method='post',
    #                        url=f'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal?timestamp={int(time.time() * 1000)}',
    #                        headers=headers, data=payload)
    token_info = response.json()
    if token_info['code'] != 0:
        return None
    redis_client.set(f'feishu:token', token_info['tenant_access_token'])
    redis_client.set(f'feishu:app_token', token_info['app_access_token'])
    redis_client.expire(f'feishu:token', token_info['expire'] - 500)
    redis_client.expire(f'feishu:app_token', token_info['expire'] - 500)
    return redis_client.get(f'feishu:{token_name}')


# 获取飞书的任务
def get_feishu_task():
    # token和table是固定d的
    token = 'bascn92h2DxjIZom4hsB1U9irLc'
    table = 'tblBBtyYcEtpS7h2'
    view_id = 'vewqaxH2xy'
    filter_condi = 'filter=CurrentValue.%5B%E6%95%B0%E6%8D%AE%E6%8A%93%E5%8F%96%E7%8A%B6%E6%80%81%5D+%3D%22%E7%AD%89%E5%BE%85%E5%A4%84%E7%90%86%22'
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{token}/tables/{table}/records?&page_size=200&view_id={view_id}"
    payload = ''
    u_token = get_feishu_token()
    if u_token is None:
        return {'err': 'token获取失败'}

    headers = {
        'Authorization': f'Bearer {u_token}',
        'Content-Type': 'application/json',
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    print('data', data)
    tmp_set = {}  # 所有的媒体
    for item in data['data']['items']:
        one = item['fields']
        task = one['任务类型']
        all_keys = item['fields'].keys()

        if tmp_set.get(task) is None:
            tmp_set[task] = []

        for key in all_keys:
            if key.startswith('关键词'):
                tmp_set[task].append(one[key])
    return tmp_set


# 获取项目名下所有爬虫文件
@app.route('/get_feishu_test')
def get_feishu_test():
    resultdb = app.config['resultdb']
    # offset = int(request.values.get('offset', 0))
    # limit = int(request.values.get('limit', 20))
    # fields = json.loads(request.values.get('fields', '{}')) or None
    # filter = json.loads(request.values.get('filter', '{}')) or None
    results = list(resultdb.select('FeishuInfo'))
    return json.dumps({"res": results}), 200, {'Content-Type': 'application/json'}


# 获取飞书抓取任务
@app.route('/get_feishu_spider')
def get_feishu_spider():
    media = request.args.get('media', "")  # 数据渠道

    task_info = get_feishu_task()

    return json.dumps(task_info)


# 飞书获取用户
@app.route('/get_feishu_user')
def get_feishu_user():
    code = request.args.get('code', "")
    if code == '':
        return '{"err":"参数不能为空"}'
    user_info = get_user_info(code)
    return user_info


# 飞书相关接口
@app.route('/get_feishu_app_token')
def get_feishu_app_token():
    token = get_feishu_token()
    return json.dumps({"token": token})


# 飞书相关接口
@app.route('/get_feishu_old_token')
def get_feishu_old_token():
    app_id = get_feishu_key()
    app_secret = get_feishu_key()

    data = json.dumps({
        "app_id": app_id,
        "app_secret": app_secret,
    })
    headers = {
        'Content-Type': 'application/json'
    }
    res = send_request(
        f'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal?timestamp={int(time.time() * 1000)}',
        method='POST',
        headers=headers,
        data=data)
    try:
        return res.decode('utf-8')
    except Exception as e:
        return e


@app.route('/get_feishu_excel')
def get_feishu_excel():
    token = request.args.get('token', "")
    sheet_token = request.args.get('sheetToken', "")
    sheet_ranges = request.args.get('sheetIDStr', "")
    headers = {
        'Authorization': f'Bearer {token}',
    }
    res = send_request(
        f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values_batch_get?ranges={sheet_ranges}&valueRenderOption=ToString&dateTimeRenderOption=FormattedString',
        headers=headers)
    return res


@app.route('/db_name')
def db_name():
    start_cp = CopyProject()

    project = request.args.get('project', "")
    media = request.args.get('media', "")
    # result = start_cp.start_copy('ScrapingTikTokPostsByCharles')
    result = start_cp.start_copy(project, media)
    return json.dumps(result), 200, {'Content-Type': 'application/json'}


@app.route('/start_new_project')
def start_new_project():
    start_cp = CopyProject()
    # result = start_cp.start_copy('ScrapingTikTokPostsByCharles')

    project_name = request.args.get('project_name', "")
    media = request.args.get('media', "")

    if media is None or media == '':
        return json.dumps({"msg": "请传入media", "code": 101}), 200, {'Content-Type': 'application/json'}

    result = start_cp.ready_project(project_name, media)
    return json.dumps(result), 200, {'Content-Type': 'application/json'}


# 删除项目表
@app.route('/del_project/<project>')
def del_project(project):
    start_cp = CopyProject()
    # result = start_cp.start_copy('ScrapingTikTokPostsByCharles')

    result = start_cp.drop_project(project)
    return json.dumps({"res": str(result)}), 200, {'Content-Type': 'application/json'}


# 获取所有项目
@app.route('/all_project')
def all_project():
    start_cp = CopyProject()
    result = start_cp.get_distinct_project()
    return json.dumps({"res": result}), 200, {'Content-Type': 'application/json'}


# 获取项目名下所有爬虫文件
@app.route('/get_project/<project>')
def get_project(project):
    start_cp = CopyProject()
    result = start_cp.get_project_by_project(project)
    return json.dumps({"res": result}), 200, {'Content-Type': 'application/json'}


# 获取项目名下所有爬虫文件
@app.route('/get_db_list')
def get_db_list():
    start_cp = CopyProject()
    project = request.args.get('project', "")
    db = request.args.get('db', "result")

    result = start_cp.get_db_list(project, db_name=db)
    return json.dumps({"res": result}), 200, {'Content-Type': 'application/json'}


# 获取项目名下所有爬虫文件
@app.route('/query_task_status')
def query_task_status():
    # task_db = app.config['taskdb']
    project = request.args.get('project', "")
    if project == '':
        return json.dumps({"msg": '不要传空值'}), 200, {'Content-Type': 'application/json'}

    start_cp = CopyProject()
    result = start_cp.get_task_status(start_cp.task_db.database, project)
    return json.dumps({"res": result}), 200, {'Content-Type': 'application/json'}


# 接收处理aws的sns订阅消息
@app.route('/aws_sns', methods=['POST', ])
def aws_sns():
    # value = request.get_data()
    # SubscribeURL = request.form['SubscribeURL']
    data = request.form
    # print('val', value.decode(), type(data))
    SubscribeURL = data.get('SubscribeURL')
    payload = json.dumps({
        "msg_type": "text",
        "content": {
            "text": request.get_data().decode()
            # "text": str(data.to_dict(flat=False))
        }
    })
    headers = {
        'Content-Type': 'application/json'
    }
    send_request(
        'https://open.feishu.cn/open-apis/bot/v2/hook/2ce3ca72-b1ca-4373-b47a-3136f6fd6e82',
        method='POST',
        headers=headers,
        data=payload)

    return json.dumps({"result": data.to_dict(flat=False), "message": str(data), "subUrl": SubscribeURL}), 200, {
        'Content-Type': 'application/json'}


# 获取项目名下所有爬虫文件
@app.route('/pack_data_to_s3')
def pack_data_to_s3():
    # result_db = app.config['resultdb']
    project = request.args.get('project', "")
    collection_name = request.args.get('collection_name', "")
    a_key = request.args.get('a_key', "")
    s_key = request.args.get('s_key', "")

    if project == '' or collection_name == '':
        return json.dumps({"msg": '不要传空值'}), 200, {'Content-Type': 'application/json'}

    start_cp = CopyProject()
    result = start_cp.save_result_to_s3(start_cp.result_db.database, collection_name, project, a_key, s_key)

    return json.dumps({"res": result}), 200, {'Content-Type': 'application/json'}


# ------------------------------------- 按项目抓取结束

@app.route('/queues')
def get_queues():
    def try_get_qsize(queue):
        if queue is None:
            return 'None'
        try:
            return queue.qsize()
        except Exception as e:
            return "%r" % e

    result = {}
    queues = app.config.get('queues', {})
    for key in queues:
        result[key] = try_get_qsize(queues[key])
    return json.dumps(result), 200, {'Content-Type': 'application/json'}


@app.route('/update', methods=['POST', ])
def project_update():
    projectdb = app.config['projectdb']
    project = request.form['pk']
    name = request.form['name']
    value = request.form['value']

    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response

    if name not in ('group', 'status', 'rate'):
        return 'unknown field: %s' % name, 400
    if name == 'rate':
        value = value.split('/')
        if len(value) != 2:
            return 'format error: rate/burst', 400
        rate = float(value[0])
        burst = float(value[1])
        update = {
            'rate': min(rate, app.config.get('max_rate', rate)),
            'burst': min(burst, app.config.get('max_burst', burst)),
        }
    else:
        update = {
            name: value
        }

    ret = projectdb.update(project, update)
    if ret:
        rpc = app.config['scheduler_rpc']
        if rpc is not None:
            try:
                rpc.update_project()
            except socket.error as e:
                app.logger.warning('connect to scheduler rpc error: %r', e)
                return 'rpc error', 200
        return 'ok', 200
    else:
        app.logger.warning("[webui index] projectdb.update() error - res: {}".format(ret))
        return 'update error', 500


@app.route('/counter')
def counter():
    rpc = app.config['scheduler_rpc']
    if rpc is None:
        return json.dumps({})

    result = {}
    try:
        data = rpc.webui_update()
        for type, counters in iteritems(data['counter']):
            for project, counter in iteritems(counters):
                result.setdefault(project, {})[type] = counter
        for project, paused in iteritems(data['pause_status']):
            result.setdefault(project, {})['paused'] = paused
    except socket.error as e:
        app.logger.warning('connect to scheduler rpc error: %r', e)
        return json.dumps({}), 200, {'Content-Type': 'application/json'}

    return json.dumps(result), 200, {'Content-Type': 'application/json'}


@app.route('/test', methods=['POST', ])
def test():
    value = request.form['value']
    return f'ok:{value}', 200


@app.route('/dispatcher', methods=['POST', ])
def dispatchertask():
    rpc = app.config['scheduler_rpc']
    if rpc is None:
        return json.dumps({})

    projectdb = app.config['projectdb']

    project = request.form['project']  # 项目文件名称
    key = request.form['key']  # 不同项目需要传入的参数
    keyword = request.form['keyword']  # 传入的搜索词
    url = request.form['url']  # 传入的url唯一值

    if not project:
        return "no such request project.", 404

    if not url:
        return "no such request url.", 404

    if not key:
        return "no such request key.", 404

    if not keyword:
        return "no such request keyword.", 404

    message = {
        key: keyword
    }

    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response

    # newtask = {
    #     # "taskid": md5string(url),
    #     'taskid': md5string('data:,on_message'),
    #     "project": project,
    #     # "url": url,
    #     'url': 'data:,on_message',
    #     "fetch": {
    #         "save": ('__command__', message),
    #     },
    #     "process": {
    #         "callback": "_on_message",
    #     },
    #     # "schedule": {
    #     #     "age": 0,
    #     #     "priority": 9,
    #     #     "force_update": True,
    #     # },
    # }

    sendtask = {
        'taskid': md5string('data:,on_message'),
        'project': project,
        'url': 'data:,on_message',
        'fetch': {
            'save': ('__command__', message),
        },
        'process': {
            'callback': '_on_message',
        }
    }

    try:
        # send_task
        # ret = rpc.newtask(sendtask)
        ret = rpc.send_task(sendtask)
    except socket.error as e:
        app.logger.warning('connect to scheduler rpc error: %r', e)
        return json.dumps({"result": False}), 200, {'Content-Type': 'application/json'}
    return json.dumps({"result": ret}), 200, {'Content-Type': 'application/json'}


@app.route('/run', methods=['POST', ])
def runtask():
    rpc = app.config['scheduler_rpc']
    if rpc is None:
        return json.dumps({})

    projectdb = app.config['projectdb']
    project = request.form['project']
    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response

    newtask = {
        "project": project,
        "taskid": "on_start",
        "url": "data:,on_start",
        "process": {
            "callback": "on_start",
        },
        "schedule": {
            "age": 0,
            "priority": 9,
            "force_update": True,
        },
    }

    try:
        ret = rpc.newtask(newtask)
    except socket.error as e:
        app.logger.warning('connect to scheduler rpc error: %r', e)
        return json.dumps({"result": False}), 200, {'Content-Type': 'application/json'}
    return json.dumps({"result": ret}), 200, {'Content-Type': 'application/json'}


@app.route('/robots.txt')
def robots():
    return """User-agent: *
Disallow: /
Allow: /$
Allow: /debug
Disallow: /debug/*?taskid=*
""", 200, {'Content-Type': 'text/plain'}
