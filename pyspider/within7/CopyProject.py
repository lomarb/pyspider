# 每次启动项目，将代码拷贝一份作为启动项目的执行代码
#
import json
import time
import gzip
import boto3
import tornado.ioloop
import tornado.httpclient
from pyspider.database.mongodb.projectdb import ProjectDB
from pyspider.database.mongodb.resultdb import ResultDB
from pyspider.database.mongodb.taskdb import TaskDB
import redis
import configparser

bucket_name = 'test-ypp0711-lambda-bucket'  # S3桶的名称


# 获取每个媒体下面的需要替换的代码
class ReplaceProject:
    media_dict_arr = {
        "ins": [],
        "twitter": [
            'ScrapingTwitterPostsByTagsV001'
        ],
        "facebook": [
            'ScrapingFacebookPageIdBykeywords',
            'ScrapingFacebookAdsByPageIdV001',
        ],
        "google": [
            'ScrapingGoogleIndexDispatcher',
            'ScrapingGoogleIndex'
        ],
        "tiktok": [
            'ScrapingTikTokPostsByCharles',
            'ScrapingTikTokUserInfoByUniqueId',
            'ScrapingTikTokUserPostsByUniqueId',
        ],
        "reddit": [
            'ScrapingRedditPostsByKeywords',
        ],
        "youtube": [
            'ScrapingYoutubeVideosByKeywordsV001',
            'ScrapingYoutubeVideoDetailsByVideoId',
            'ScrapingYoutubeChannelAboutByChannelUrl'
        ],
    }

    youtube = media_dict_arr.get('youtube', [])
    twitter = media_dict_arr.get('twitter', [])
    facebook = media_dict_arr.get('facebook', [])
    google = media_dict_arr.get('google', [])
    tiktok = media_dict_arr.get('tiktok', [])
    reddit = media_dict_arr.get('reddit', [])

    def __init__(self):
        pass

    def get_media(self, media):
        return self.media_dict_arr.get(media, [])


rp_project = ReplaceProject()

try:
    config = configparser.ConfigParser()
    config.read('/opt/pyspider/key.config')
    db_name = config['DB']['db_name']
    db_pass = config['DB']['db_pass']
except:
    db_name = ''
    db_pass = ''


class CopyProject:

    def __init__(self):
        url = f'mongodb://{db_name}:{db_pass}@3.134.227.240/projectdb?authSource=admin'
        self.db = ProjectDB(url, database='projectdb')
        self.result_db = ResultDB(url)
        self.task_db = TaskDB(url)

        REDIS_URL = 'redis://3.134.227.240:6379/{db}'
        self.rd_db = redis.Redis.from_url(REDIS_URL.format(db=6), decode_responses=True, encoding='utf-8')

    # 任务存入redis p_name为项目名称 {"ins": "key1,key2", "tw": "key1", "tk": "key1"}
    def create_new_project(self, p_name, keywords_dict):
        self.rd_db.hset('project:task', p_name, keywords_dict)
        # self.rd_db.hget('project:task', 'www')

    # 查询任务进行情况
    def find_task_process(self, collection_name):
        cursor = list(self.task_db.database[collection_name].find())
        results = []
        documents = [doc for doc in cursor]
        for doc in documents:
            doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            results.append(doc)

        return results

    # object_name 存路径
    def save_result_to_s3(self, db, collection_name, project, a_key, s_key):
        # 保存在S3中的对象名称（通常以.gz结尾）
        object_name = f'resultDB/ods/{project}/{collection_name.split("_")[0]}/data.json.gz'
        cursor = list(db[collection_name].find())
        print('data', len(cursor), cursor)
        res_str = ''
        # 将数据转换为JSON字符串
        documents = [doc for doc in cursor]
        for doc in documents:
            doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            res_str += json.dumps(doc, default=str) + '\n'
        # Serialize the list of documents to JSON
        # json_string = json.dumps(documents, default=str)

        # 使用gzip进行压缩
        # compressed_data = gzip.compress(json_string.encode('utf-8'))
        compressed_data = gzip.compress(res_str.encode('utf-8'))

        # 将压缩后的数据上传到S3
        s3_client = boto3.client('s3', aws_access_key_id=a_key, aws_secret_access_key=s_key)
        try:
            response = s3_client.put_object(Body=compressed_data, Bucket=bucket_name, Key=object_name)
            print('response', response)
            print(f"数据已成功打包并上传到S3桶 {bucket_name} 中，保存为对象 {object_name}")
            del_drop_result = self.result_db.database[collection_name].drop()
            self.drop_project_by_name(collection_name)
            del_drop_task = self.task_db.database[collection_name].drop()
            return {"del_drop_result": del_drop_result, "del_drop_task": del_drop_task, "upload": response}
        except Exception as e:
            print(f"上传数据到S3时出现错误：{e}")
            return str(e)

    @staticmethod
    def replace_script(script, p_name, media):
        media_arr = rp_project.get_media(media)
        for s in media_arr:
            script = script.replace(f'class {s}(BaseHandler)', f'class {s}_{p_name}(BaseHandler)')
            script = script.replace(f"self.send_message('{s}'", f'self.send_message("{s}_{p_name}"')
            script = script.replace(f'self.send_message("{s}"', f'self.send_message("{s}_{p_name}"')
            script = script.replace(f'"{s}"', f'"{s}_{p_name}"')
            script = script.replace(f"'{s}'", f"'{s}_{p_name}'")
        if len(media_arr) == 0:
            script = f'# error:{media}-{p_name}'

        return script

    # 准备拷贝新的项目
    def ready_project(self, p_name, media=None):
        media_arr = rp_project.youtube

        if media:
            media_arr = rp_project.get_media(media)

        results = []
        for m in media_arr:
            temp = self.start_copy(f"{m}_{p_name}", media)
            results.append(temp)
        return results

    # 清除项目
    def drop_project_by_name(self, name):
        return self.db.collection.remove({'name': name})

    # 清除完成的任务
    def drop_project(self, name):
        return self.db.collection.remove({'temp_name': name})

    # 查询当前所有项目
    def get_distinct_project(self):
        return self.db.collection.distinct('temp_name')

    # 查询当前项目下面的爬虫文件
    def get_project_by_project(self, temp_name):
        data_list = list(self.db.collection.find({"temp_name": temp_name}))
        res_list = []
        for doc in data_list:
            doc['_id'] = str(doc['_id'])
            res_list.append(doc)
        return res_list

    # 拷贝项目
    def start_copy(self, project_name, media):
        # self.collection['']
        project, p_name = project_name.split('_')
        pipeline = [
            # {"$match": {"result.keyword":{"$eq":"@Frontrunneroutfitters"}}},
            {"$match": {"result.name": {"$eq": project}}},
        ]
        # tk_code = self.db.collection.aggregate(pipeline)
        # print(list(tk_code))
        # return list(tk_code)
        cpdb = self.db.get(project)
        print(cpdb, type(cpdb))
        # return cpdb
        script = cpdb['script']
        cpdb['script'] = self.replace_script(script, p_name, media)
        cpdb['temp_name'] = p_name
        cpdb['group'] = p_name
        cpdb['updatetime'] = time.time()
        cpdb['status'] = 'TODO'
        cpdb['name'] = project_name
        insert_res = self.db.collection.update_one({"name": project_name}, {"$set": cpdb}, upsert=True)
        return {'count': insert_res.modified_count, 'res': str(insert_res.raw_result)}

    def update_project_status(self, project, value):
        name = 'status'
        # value = 'DEBUG'  # TODO DEBUG RUNNING CHECKING
        update = {
            name: value
        }
        ret = self.db.update(project, update)
        return {'count': str(ret)}

    # 查询任务状态
    def get_task_status(self, task_db, db_name):
        cursor = task_db[db_name].find({'url': {'$regex': 'data:,on'}})
        documents = [doc for doc in cursor]
        for doc in documents:
            doc['_id'] = str(doc['_id'])  # Convert ObjectId to string

        return documents

    # 查询拿到数据的项目
    def get_db_list(self, project, db_name='result'):
        db_task = []
        db_dict = {
            "result": self.result_db,
            "task": self.task_db,
        }
        db = db_dict.get(db_name, None)
        if db is None:
            return []

        for media_key in rp_project.media_dict_arr:
            collection = rp_project.media_dict_arr[media_key]
            for c in collection:
                collection_name = f"{c}_{project}"
                if collection_name in db.database.list_collection_names():
                    print(f"'{collection_name}' exists.")
                    db_task.append(f"'{collection_name}' exists.")
                else:
                    print(f"'{collection_name}' not exists.")
                    db_task.append(f"'{collection_name}' not exists.")
        return db_task

    def notify_status_to_s3(self):
        # 创建一个 Step Functions 客户端
        client = boto3.client('stepfunctions')
        # 定义输入参数
        input_params = {
            'project': 'ridge',
            'tables': 'ScrapingFacebookAdsByPageIdV001,ScrapingFacebookPageIdBykeywords,ScrapingRedditPostsByKeywords,ScrapingTwitterPostsByTagsV001,ScrapingYoutubeChannelAboutByChannelUrl,ScrapingYoutubeVideoDetailsByVideoId,ScrapingYoutubeVideosByKeywordsV001'
        }
        # 将参数转换为 JSON 格式
        input_str = json.dumps(input_params)
        # 启动新的状态机执行
        response = client.start_execution(
            stateMachineArn='arn:aws:states:us-east-2:080794739569:stateMachine:LambdaStateMachine',
            input=input_str
        )
        # 打印出响应
        print(response)

    # 查询爬取结果数据
    def query_result_data(self):
        pass


def send_request(url, method='GET', headers=None, data=None):
    http_client = tornado.httpclient.HTTPClient()
    res_data = None
    err = None
    try:
        request = tornado.httpclient.HTTPRequest(
            url=url,
            method=method,
            headers=headers,
            body=data
        )
        res_data = http_client.fetch(request)
    except tornado.httpclient.HTTPError as e:
        print("Error:", e)
        err = e
    except Exception as e:
        print("Error:", e)
        err = e
    finally:
        http_client.close()
        if res_data is not None:
            return res_data.body
        else:
            return json.dumps({"err": str(err)})


def fetch_url(url):
    http_client = tornado.httpclient.HTTPClient()
    res_data = None
    err = None
    try:
        response = http_client.fetch(url)
        # response.body
        res_data = response
    except tornado.httpclient.HTTPError as e:
        print("Error:", e)
        err = e
    except Exception as e:
        print("Error:", e)
        err = e
    finally:
        http_client.close()
        if res_data is not None:
            return res_data.body
        else:
            return str(err)
