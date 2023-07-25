# 每次启动项目，将代码拷贝一份作为启动项目的执行代码
#
import json
import time
import gzip
import boto3

from pyspider.database.mongodb.projectdb import ProjectDB
from pyspider.database.mongodb.resultdb import ResultDB


class CopyProject:

    def __init__(self):
        url = 'mongodb://root:8a2p9j3x9g@3.134.227.240/projectdb?authSource=admin'
        self.db = ProjectDB(url, database='projectdb')
        self.result_db = ResultDB(url)

    def save_result_to_s3(self, collection_name, object_name, keyword):
        cursor = list(self.result_db.database[collection_name].find())
        # data = list(collection.aggregate(pipeline))
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
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        try:
            response = s3_client.put_object(Body=compressed_data, Bucket=bucket_name, Key=object_name)
            print('response', response)
            print(f"数据已成功打包并上传到S3桶 {bucket_name} 中，保存为对象 {object_name}")
        except Exception as e:
            print(f"上传数据到S3时出现错误：{e}")


    @staticmethod
    def replace_script(script, p_name):
        origin_script_arr = [
            # 'ScrapingTikTokPostsByCharles',
            # 'ScrapingTikTokUserInfoByUniqueId',
            # 'ScrapingTikTokUserPostsByUniqueId',

            'ScrapingYoutubeVideosByKeywordsV001',
            'ScrapingYoutubeVideoDetailsByVideoId',
            'ScrapingYoutubeChannelAboutByChannelUrl'
        ]
        for s in origin_script_arr:
            script = script.replace(f'class {s}(BaseHandler)', f'class {s}_{p_name}(BaseHandler)')
            script = script.replace(f"self.send_message('{s}'", f'self.send_message("{s}_{p_name}"')
            script = script.replace(f'self.send_message("{s}"', f'self.send_message("{s}_{p_name}"')

        return script

    # 准备拷贝新的项目
    def ready_project(self, p_name):
        media_arr = [
            # 'ScrapingTikTokPostsByCharles',
            # 'ScrapingTikTokUserInfoByUniqueId',
            # 'ScrapingTikTokUserPostsByUniqueId',

            'ScrapingYoutubeVideosByKeywordsV001',
            'ScrapingYoutubeVideoDetailsByVideoId',
            'ScrapingYoutubeChannelAboutByChannelUrl'
        ]
        results = []
        for media in media_arr:
            temp = self.start_copy(f"{media}_{p_name}")
            results.append(temp)
        return results

    # 清除完成的任务
    def drop_project(self, name):
        return self.db.collection.remove({'temp_name': name})

    def start_copy(self, project_name):
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
        cpdb['script'] = self.replace_script(script, p_name)
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





