# 每次启动项目，将代码拷贝一份作为启动项目的执行代码
#
import time

from pyspider.database.mongodb.projectdb import ProjectDB


class CopyProject:

    def __init__(self):
        url = 'mongodb://root:8a2p9j3x9g@3.134.227.240/projectdb?authSource=admin'
        self.db = ProjectDB(url, database='projectdb')

    @staticmethod
    def replace_script(script, p_name):
        origin_script_arr = [
            'ScrapingTikTokPostsByCharles',
            'ScrapingTikTokUserInfoByUniqueId',
            'ScrapingTikTokUserPostsByUniqueId',
        ]
        for s in origin_script_arr:
            script = script.replace(f'class {s}(BaseHandler)', f'class {s}_{p_name}(BaseHandler)')
            script = script.replace(f"self.send_message('{s}'", f"self.send_message('{s}_{p_name}'")

        return script

    # 准备拷贝新的项目
    def ready_project(self, p_name):
        media_arr = [
            'ScrapingTikTokPostsByCharles',
            'ScrapingTikTokUserInfoByUniqueId',
            'ScrapingTikTokUserPostsByUniqueId',
        ]
        results = []
        for media in media_arr:
            temp = self.start_copy(f"{media}_{p_name}")
            results.append(temp)
        return results

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





