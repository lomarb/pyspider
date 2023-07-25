# 每次启动项目，将代码拷贝一份作为启动项目的执行代码
#
import time

from pyspider.database.mongodb.projectdb import ProjectDB


class CopyProject:

    def __init__(self):
        url = 'mongodb://root:8a2p9j3x9g@3.134.227.240/projectdb?authSource=admin'
        self.db = ProjectDB(url, database='projectdb')

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
        cpdb['script'] = script.replace(project, project_name)
        cpdb['temp_name'] = p_name
        cpdb['updatetime'] = time.time()
        cpdb['status'] = 'TODO'

        return self.db.collection.insert_one()
        # return self.db.insert(f"{project_name}", cpdb)


