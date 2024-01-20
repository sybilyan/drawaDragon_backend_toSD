
import datetime
import logging
import os
import traceback
from gridfs import GridFS
import pymongo

from util.error_helper import ErrHelper, ErrType


db_url = 'mongodb://admin:AbcD%23123%2B%2B@43.135.80.90:27017/'
db_database = 'dragon'
db_task_table = 'dragon_task'


class MongoConnection:
    """
    Singleton class.
    Manage the connection to MongoDB.
    """
    _instance = None

    def __init__(self) -> None:
        if self.db is None:
            self.connection = pymongo.MongoClient(
                db_url).get_database(db_database)
            self.collection = self.connection[db_task_table]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.connection = None
            cls._instance.db = None

        return cls._instance

    def __del__(self):
        self.connection = None
        self.db = None
        MongoConnection._instance = None

    def get_db(self):
        # connect
        if self.db is None:
            if self.connection is None:
                self.connection = pymongo.MongoClient(db_url)
            self.db = self.connection[db_database]

        return self.db

    def update_one(self, task_id, status, res=[]):
        """"
        Get task information from DB according to task id.
        And then upadte task status to 2, which mean the task is in processing.

        Return:
        task_data, dict, the configuration data of the task.
        """
        try:
            # updata task status
            self.collection.find_one_and_update(
                {'taskId': task_id}, {'$set': {'dealStatus': status, 'result': res}})

        except Exception as e:
            errMsg = ErrHelper(err_type=ErrType.UNEXPECTED_ERR,
                               err_info=traceback.format_exc()).get_err_msg()
            logging.error(errMsg)

        return None

    def find_task(self, task_id):
        """"
        Get task information from DB according to task id.
        And then upadte task status to 2, which mean the task is in processing.

        Return:
        task_data, dict, the configuration data of the task.
        """
        task_res = None

        try:
            task_res = self.collection.find_one({'taskId': task_id})
        except Exception as e:
            errMsg = ErrHelper(err_type=ErrType.UNEXPECTED_ERR,
                               err_info=traceback.format_exc()).get_err_msg()
            logging.error(errMsg)
            task_res = None

        return task_res

    def get_table(self):
        _db = self.get_db()
        return _db[db_task_table]

    def upload_file(self, file_name):
        """
        上传图片到MongoDB Gridfs
        :param data_host mongodb 服务器地址
        :param data_db: 数据库地址
        :param data_col:集合
        :param file_name: （图片）文件名称
        :return:
        """

        data_col = 'dragon_picture'
        gridfs_col = GridFS(self.connection, collection=data_col)
        filter_condition = {"filename": file_name}

        """本地图片上传到MongoDB Gridfs后删除本地文件"""
        with open("%s".decode('utf-8') % file_name, 'rb') as my_image:
            picture_data = my_image.read()
            file_grid = gridfs_col.put(
                data=picture_data, **filter_condition)  # 上传到gridfs
            logging.info(f'save mongodb gridfs:{file_grid}')
            my_image.close()
            os.remove("%s" % file_name)
