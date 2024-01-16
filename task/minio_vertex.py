import logging
import os
import uuid, time

import minio

# from ..pojo.vertex import Vertex


minio_settings_endpoint ="s3.ap-northeast-1.amazonaws.com"
minio_settings_region= "ap-northeast-1"
minio_settings_access_key= "AKIA3CZGA3ILGMI2HPEL"
minio_settings_secret_key= "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"
minio_settings_bucket= "aurobit-s3-01"



class MinIOConnection:
    """
    Singleton class.
    Manage the connection to minIO server.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

            cls._instance.endpoint = minio_settings_endpoint
            cls._instance.access_key = minio_settings_access_key
            cls._instance.secret_key = minio_settings_secret_key
            cls._instance.region = minio_settings_region
            cls._instance.region = None


            cls._instance.connection = minio.Minio(
                endpoint=cls._instance.endpoint,
                access_key=cls._instance.access_key,
                secret_key=cls._instance.secret_key,
                secure=True,
                region=cls._instance.region
            )
        return cls._instance


    # def __del__(self):
    #     print('close connection.')
    #     self.connection.close()


    def get_connection(self):
        return self.connection
    






# class MinIODownloadVertex(Vertex):
#     """
#     Download data from server.
#     """
#     def __init__(self, data: dict) -> None:
#         default_params = {
#             "bucket": minio_settings_bucket,
#             "object_name": "",
#             "file_dir": "img/",                 # dir the downloaded file will be saved in
#                                             # CONFIG['tmp_path'] will be used if it is ''
                                            
#             "global_path": False,
#             "overwrite": 1
#         }
#         super().__init__(data, default_params)



#     def _process(self):
        
#         # file name
#         object_name = self.params['object_name']
#         obj_basename = os.path.basename(object_name)

#         # output path
#         output_dir = self.params['file_dir']
#         output_name = output_dir + '/' + obj_basename
#         if self.params['global_path']:
#             output_name = os.path.abspath(output_name)

#         # do not overwrite
#         if not self.params['overwrite'] and os.path.exists(output_name):
#             logging.info('[MinIODownloadVertex]:  file exist, do nothing.')
#             return output_name

#         _start_time = time.time()
#         # connection
#         conn = MinIOConnection().get_connection()

#         # get object
#         conn.fget_object(bucket_name=self.params['bucket'],
#                  object_name=object_name,
#                  file_path=output_name
#                  )
        
        
#         _end_time = time.time()
#         _elapsed_time = _end_time - _start_time
#         logging.info('[MinioDonwload] elapsed time:{:.2f} seconds'.format(_elapsed_time))
#         logging.info(f'[MinioDonwload] Done. file: {object_name}')

#         return output_name
    


# class MinIOUploadsVertex(Vertex):
#     """
#     Upload datas to server
#     """
#     def __init__(self, data: dict) -> None:
#         default_params = {
#             "bucket": minio_settings_bucket,
#             # "object_name": "",          # if set, use this name directly
#             "object_dir": "img/",           # world if object_name not set
#             "file_paths": [],
#             "rename": 0                 # work if object_name not set
#         }
#         super().__init__(data, default_params)



#     def _process(self):
        
#         object_names = []
#         for file_path in self.params['file_paths']:

#             # object name
#             object_name = ""#self.params['object_name']
#             object_dir = self.params['object_dir']
#             file_name = os.path.basename(file_path)
#             obj_basename, obj_extension = os.path.splitext(file_name)    
#             # rename file basename by uuid
#             if self.params['rename']:
#                 obj_basename = str(uuid.uuid4())
#             object_name = object_dir + '/' + obj_basename + obj_extension
            
#             _start_time = time.time()
             
#             # connection
#             conn = MinIOConnection().get_connection()

#             # get object
#             # bucket_name：目标远程存储桶名，指定文件将被存储在哪个桶下。
#             # object_name：指定目标文件将在远程桶下以什么名字被存储。
#             # data：指定文件的I/O缓冲数据，这也是为什么我们用with open先读取文件了
#             # length：Minio需要在上传时指定文件的大小，它并不会自动计算文件的大小，因此需要我们在终端获取到文件的大小并作为参数传递给API。
#             conn.fput_object(bucket_name=self.params['bucket'],
#                     object_name=object_name,
#                     file_path=file_path
#                     )
            
#             object_names.append(object_name)

#             _end_time = time.time()
#             _elapsed_time = _end_time - _start_time
#             logging.info('[MinioUploads] elapsed time:{:.2f} seconds'.format(_elapsed_time))
#             logging.info(f'[MinioUploads] file: {object_name}')

#         return object_names
    

# class MinIODownloadLoraVertex(MinIODownloadVertex):
#     """
#     Download data from server.
#     """
#     def __init__(self, data: dict) -> None:
#         super().__init__(data)
#         self.params['file_dir'] = CONFIG['sd_lora_path']


#     def _process(self):
#         return super()._process()
    


