# 导入模块
import ast
import datetime
import json
import logging
import math
import os

from flask import jsonify
from kafka import KafkaConsumer
from pojo.controlnet_fordragon import SDControlNet
from pojo.img2img_fordragon import StableDiffusionImg2ImgVertex
from task.minio_vertex import MinIOConnection, minio_settings_bucket
# 导入kafka的消费模块

# 导入mongo模块
from task.mongo_connection import MongoConnection
from util.image_util import ImageUtil

#设置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO) 

now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
#设置将日志输出到文件中，并且定义文件内容
fileinfo = logging.FileHandler(f"logs/AutoTest_log_{now}.log",mode='a', encoding='utf-8', delay=False)
fileinfo.setLevel(logging.INFO) 
fileinfo.setFormatter(formatter)
#设置将日志输出到控制台
controlshow = logging.StreamHandler()
controlshow.setLevel(logging.INFO)
controlshow.setFormatter(formatter)


logger.addHandler(fileinfo)
logger.addHandler(controlshow)
 
kafka_url = "119.45.243.21:9092"     
kafka_topic = "dragon_task_test"       
kafka_group = "g_drago_task_test"


# 上传的图片保存路径
UPLOAD_PATH = os.path.join(os.path.dirname(__file__), 'img')
mongoConnection = MongoConnection()
s3 = MinIOConnection()

#访问SD处理请求
def del_msg(json, pic_id):
    # 获取参数
    img_raw = None
    inputData_doodle = None
    img_doodle = None
    img_whole = None


    inputData_raw = json.get("file_raw")
    inputData_doodle = json.get("file_doodle")
    inputData_img_doodle = json.get("file_img_doodle")
    color = json.get("color") if json.get("color") else ""
    width = json.get("width")
    height = json.get('height')

    # 上传文件夹如果不存在则创建
    if not os.path.exists(UPLOAD_PATH):
        os.mkdir(UPLOAD_PATH)
    try:
        # load image
        img_raw = ImageUtil.base64_to_image(inputData_raw)
        # load mask
        inputData_doodle = ImageUtil.base64_to_image(inputData_doodle)
        img_doodle = ImageUtil.invert_doodle(inputData_doodle, os.path.join(UPLOAD_PATH, now+"_test_doodle.png"))
        #load mask_image
        img_whole = ImageUtil.base64_to_image(inputData_img_doodle)


        img_width = img_raw.width  # 图片宽度
        img_height = img_raw.height  # 图片高度
        print("raw img width -> {}, height -> {}".format(img_width, img_height))
        

        img_width = img_doodle.width  # 图片宽度
        img_height = img_doodle.height  # 图片高度
        print("img_doodle width -> {}, height -> {}".format(img_width, img_height))
    except Exception as e:
        # info = e.format_exc()
        logging.error(f'load info error: {e.__format__()}')
    #load color
    color_dragon = color+"_dragon"
    prompt = "(masterpiece, best quality), cute_shouhui_dragon, Baring head, solo," + color_dragon + ",(dragon head), eastern dragon, <lora:first_chance0.124:1>"
    negprompt = "(worst quality:2), (low quality:2), (normal quality:2), lowres, bad anatomy, bad hands, ((monochrome)), ((grayscale)), watermark, bad legs, bad arms, blurry, cross-eyed, mutated hands, text, watermark, wordage, (hand:1.5)"

    ver_config = {
    "params": {
        "url": {
            "value": {
                "value": "http://127.0.0.1:7860"
            }
        },
        "image": {
            "value": {
                "value": img_raw
            }
        },
        "mask": {
            "value": {
                "value": img_doodle
            }
        },
        "control_mask": {
            "value": {
                "value": img_whole
            }
        },
        "denoising_strength": {
            "value": {
                "value": 0.89
            }
        },
        "prompt": {
            "type": "string",
            "value": {
                "connected": 1,
                "value": [prompt, negprompt]
            }
        },
        "width": {
            "type": "int",
            "value": {
                "connected": 0,
                "value": int(width)
            }
        },
        "height": {
            "type": "int",
            "value": {
                "connected": 0,
                "value": int(height)
            }
        },
        "seed": {
            "type": "int",
            "value": {
                "connected": 0,
                "value": -1
            }
        },
        "batch_size": {
            "type": "int",
            "value": {
                "connected": 0,
                "value": 4
            }
        },
        "sd_model_checkpoint": {
            "type": "string",
            "value": {
                "connected": 0,
                "value": "AnythingV5_v5PrtRE"
            }
        },
        "plugin": {
            "value": {
                "value": None
            }
        }

    }
}

    ctrlnet= SDControlNet(ver_config)
    ctrlnet_out = ctrlnet.process()
    ver_config['params']['plugin']['value']['value'] = ctrlnet_out
    sd = StableDiffusionImg2ImgVertex(data = ver_config)
    output = sd.process()

    index = 0
    for index, img in enumerate(output):
        path = f'img/{pic_id}_{str(index)}.png'
        s3_path = f'dragon/{pic_id}_{str(index)}.png'
        img.save(path)
        s3.get_connection().fput_object(minio_settings_bucket, s3_path, path)

    pic_path = pic_id+'_'+str(index)+'.png'
    ret = {"picture":pic_path}
    return index

     





if __name__ == '__main__':
    # 连接kafka
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers = kafka_url ,group_id=kafka_group, auto_offset_reset='smallest', value_deserializer=json.loads)
    
    # 遍历内容
    try:
        while True:
            messages = consumer.poll(timeout_ms=500)  # 每500毫秒拉取一次消息
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    try:
                        lists = message.value
                        task_id = lists['taskId']
                        logging.info(f'receive task{task_id}')
                        # 更新mongo中状态为正在执行

                        mongoConnection.update_one(task_id, 1)

                        # 访问SD并将结果放至本地并传至minIO
                        index = del_msg(lists['taskDetail'], task_id)
                        
                        # 将mongdb中的任务结果改为已完成
                        mongoConnection.update_one(task_id, 2)


                    except ValueError as e:
                         logging.error('Failed to deal message {}: {}'.format(message, e))
                  

    except KeyboardInterrupt:
        pass


