# 导入模块
import ast
import datetime
import json
import logging
import math
import os

from kafka import KafkaConsumer
from pojo.controlnet_fordragon import SDControlNet
from pojo.img2img_fordragon import StableDiffusionImg2ImgVertex
from task.minio_vertex import MinIOConnection, minio_settings_bucket
# 导入kafka的消费模块

# 导入mongo模块
from task.mongo_connection import MongoConnection
from util.image_util import ImageUtil
from util.timer import timer_decorator

# 设置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
# 设置将日志输出到文件中，并且定义文件内容
file_info = logging.FileHandler(
    f"logs/AutoTest_log_{now}.log", mode='a', encoding='utf-8', delay=False)
file_info.setLevel(logging.INFO)
file_info.setFormatter(formatter)


logger.addHandler(file_info)

kafka_url = "119.45.243.21:9092"
kafka_topic = "dragon_task_test"
kafka_group = "g_drago_task_test"


# 上传的图片保存路径
UPLOAD_PATH = os.path.join(os.path.dirname(__file__), 'img')
mongoConnection = MongoConnection()
s3 = MinIOConnection()

# 访问SD处理请求


@timer_decorator
def deal_msg(task_id, inputData_raw, inputData_doodle, inputData_img_doodle, color, width, height):
    # 获取参数
    img_raw = None
    img_doodle = None
    img_whole = None

    # 上传文件夹如果不存在则创建
    if not os.path.exists(UPLOAD_PATH):
        os.mkdir(UPLOAD_PATH)
    try:
        # load image
        img_raw = ImageUtil.base64_to_image(inputData_raw)
        resized_image_raw = img_raw.resize((int(width),int(height)))
        logger.info(f"input image type:{resized_image_raw.mode}")
        # load mask
        inputData_doodle = ImageUtil.base64_to_image(inputData_doodle)
        resized_inputData_doodle = inputData_doodle.resize((int(width),int(height)))
        img_doodle = ImageUtil.invert_doodle(
            resized_inputData_doodle, os.path.join(UPLOAD_PATH, now+"_test_doodle.png"))
        # load mask_image
        img_whole = ImageUtil.base64_to_image(inputData_img_doodle)

    except Exception as e:
        # info = e.format_exc()
        logging.error(f'load info error: {format(e)}')
        return 0
    # load color
    color_dragon = color + " dragon"
    prompt = "(masterpiece, best quality), cute_shouhui_dragon, Baring head, solo, " + \
        color_dragon + \
        ", (dragon head), liner, <lora:dragon0117_epoch015_loss0.062:1>"
    negprompt = "(worst quality:2), (low quality:2), (normal quality:2), lowres, bad anatomy, bad hands, (one-eyed:1.5), cross-eyed, ((monochrome)), ((grayscale)), watermark, blurry, mutated hands, text, wordage, (hands:1.5), (fingers:1.5), tooth"

    ver_config = {
        "params": {
            "url": {
                "value": {
                    "value": "http://127.0.0.1:7861"
                }
            },
            "image": {
                "value": {
                    "value": resized_image_raw
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
                    "value": 0.9
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

    ctrlnet = SDControlNet(ver_config)
    ctrlnet_out = ctrlnet.process()

    ver_config['params']['plugin']['value']['value'] = ctrlnet_out
    sd = StableDiffusionImg2ImgVertex(data=ver_config)
    output = sd.process()

    index = 0
    for index, img in enumerate(output):
        path = f'img/{task_id}_{str(index)}.png'
        s3_path = f'dragon/{task_id}_{str(index)}.png'
        img.save(path)
        s3.get_connection().fput_object(minio_settings_bucket, s3_path, path)

    # pic_path = task_id+'_'+str(index)+'.png'
    # ret = {"picture":pic_path}
    return index+1


if __name__ == '__main__':
    logger.info("hello world")
    # 连接kafka
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_url, group_id=kafka_group,
                             auto_offset_reset='smallest', value_deserializer=json.loads)

    # 遍历内容
    try:
        while True:
            # logger.info("hello world11")
            messages = consumer.poll(timeout_ms=500)  # 每500毫秒拉取一次消息
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    try:
                        lists = message.value
                        task_id = lists['taskId']

                        logging.info(f'receive task:::{task_id}')
                        # 更新mongo中状态为正在执行
                        timer_decorator(mongoConnection.update_one)(task_id, 1)

                        # 查到mongo中任务内容
                        task_request = timer_decorator(
                            mongoConnection.find_task)(task_id)

                        # 访问SD并将结果放至本地并传至minIO
                        # result_num = del_msg(lists['taskDetail'], task_id)
                        result_num = deal_msg(task_id, task_request['taskDetail']['file_raw'],
                                              task_request['taskDetail']['file_doodle'],
                                              task_request['taskDetail']['file_img_doodle'],
                                              task_request['taskDetail']['color'],
                                              task_request['taskDetail']['width'],
                                              task_request['taskDetail']['height'])

                        if (result_num != 0):
                            pic_list = ["https://duck-test-1319341997.cos.ap-nanjing.myqcloud.com/dragon/" +
                                        task_id + "_" + str(i) + ".png" for i in range(result_num)]
                            # 将mongdb中的任务结果改为已完成
                            timer_decorator(mongoConnection.update_one)(
                                task_id, 2, pic_list)
                        else:
                            logger.error(f'Failed to deal {task_id}')
                            # 将mongdb中的任务结果改为已出错
                            timer_decorator(mongoConnection.update_one)(
                                task_id, 3)

                    except Exception as e:
                        logging.error(
                            'Failed to deal message {}: {}'.format(message, e))

    except KeyboardInterrupt:
        pass
