import datetime
import json
import random
import time

import requests
from overrides import overrides
from util.image_util import ImageUtil
import logging
from util.error_helper import VertexErrHelper,ErrType
from pojo.vertex import Vertex

logger = logging.getLogger()
logger.setLevel(logging.INFO) 

now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
#设置将日志输出到文件中，并且定义文件内容
fileinfo = logging.FileHandler(f"logs/AutoTest_log_{now}.log")
fileinfo.setLevel(logging.INFO) 
fileinfo.setFormatter(formatter)


#设置将日志输出到控制台
controlshow = logging.StreamHandler()
controlshow.setLevel(logging.INFO)
controlshow.setFormatter(formatter)


logger.addHandler(fileinfo)
logger.addHandler(controlshow)

# \brief Vertex of stable diffusion
class StableDiffusionImg2ImgVertex(Vertex):
    @overrides
    def __init__(self, data: dict) -> None:
        default_params = {
            "url": "http://127.0.0.1:7860/sdapi/v1/img2img",
            "prompt": ["(masterpiece, best quality), cute_shouhui_dragon, Baring head, solo, dragon, (dragon head), eastern dragon, ", 
                       "(worst quality:2), (low quality:2), (normal quality:2), lowres, bad anatomy, bad hands, ((monochrome)), ((grayscale)), watermark, bad legs, bad arms, blurry, cross-eyed, mutated hands, text, watermark, wordage, (hand:1.5)"],  # ["positive prompt", "negative prompt"]
            "steps": 20,
            "width": 512,
            "height": 512,
            "batch_size": 4,
            "cfg_scale": 4.5,
            "seed": -1,
            "sampler_name": "DPM++ 2M Karras",
            "sd_model_checkpoint": "AnythingV5_v5PrtRE",
            "CLIP_stop_at_last_layers": 1,
            "sd_vae": "Automatic",
            "override_settings_restore_afterwards": False,
            "discard_last_output": False,  # sometimes the last output of sd should be droped away
        }
        super().__init__(data, default_params)


    def _get_end_point(self) -> str:
        return '/sdapi/v1/img2img'


    def _get_request_data(self, sd_plugins) -> dict:
        input_image = self.params['image']
        try:
            base64_str = ImageUtil.image_to_base64(input_image)

            mask_image = self.params['mask']
            mask_str = None
            if mask_image:
                mask_str = ImageUtil.image_to_base64(mask_image)

            # seed
            seed = self.params['seed']
            if seed < 0:
                seed = random.randint(1, 2147483647)

            return {
                # for img2img
                "denoising_strength": self.params.get('denoising_strength', 0.86),
                "include_init_images": self.params.get('include_init_images', False),
                "init_images": [base64_str],
                "inpaint_full_res": self.params.get('inpaint_full_res', False),
                "inpaint_full_res_padding": self.params.get('inpaint_full_res_padding', 0),
                "inpainting_fill": self.params.get('inpainting_fill', 1),
                "inpainting_mask_invert": self.params.get('inpainting_mask_invert', False),
                "mask": mask_str,
                "mask_blur": self.params.get('mask_blur', 22),
                "resize_mode": self.params.get('resize_mode', 0),
                #
                "prompt": self.params['prompt'][0],
                "negative_prompt": self.params['prompt'][1],
                "steps": self.params['steps'],
                "width": self.params['width'],
                "height": self.params['height'],
                "batch_size": self.params['batch_size'],
                "cfg_scale": self.params['cfg_scale'],
                "seed": seed,
                "sampler_name": self.params['sampler_name'],
                "override_settings": {
                    "sd_model_checkpoint": self.params['sd_model_checkpoint'],
                    "CLIP_stop_at_last_layers": self.params['CLIP_stop_at_last_layers'],
                    "sd_vae": self.params['sd_vae']
                },
                "override_settings_restore_afterwards": self.params['override_settings_restore_afterwards'],
                "alwayson_scripts": sd_plugins
                #
            }
        except (FileNotFoundError, PermissionError, IsADirectoryError, UnicodeDecodeError, OSError) as e:    
            logger.error(f'img2img cannot read image:{type(e)}')
            raise

    def _get_plugins(self) -> dict:
        sd_plugins = {}
        for key, plugin in self.params.items():
            # print("key:::",key)
            if key.startswith('plugin') and plugin is not None:
                plugin_name = plugin.get_name()  # str
                plugin_data = plugin.get_data()  # object
                sd_plugins[plugin_name] = plugin_data
        return sd_plugins
    
    @overrides
    def _process(self) -> list:
        request_data = self._get_request_data(self._get_plugins())

        data_str = json.dumps(request_data, indent=4, ensure_ascii=False)
        # logger.info(f'[SD Vertex] request data is: \n {data_str}')

        _start_time = time.time()
        response = self._send_request(self.params['url'] + self._get_end_point(), request_data)
        # logger.info(f'[SD Vertex, response]: {response}')
        _end_time = time.time()
        _elapsed_time = _end_time - _start_time
        logger.info('[SD Vertex] elapsed time:{:.2f} seconds'.format(_elapsed_time))
        

        if response.status_code != requests.codes.ok:
            logger.error('[SD Vertex] SD not response.')
            self.err = VertexErrHelper(self, ErrType.SD_NOT_RESPONSE, f'{response}, {response.reason}')
            return []

        json_response = response.json()
        images_base64 = json_response['images']
        images = [ImageUtil.base64_to_image(val) for val in images_base64]

        # discard the last image(controlnet preview) in the list
        # if self.params['discard_last_output']:
        images = images[:-1]

        logger.info(f'[SD Vertex] output is: \n {images}')
        return images



    def _send_request(self, url, data):
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps(data)
        response = requests.post(url, data=payload) 
        return response