
from pojo.vertex import Vertex
from util.image_util import ImageUtil


class SDPluginVertex(Vertex):
    def __init__(self, data: dict, default_params: dict = None) -> None:
        super().__init__(data, default_params)
        self.plugin_name = "None"

    def _process(self):
        return self

    def get_name(self):
        """
        Get the name of the plugin
        """
        return self.plugin_name

    def get_data(self):
        """
        Get args data that can pass to the plugin
        """
        raise NotImplementedError("Function not implemented.")


class SDControlNet(SDPluginVertex):
    """
    Vertex of Stable Diffusion control net plugin
    """

    def __init__(self, data: dict) -> None:
        default_params = {
            "input_image": "",
            "module": "depth_midas",
            "model": "control_v11f1p_sd15_depth [cfd03158]",
            "weight": 1,
            "pixel_perfect": True,
            "guidance_start": 0,
            "guidance_end": 1,
            "control_mode": 0,
            "processor_res": 512,
            "threshold_a": 64,
            "threshold_b": 64,
            "resize_mode": 0,
            "save_detected_map": True
        }
        super().__init__(data, default_params)
        self.plugin_name = 'controlnet'

    def get_data(self):
        """
        Get args data that can pass to the plugin
        """
        input_img = ImageUtil.image_to_base64(self.params['control_mask'])

        return {
            "args": [
                {
                    "input_image": input_img,
                    "module": self.params['module'],
                    "model": self.params['model'],
                    "weight": self.params['weight'],
                    "pixel_perfect": self.params['pixel_perfect'],
                    "guidance_start": self.params['guidance_start'],
                    "guidance_end": self.params['guidance_end'],
                    "control_mode": self.params['control_mode'],
                    "resize_mode": self.params['resize_mode'],
                    # "processor_res": self.params['processor_res'],
                    "threshold_a": self.params['threshold_a'],
                    "threshold_b": self.params['threshold_b'],
                    "save_detected_map": self.params['save_detected_map']

                }
            ]
        }
