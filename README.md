# drawadragon_backend
backend for draw a dragon 

## 使用代码

  1. 本地运行SD起到7860端口后运行main.py

  2. python client.py可以模拟调用
  
## API

### /dragon/upload POST

  1. 入参：<br>
  "Content-Type": "application/json"<br>
  "data": {<br>
    "color":"white",<br>
    "width":512,<br>
    "height":512, <br>
    "file_raw":file_raw_decoded,(图片转为base64格式)<br>
    "file_doodle":file_doodle_decoded,(图片转为base64格式)<br>
    "file_img_doodle":file_img_doodle_decoded(图片转为base64格式)<br>
    }<br>

  2. 出参<br>
  response: {<br>
    "charset":"utf-8",<br>
    "content":{"picture":"xxx"},<br>
    "content_type":"application/json;charset=utf-8",<br>
    "reason":"success",<br>
    "status":"200"<br>
    }<br>


