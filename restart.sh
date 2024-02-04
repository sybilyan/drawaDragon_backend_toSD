#!/bin/sh
count=`ps -ef |grep dragon_api_main.py |grep -v "grep" |wc -l`
if [ 0 == $count ]
	then
	echo "服务未启动"
else
	old_ids=$(ps -ef | grep dragon_api_main.py | grep -v "grep" | awk '{print $2}')
	kill $old_ids
	echo "已停止服务"
fi

VENV_DIR=venv_dragon
. "$VENV_DIR/bin/activate"
nohup python dragon_api_main.py --config "debug_dp" > nohup.out 2>&1 &
echo "重启服务成功"

