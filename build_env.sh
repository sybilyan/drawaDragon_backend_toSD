VENV_DIR=venv_dragon
REQUIREMENTS=requirements.txt

echo Setting up virtual environment...
if [ ! -d "$VENV_DIR" ]; then
python3 -m venv "$VENV_DIR"
fi
. "$VENV_DIR/bin/activate"

echo Installing requirements...
pip3 install -r "$REQUIREMENTS"

echo Setup complete.

echo Running the application...
. "$VENV_DIR/bin/activate"
nohup python dragon_api_main.py --config "debug_dp" > nohup.out 2>&1 &
