import logging
import traceback, time
from util.error_helper import VertexErrHelper, ErrType
    
logger = logging.getLogger()
logger.setLevel(logging.INFO) 
class Vertex:
    def __init__(self, data: dict, default_value: dict) -> None:
        self.id = -1
        self._data = data
        self._parse_data(default_value)
        self._built_object = None
        self._built = False
        self.err = None

        # set id
        if 'id' in data:
            self.id = data['id']

    def _parse_data(self, default_values) -> None:
        self.data = self._data
        self.params = default_values
        
        try:
            for key, val in self.data['params'].items():
                self.params[key] = val['value']['value']
        except Exception as e:
            # set error
            self.err = VertexErrHelper(self, ErrType.INVALID_DATA_STRUCT, str(e))



    def _process(self):
        return None
    

    
    def process(self):
        output = None
        try:
            _start_time = time.time()
            output = self._process()
            _end_time = time.time()
            _elapsed_time = _end_time - _start_time
            logger.info('[Vertex] elapsed time:{:.2f} seconds'.format(_elapsed_time))
        except Exception as e:
            # raise unexpected err
            self.err = VertexErrHelper(self, ErrType.UNEXPECTED_ERR, traceback.format_exc())
            logger.warning(f'[Error]: {self.err.get_err_msg()}')
    
        return output
    

    def pop_last_err(self):
        _err = self.err
        self.err = None
        return _err


    def set_param(self, param_name, param_value):
        self.params[param_name] = param_value

    
    def get_param(self, param_name):
        return self.params[param_name]
    
    def is_control_vertex(self):
        return False

    def __repr__(self) -> str:
        return f"Node(id={self.id}, data={self.data})"

    def __eq__(self, __o: object) -> bool:
        return self.id == __o.id if isinstance(__o, Vertex) else False

    def __hash__(self) -> int:
        return id(self)

    def _built_object_repr(self):
        return repr(self._built_object)

