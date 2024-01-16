from enum import Enum


class ErrType(Enum):
    UNEXPECTED_ERR = 100

    INVALID_PARAM = 200
    MISSING_PARAM = 201

    SD_NOT_RESPONSE = 210

    INVALID_DATA_STRUCT = 300

    INVALID_TASK_ID = 400



class ErrHelper:
    def __init__(self, err_type:ErrType, err_info:str) -> None:
        self.err_type = err_type
        self.err_info = err_info

        self.err_msg = self._gen_err_msg(err_info)



    def _gen_err_msg(self, err_info):
        msg = 'ERROR: \n[' + self.err_type.name + ']: ' + err_info
        return msg
    
    def get_err_msg(self):
        return self.err_msg




class VertexErrHelper(ErrHelper):

    def __init__(self, ver, err_type:ErrType, err_info:str) -> None:
        self.err_node = ver.id
        self.err_class = type(ver).__name__
        super().__init__(err_type, err_info)


    def _gen_err_msg(self,err_info):
        msg = 'ERROR: \n[' + self.err_type.name + ']: ' + err_info
        msg = msg + '\n  id:    ' + str(self.err_node)
        msg = msg + '\n  class: ' + str(self.err_class)
        return msg
