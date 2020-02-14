import json
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types

class Action(object):
    def __init__(self, do_req, connection, loop):
        self.__do_req = do_req
        self.__connection = connection
        self.__loop = loop

    def __str__(self):
        return f"{{type: {self.type}, value: {self.value}, source: {self.source}}}"

    def __repr__(self):
        return self.__str__()

    @property
    def type(self):
        return self.__do_req.type

    @property
    def value(self):
        return json.loads(self.__do_req.value)

    @property
    def source(self):
        return self.__do_req.source

    def end(self, value):
        self.__loop.create_task(
            self.__connection.send(self.__build_do_rep(value))
        )

    def done(self, value):
        self.__loop.create_task(
            self.__connection.send(self.__build_do_rep(value))
        )

    def failed(self, code, desc = ""):
        if code < 1024:
            raise Exception(f"Code must be >=1024, but now ${code}.")
        self.__loop.create_task(
            self.__connection.send(self.__build_error_rep(code, desc))
        )

    def __build_do_rep(self, value):
        do_rep = protocol_types.do_rep_t()
        do_rep.value = json.dumps(value)
        do_rep.traces.extend(self.__do_req.traces)
        return do_rep

    def __build_error_rep(self, code, desc):
        error2_rep = protocol_types.error2_rep_t()
        error2_rep.code = code
        error2_rep.desc = json.dumps(desc)
        error2_rep.traces.extend(self.__do_req.traces)
        return error2_rep
