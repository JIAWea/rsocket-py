from rsocket.frame import ErrorCode, SetupFrame

ERROR_INVALID_STREAM = "SETUP or RESUME is not a frame with stream id of 0"
ERROR_INVALID_FIRST_FRAME = "first frame must be setup or resume"
ERROR_UNSUPPORTED_RESUME = "unsupported resume yet"


class CustomException(Exception):
    def __init__(self, err_code, err_msg):
        super().__init__(err_code, err_msg)
        self.err_code = err_code
        self.err_msg = err_msg


class InvalidSetupException(CustomException):
    pass


class RejectedSetupException(CustomException):
    pass


class UnsupportedSetupException(CustomException):
    pass


class ConnectionErrorException(CustomException):
    pass


class IllegalArgumentException(CustomException):
    pass


class ApplicationException(CustomException):
    pass


class RejectedException(CustomException):
    pass


class ConnectionException(CustomException):
    pass


class CanceledException(CustomException):
    pass


class InvalidException(CustomException):
    pass


class RuntimeExceptions:
    @classmethod
    def handle(cls, stream_id, err_code, err_msg):
        if stream_id == 0:
            if err_code == ErrorCode.INVALID_SETUP:
                return InvalidSetupException(err_code, err_msg)
            elif err_code == ErrorCode.UNSUPPORTED_SETUP:
                return UnsupportedSetupException(err_code, err_msg)
            elif err_code == ErrorCode.REJECTED_SETUP:
                return RejectedSetupException(err_code, err_msg)
            elif err_code == ErrorCode.CONNECTION_ERROR:
                return ConnectionErrorException(err_code, err_msg)
            else:
                return IllegalArgumentException(
                    err_code, "Invalid Error frame in Stream ID 0: {}, '{}'".format(stream_id, err_msg))
        else:
            if err_code == ErrorCode.APPLICATION_ERROR:
                return ApplicationException(err_code, err_msg)
            elif err_code == ErrorCode.REJECTED:
                return RejectedException(err_code, err_msg)
            elif err_code == ErrorCode.CANCELED:
                return CanceledException(err_code, err_msg)
            elif err_code == ErrorCode.INVALID:
                return InvalidException(err_code, err_msg)
            else:
                return IllegalArgumentException(
                    err_code, "Invalid Error frame in Stream ID 0: {}, '{}'".format(stream_id, err_msg))


def verify_first_frame(frame):
    if not frame.stream_id == 0:
        raise InvalidSetupException(ErrorCode.INVALID_SETUP, ERROR_INVALID_STREAM)

    # if not isinstance(frame, SetupFrame) or not isinstance(frame, ResumeFrame):
    if not isinstance(frame, SetupFrame):
        raise InvalidSetupException(ErrorCode.INVALID_SETUP, ERROR_INVALID_FIRST_FRAME)