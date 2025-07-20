from logging import Logger as BaseLogger, Formatter, StreamHandler

DEFAULT_FORMATTER = Formatter(
    fmt="[%(asctime)s][%(name)s][%(levelname)s][%(funcName)s] %(message)s"
)
DEFAULT_HANDLER = StreamHandler()
DEFAULT_HANDLER.setFormatter(DEFAULT_FORMATTER)


class Logger(BaseLogger):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addHandler(DEFAULT_HANDLER)
