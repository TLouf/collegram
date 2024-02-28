
import json
import logging.config


def init_logging(script_path):
    logging_conf = json.loads((script_path.parent / 'logging.json').read_text())
    logging_conf['handlers']['file_handler']['filename'] = str(script_path.with_suffix('.log'))
    logging.config.dictConfig(logging_conf)
    logger = logging.getLogger()
    return logger

