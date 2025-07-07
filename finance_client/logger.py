import importlib
import logging
import os

import yaml


def apply_partial_config(config_dict, target_loggers: list[str]):
    """
    apply logger settings except root

    :param config_dict: dictConfig
    :param target_loggers: namespace to apply it
    """
    formatters = config_dict.get("formatters", {})
    handlers_conf = config_dict.get("handlers", {})
    loggers_conf = config_dict.get("loggers", {})
    PROJECT_DIR = os.path.dirname(__file__)

    handler_objs = {}
    for handler_name, handler_conf in handlers_conf.items():
        handler_class = _resolve(handler_conf["class"])
        if handler_class is None:
            raise ValueError(f"Unknown handler class: {handler_conf['class']}")

        if "filename" in handler_conf:
            rel_path = handler_conf["filename"]
            abs_path = os.path.join(PROJECT_DIR, rel_path)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            handler_conf["filename"] = str(abs_path)

        kwargs = {k: v for k, v in handler_conf.items() if k not in ["class", "formatter", "level"]}
        handler = handler_class(**kwargs)
        level = handler_conf.get("level")
        if level:
            handler.setLevel(level)

        formatter_name = handler_conf.get("formatter")
        if formatter_name:
            fmt_conf = formatters.get(formatter_name)
            if fmt_conf:
                handler.setFormatter(logging.Formatter(fmt_conf["format"]))

        handler_objs[handler_name] = handler

    for logger_name in target_loggers:
        logger_conf = loggers_conf.get(logger_name)
        if logger_conf is None:
            continue

        logger = logging.getLogger(logger_name)
        logger.setLevel(logger_conf["level"])

        for handler_name in logger_conf["handlers"]:
            handler = handler_objs[handler_name]
            logger.addHandler(handler)

        logger.propagate = logger_conf.get("propagate", True)


def _resolve(class_path: str):
    parts = class_path.split(".")
    module_name = ".".join(parts[:-1])
    class_name = parts[-1]
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        return None


def setup_logging():
    if "FC_DEBUG" in os.environ:
        __DEBUG = bool(os.environ["FC_DEBUG"])
        print(f"fc debug mode: {__DEBUG}")
    else:
        __DEBUG = False
    BASE_PATH = os.path.dirname(__file__)
    if __DEBUG:
        path = os.path.join(BASE_PATH, "logging.yaml")
    else:
        path = os.path.join(BASE_PATH, "logging_test.yaml")
    with open(path) as f:
        config = yaml.safe_load(f)
    apply_partial_config(config, target_loggers=list(config["loggers"].keys()))
