import sys

from loguru import logger

log = logger
log.remove()
log.add(
    sys.stdout,
    colorize=True,
    format="<blue>{time:HH:mm:ss}</blue> <fg 255>{module}:{function}:{line} | </fg 255><level>{message}</level>",
)
