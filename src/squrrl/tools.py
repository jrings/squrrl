import sys

from loguru import logger

log = logger
log.remove()
log.add(
    sys.stdout,
    colorize=True,
    format="<fg 86>{time:HH:mm:ss}</fg 86> <fg 255>{module}:{function}:{line} | </fg 255><level>{message}</level>",
)
