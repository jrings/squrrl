import sys

from loguru import logger

log = logger
log.remove()
log.add(
    sys.stdout,
    colorize=True,
    format="<m>{time:HH:mm:ss}</m> <c>{module}:{function}:{line} | </c><level>{message}</level>",
)
