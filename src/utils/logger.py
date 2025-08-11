# src/utils/logger.py

import logging
from config.settings import settings

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(settings.LOG_FILE),
            logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)
