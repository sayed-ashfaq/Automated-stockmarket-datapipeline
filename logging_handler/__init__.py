# custom_logger/__init__.py
from .logger import CustomLogger
# Create a single shared logger instance
GLOBAL_LOGGER = CustomLogger().get_logger("Automated-stockmarket-datapipeline")