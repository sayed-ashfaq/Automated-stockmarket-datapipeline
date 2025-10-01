import os
import logging
import structlog
from datetime import datetime

class CustomLogger:
    def __init__(self, log_dir= "logs"):

        # Ensure logs directory exists
        self.logs_dir= os.path.join(os.getcwd(), log_dir)
        os.makedirs(self.logs_dir, exist_ok=True)  # create a file directory if not exists

        # create file - Timestamped for persistence
        log_file= f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log" # naming
        self.log_file_path= os.path.join(self.logs_dir, log_file) #path (not yet created)

    def get_logger(self, name= __file__):

        logger_name= os.path.basename(name)

        # configure logging for file(in json) - able to log info
        file_handler= logging.FileHandler(self.log_file_path) # FileHandler: writes log on your disk in the file path
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(message)s")) # Raw JSON

        # Configure for console
        console_handler= logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(message)s"))

        logging.basicConfig(
            level= logging.INFO,
            format= "%(message)s", # JSON- structlog will handle
            handlers=[file_handler, console_handler]
        )

        # Configure structlog for JSON structured logging
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt= 'iso', utc=True, key= "timestamp"),
                structlog.processors.add_log_level,
                structlog.processors.EventRenamer(to= "event"),
                structlog.processors.JSONRenderer()
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use= True,
        )
        return structlog.get_logger(logger_name)

# --- Usage Example ---

if __name__ == "__main__":
    log= CustomLogger().get_logger(__file__)
    log.info("user uploaded a file", user_id= 123, filename= "report.pdf")
    log.error("Failed to process PDF", error= "File not found", user_id= 1234)