import sys
import traceback
from typing import Optional, cast

class AutomatedDataPipeline(Exception):
    def __init__(self, error_message, error_details: Optional[object] = None):
        # Normalize message
        if isinstance(error_message, BaseException):
            norm_msg= str(error_message) #f"{error_message.__class__.__name__}: {error_message}"
        else:
            norm_msg = str(error_message)

        # Resolve exc_info (supports: sys module, Exception object, or current context)

        exc_type= exc_value = exc_tb = None
# -- -------------Core start------------------#
        if error_details is None:
            exc_type, exc_value, exc_tb = sys.exc_info()
        else:
            if hasattr(error_details, "exc_info"):
                exc_info_obj= cast(sys, error_details)
                exc_type, exc_value, exc_tb = exc_info_obj.exc_info()

            elif isinstance(error_details, BaseException):
                exc_type, exc_value, exc_tb = type(error_details), error_details, error_details.__traceback__
            else:
                exc_type, exc_value, exc_tb = sys.exc_info()
# - ----------------Core end-----------------------------

        ## Walk to the last frame to report the most relevant location

        last_tb= exc_tb
        while last_tb and last_tb.tb_next:
            last_tb = last_tb.tb_next

        self.file_name= last_tb.tb_frame.f_code.co_filename if last_tb else '<unknown>'
        self.lineno= last_tb.tb_lineno if last_tb else -1
        self.error_message= norm_msg

        # Full pretty traceback (if available)

        if exc_type and exc_tb:
            self.traceback_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        else:
            self.traceback_str = ""

        super().__init__(self.__str__())

    def __str__(self):
        # Compact logger friendly message (no leading spaces)
        base= f"Error in [{self.file_name}] at line[{self.lineno}] | Message: {self.error_message}"
        if self.traceback_str:
            return f"{base}\n{self.traceback_str}"
        return base

    def __repr__(self):
        return f"DocumentPortalException(file= {self.file_name}), lineno= {self.lineno}, message= {self.error_message}"

# DATA INGESTION PIPELINE SPECIFIC EXCEPTIONS

class DataIngestionException(AutomatedDataPipeline):
    """Base exception for all pipeline errors"""
    def __init__(self, message, ticker=None, details=None):
        self.ticker = ticker
        self.details = details
        super().__init__(message)


class DataDownloadException(DataIngestionException):
    """Raised when data download from yfinance fails"""
    pass


class DataValidationException(DataIngestionException):
    """Raised when data validation fails"""
    pass


class GCSUploadException(DataIngestionException):
    """Raised when upload to Google Cloud Storage fails"""
    pass


class GCSConnectionException(DataIngestionException):
    """Raised when connection to GCS fails"""
    pass


class ConfigurationException(DataIngestionException):
    """Raised when configuration is invalid or missing"""
    pass


class DataProcessingException(DataIngestionException):
    """Raised when data processing/transformation fails"""
    pass