import sys
import traceback
from typing import Optional, cast


class AutomatedDataPipeline(Exception):
    def __init__(self, error_message, error_details: Optional[BaseException] = None):
        # Normalize message
        if isinstance(error_message, BaseException):
            self.error_message = str(error_message)
        else:
            self.error_message = str(error_message)

        # Determine traceback
        if error_details:
            exc_type = type(error_details)
            exc_value = error_details
            exc_tb = error_details.__traceback__
        else:
            exc_type, exc_value, exc_tb = sys.exc_info()

        # Walk to the last frame
        last_tb = exc_tb
        while last_tb and last_tb.tb_next:
            last_tb = last_tb.tb_next

        self.file_name = last_tb.tb_frame.f_code.co_filename if last_tb else '<unknown>'
        self.lineno = last_tb.tb_lineno if last_tb else -1

        # Full traceback string
        if exc_type and exc_tb:
            self.traceback_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        else:
            self.traceback_str = ""

        super().__init__(self.__str__())

    def __str__(self):
        base = f"Error in [{self.file_name}] at line[{self.lineno}] | Message: {self.error_message}"
        if self.traceback_str:
            return f"{base}\n{self.traceback_str}"
        return base

    def __repr__(self):
        return f"AutomatedDataPipeline(file={self.file_name}, lineno={self.lineno}, message={self.error_message})"


# DATA INGESTION PIPELINE SPECIFIC EXCEPTIONS

class DataIngestionException(AutomatedDataPipeline):
    """Base exception for all pipeline errors"""
    def __init__(self, message, ticker=None, details=None):
        self.ticker = ticker
        self.details = details
        super().__init__(message)



class DataDownloadError(DataIngestionException):
    """Raised when data validation fails"""
    pass

class DataValidationError(DataIngestionException):
    """Raised when data processing/transformation fails"""
    pass

class GCSUploadError(DataIngestionException):
    """Raised when upload to Google Cloud Storage fails"""
    pass


class GCSConnectionError(DataIngestionException):
    """Raised when connection to GCS fails"""
    pass


class ConfigurationError(DataIngestionException):
    """Raised when configuration is invalid or missing"""
    pass




if __name__ == "__main__":
    try:
        print("testing custom exceptions")
        2 / 0
    except Exception as e:
        raise DataDownloadError("Failed to download data")