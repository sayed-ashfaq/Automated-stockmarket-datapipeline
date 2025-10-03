# ingest the data from yfinance to google cloud bucket
import datetime
import time
import sys
from pathlib import Path
import os
from dotenv import load_dotenv


import pandas as pd
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
import yfinance as yf

from config.settings import load_config, load_settings
from logging_handler import GLOBAL_LOGGER as log 
from exception_handler.custom_exceptions import *

class StockDataIngestionPipeline:
    def __init__(self, tickers = None):

        log.info("Initializing StockDataIngestionPipeline")
        load_dotenv()

        self.bucket_name = os.getenv('BUCKET_NAME')
        self._load_configuration()
        self._initialize_gcs_client()
        
        self.tickers = tickers

        log.info("Pipeline initialization completed successfully")

    def _load_configuration(self):
        try:
            self.config = load_config()
            
            self.settings = load_settings()
            #  Log key configurations
            log.info(f"Data Source: {self.config.data_source.provider}")
            log.info(f"Period: {self.config.data_source.period}, Interval: {self.config.data_source.interval}")
            log.info(f"Total tickers: {len(self.config.tickers.get_all_tickers())}")
            log.info(f"GCS Bucket: {self.settings.bucket_name}")

            log.info(f"Configuration loaded successfully. Bucket: {self.bucket_name}")
        except FileNotFoundError as e:
            log.error(f"Configuration file not found: {str(e)}")
            raise ConfigurationError(
                "Configuration file not found",
                details="Ensure config.yaml exists in config/ directory"
            )
        except Exception as e:
            log.error(f"Failed to load configuration: {str(e)}")
            raise ConfigurationError(
                f"Configuration loading failed: {str(e)}"
            )
    
    def _initialize_gcs_client(self):
        try:
            self.client = storage.Client()
            self.export_bucket = self.client.get_bucket(self.bucket_name)

            log.info(f"Successfully connected to GCS bucket: {self.bucket_name}")
        except gcp_exceptions.NotFound:
            log.error(f"GCS bucket not found: {self.bucket_name}")
            raise GCSConnectionError(
                f"Bucket '{self.bucket_name}' does not exist",
                details="Verify bucket name and ensure it exists in your GCP project"
            )
        except gcp_exceptions.Forbidden:
            log.error(f"Access denied to GCS bucket: {self.bucket_name}")
            raise GCSConnectionError(
                f"Access denied to bucket '{self.bucket_name}'",
                details="Check your GCP credentials and bucket permissions"
            )
        except Exception as e:
            log.error(f"Failed to initialize GCS client: {str(e)}")
            raise GCSConnectionError(
                f"GCS connection failed: {str(e)}",
                details="Verify credentials and network connectivity"
            )
        
    def load_and_process_data(self, ticker_symbol: str) -> pd.DataFrame:
        try:
            log.info(f"Downloading data for {ticker_symbol}")
            df = yf.download(
                ticker_symbol,
                period=self.config.data_source.period,
                interval=self.config.data_source.interval,
                progress=False
            )
            if df.empty:
                raise DataDownloadError(
                    f"No data available for {ticker_symbol}",
                    ticker=ticker_symbol,
                    details=f"Period: {self.config.data_source.period}, Interval: {self.config.data_source.interval}"
                )
            
            # Process multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns= df.columns.get_level_values(0)

            # Reset index to make 'Date' a column
            df.reset_index(inplace=True)

            log.info(f"Data for {ticker_symbol} downloaded and processed successfully")
            return df
            
            
        except DataDownloadError:
            raise
        except Exception as e:
            log.error(f"Failed to download data for {ticker_symbol}: {str(e)}")
            raise DataDownloadError(
                f"Data download failed for {ticker_symbol}: {str(e)}",
                ticker=ticker_symbol
            )
        
    def _validate_data(self, df: pd.DataFrame, ticker_symbol: str):
        try:
            required_columns= {"Open", "High", "Low", "Close", "Volume"}
            missing_columns = required_columns - set(df.columns)

            if missing_columns:
                raise DataValidationError(
                    f"Missing columns {missing_columns} in data for {ticker_symbol}",
                    ticker=ticker_symbol,
                    details=f"Required columns: {required_columns}"
                )
            # Check for negative values in price columns
            price_columns = ['Open', 'High', 'Low', 'Close']
            for col in price_columns:
                if (df[col] < 0).any():
                    log.warning(f"Negative values found in {col} column for {ticker_symbol}")
            
            log.debug(f"Data validation passed for {ticker_symbol}")
            
        except DataValidationError:
            raise
        except Exception as e:
            log.error(f"Data validation failed for {ticker_symbol}: {str(e)}")
            raise DataValidationError(
                f"Validation error for {ticker_symbol}: {str(e)}",
                ticker=ticker_symbol
            )
            
    
    def upload_to_gcs(self, df: pd.DataFrame, ticker_symbol: str) -> str:
        try:
            #generate file name
            if self.config.storage.include_timestamp:
                current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                file_name = f'{ticker_symbol}_{current_time}.csv'
            else:
                file_name = f'{ticker_symbol}.csv'

            # Create blob path
            blob_path = f'{self.config.storage.raw_data_path}/{ticker_symbol}/{file_name}'
            export_blob = self.export_bucket.blob(blob_path)

            # convert dataframe to csv and upload to gcs
            csv_data = pd.DataFrame(df.values).to_csv(header=False, index=False)
            export_blob.upload_from_string(csv_data, content_type='text/csv')

            log.info(
                f"Successfully uploaded {file_name} to {blob_path}"
                f" in bucket {self.bucket_name}"
            )
            return blob_path

        except gcp_exceptions.Forbidden:
            log.error(f"Permission denied when uploading {ticker_symbol}")
            raise GCSUploadError(
                f"Access denied when uploading {ticker_symbol}",
                ticker=ticker_symbol,
                details="Check bucket write permissions"
            )
        except Exception as e:
            log.error(f"Failed to upload {ticker_symbol} to GCS: {str(e)}")
            raise GCSUploadError(
                f"Upload failed for {ticker_symbol}: {str(e)}",
                ticker=ticker_symbol,
                details=f"Target path: {blob_path}"
            )


    def process_ticker(self, ticker_symbol: str, retry: int= 0) -> bool:
        try:
            log.info(f"Starting processing for {ticker_symbol}, Attempt: {retry+1}")
            # step 1: load and process data
            df = self.load_and_process_data(ticker_symbol)

            # validate
            self._validate_data(df, ticker_symbol)

            # step 2: upload to gcs
            blob_path = self.upload_to_gcs(df, ticker_symbol)
            
            log.info(f"Successfully Processed {ticker_symbol}")
            return True
        except (DataDownloadError, DataValidationError, GCSUploadError) as e:
            log.error(f"Error processing {ticker_symbol}: {str(e)}")
            return False

            # Retry logic
            if retry < self.config.pipeline.retry_attempts - 1:
                log.info(f"Retrying {ticker_symbol} in {self.config.pipeline.retry_delay} seconds...")
                time.sleep(self.config.pipeline.retry_delay)
                return self.process_ticker(ticker_symbol, retry + 1)
            else:
                log.error(f"Failed to process {ticker_symbol} after {self.config.pipeline.retry_attempts} attempts")
                return False
            
        except Exception as e:
            log.error(f"Unexpected error processing {ticker_symbol}: {str(e)}")
            return False

    def run(self, ticker_list: list = None):
        log.info("Starting Stock Data Ingestion Pipeline")
        if ticker_list is None:
            tickers = self.config.tickers.get_all_tickers(indian= True)
        else:
            tickers = ticker_list

        log.info(f"Processing tickers: {tickers}")
        successful = []
        failed = []

        for ticker in tickers:
            log.info('-' * 50)
            if self.process_ticker(ticker):
                successful.append(ticker)
            else:
                failed.append(ticker)
            
            if ticker != tickers[-1]:
                log.debug(f"Waiting {self.config.pipeline.rate_limit_delay} seconds (rate limiting)...")
                time.sleep(self.config.pipeline.rate_limit_delay)
        
        # Log summary
        log.info("=" * 60)
        log.info("Pipeline Execution Summary")
        log.info("=" * 60)
        log.info(f"Total Tickers: {len(tickers)}")
        log.info(f"Successful: {len(successful)}")
        log.info(f"Failed: {len(failed)}")
        
        if successful:
            log.info(f"Successfully processed: {', '.join(successful)}")
        
        if failed:
            log.warning(f"Failed to process: {', '.join(failed)}")
        
        log.info("=" * 60)
        log.info("Pipeline execution completed")
        log.info("=" * 60)
    
def main():
    try:
        pipeline = StockDataIngestionPipeline()
        pipeline.run(['TSLA'])
    except (ConfigurationError, GCSConnectionError) as e:
        log.critical(f"Pipeline initialization failed: {str(e)}")
        sys.exit(1)
    except Exception as e:
        log.critical(f"Unexpected error in main: {str(e)}")
        sys.exit(1)
    
if __name__ == "__main__":
    main()


# =========================================================================
# #step 1: load the data into dataframe
# US_tickers=  ['TSLA', 'META', 'NVDA', 'PLTR', 'INTC']
# indian_tickers = ['TATAMOTORS.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS']



# # step 2: upload the df into cloud storage service

# load_dotenv()   

# client = storage.Client()
# export_bucket = client.get_bucket(os.getenv('BUCKET_NAME')) 

# def load_and_process_raw_data(ticker_symbol, period, interval):
#     df= yf.download(ticker_symbol, period=period, interval=interval)    
#     df.columns = df.columns.get_level_values(0)
#     df.reset_index(inplace=True)
#     return df

# def upload_to_gcs(df, ticker_symbol):
#     # create a blob object with the path where you want to store the file
#     current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
#     file_name = f'{ticker_symbol}_{current_time}.csv'
#     blob_path = f'raw/{ticker_symbol}/{file_name}'
#     export_blob = export_bucket.blob(blob_path)
    
#     # convert dataframe to csv and upload to gcs
#     csv_data = df.to_csv(index=True) #np.savetxt("Output.csv", df.values, delimiter= ",", fmt= "%s")
#     export_blob.upload_from_string(csv_data, content_type='text/csv')
#     # log.info(f'File {file_name} uploaded to {blob_path} in bucket {os.getenv("BUCKET_NAME")}')
#     return blob_path

# if __name__ == "__main__":
#     # for ticker in US_tickers:
#     #     df= load_data(ticker, period='3y', interval='1d')
#     #     blob_path = upload_to_gcs(df, ticker)
#     #     time.sleep(10)  # to avoid hitting API rate limits
#     indian_tickers = ['TATAMOTORS.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS']

#     for ticker in indian_tickers:
#         df= load_and_process_raw_data(ticker, period='3y', interval='1d')
#         blob_path = upload_to_gcs(df, ticker)
#         time.sleep(10)  # to avoid hitting API rate limits
#     # log.info("Data ingestion completed successfully.")


#============================================================================================

# make a pipeline where data is getting loaded every month to the gcs bucket 