from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from typing import List, Optional
import yaml
from pathlib import Path

class DataSourceConfig(BaseModel):
    """Data source configuration"""
    provider: str = Field(default="yfinance", description="Data provider name")
    period: str = Field(default="3y", description="Historical data period")
    interval: str = Field(default="1d", description="Data interval")
    
    @field_validator('period')
    def validate_period(cls, v):
        valid_periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '3y', '5y', '10y', 'ytd', 'max']
        if v not in valid_periods:
            raise ValueError(f'Period must be one of {valid_periods}')
        return v
    
    @field_validator('interval')
    def validate_interval(cls, v):
        valid_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']
        if v not in valid_intervals:
            raise ValueError(f'Interval must be one of {valid_intervals}')
        return v
    
class TickersConfig(BaseModel):
    us_stocks: List[str] = Field(default_factory=list, description="US stock tickers")
    indian_stocks: List[str] = Field(default_factory=list, description="Indian stock tickers")
    
    # @field_validator(cls, v)
    def validate_tickers(cls, v):
        if not v:
            raise ValueError("Ticker list cannot be empty")
        return v
    def get_all_tickers(self, indian:bool= False) -> List[str]:
        if indian: 
            return self.indian_stocks
        else:
            return self.us_stocks
    
class StorageConfig(BaseModel):
    """Storage configuration"""
    provider: str = Field(default="gcs", description="Storage provider")
    raw_data_path: str = Field(default="raw", description="Path for raw data")
    file_format: str = Field(default="csv", description="Output file format")
    include_timestamp: bool = Field(default=True, description="Include timestamp in filename")
    include_index: bool = Field(default=True, description="Include index in CSV")
    
    @field_validator('file_format')
    def validate_format(cls, v):
        valid_formats = ['csv', 'parquet', 'json']
        if v not in valid_formats:
            raise ValueError(f'File format must be one of {valid_formats}')
        return v
    
class PipelineConfig(BaseModel):
    rate_limit_delay: int = Field
    rate_limit_delay: int = Field(default=2, ge=0, description="Delay between API calls (seconds)")
    retry_attempts: int = Field(default=1, ge=1, le=10, description="Number of retry attempts")
    retry_delay: int = Field(default=5, ge=1, description="Delay between retries (seconds)")
    enable_validation: bool = Field(default=True, description="Enable data validation")
    enable_logging: bool = Field(default=True, description="Enable logging")

class Config(BaseModel):
    """Main configuration model"""
    data_source: DataSourceConfig
    tickers: TickersConfig
    storage: StorageConfig
    pipeline: PipelineConfig
    
    @classmethod
    def from_yaml(cls, config_path: str = None) -> "Config":
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config.yaml file
            
        Returns:
            Config object
        """
        if config_path is None:
            # Default path relative to this file
            config_dir = Path(__file__).parent
            config_path = config_dir / "config.yaml"
        
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        return cls(**config_dict)

class Settings(BaseSettings):
    """
    Environment-based settings using Pydantic BaseSettings.
    These are loaded from .env file or environment variables.
    """
    bucket_name: str = Field(..., description="GCS bucket name")
    google_application_credentials: Optional[str] = Field(
        None, 
        description="Path to GCP service account key"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"


# Global configuration instances
def load_config(config_path: str = None) -> Config:
    """Load and validate configuration"""
    return Config.from_yaml(config_path)


def load_settings() -> Settings:
    """Load and validate environment settings"""
    return Settings()