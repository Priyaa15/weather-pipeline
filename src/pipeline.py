"""
Real-Time Weather Data Pipeline
A complete ETL pipeline that extracts weather data from OpenWeather API,
transforms it, and loads it into a PostgreSQL database.
"""

import os 
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

"1. import OS - access OS functions, such as env variables, file paths etc - eg to get api key from env variable"
"2. import json - to work with json data to read API data"
"3. import logging - track logs and debugg"
"4. pandas - data manipulation in Python to read csv, dataframes and filtering https://pandas.pydata.org/docs/getting_started/intro_tutorials/ "


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Base = declarative_base()

# Database Model
class WeatherData(Base):
    """SQLAlchemy model for weather data"""
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False)
    country = Column(String(10))
    timestamp = Column(DateTime, nullable=False)
    temperature_celsius = Column(Float)
    temperature_fahrenheit = Column(Float)
    feels_like_celsius = Column(Float)
    humidity = Column(Integer)
    pressure = Column(Integer)
    weather_condition = Column(String(100))
    weather_description = Column(String(255))
    wind_speed = Column(Float)
    cloudiness = Column(Integer)
    is_raining = Column(Boolean, default=False)
    extraction_time = Column(DateTime, default=datetime.utcnow)

class WeatherPipeline:
    """Main ETL pipeline for weather data"""
    
    def __init__(self, api_key: str, db_connection_string: str):
        """
        Initialize the pipeline
        
        Args:
            api_key: OpenWeather API key
            db_connection_string: SQLAlchemy database connection string
        """
        self.api_key = api_key
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        self.engine = create_engine(db_connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def extract(self, cities: List[str]) -> List[Dict]:
        """
        Extract weather data from OpenWeather API
        
        Args:
            cities: List of city names to fetch weather for
            
        Returns:
            List of raw weather data dictionaries
        """
        logger.info(f"Starting extraction for {len(cities)} cities")
        raw_data = []
        
        for city in cities:
            try:
                params = {
                    'q': city,
                    'appid': self.api_key,
                    'units': 'metric'  # Get data in Celsius
                }
                
                response = requests.get(self.base_url, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                raw_data.append(data)
                logger.info(f"Successfully extracted data for {city}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to extract data for {city}: {str(e)}")
                continue
                
        logger.info(f"Extraction complete. Retrieved {len(raw_data)} records")
        return raw_data
    
    def transform(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Transform raw weather data into structured format
        
        Args:
            raw_data: List of raw API responses
            
        Returns:
            Transformed pandas DataFrame
        """
        logger.info("Starting transformation")
        transformed_records = []
        
        for record in raw_data:
            try:
                # Extract relevant fields
                transformed = {
                    'city': record['name'],
                    'country': record['sys']['country'],
                    'timestamp': datetime.fromtimestamp(record['dt']),
                    'temperature_celsius': record['main']['temp'],
                    'temperature_fahrenheit': self._celsius_to_fahrenheit(record['main']['temp']),
                    'feels_like_celsius': record['main']['feels_like'],
                    'humidity': record['main']['humidity'],
                    'pressure': record['main']['pressure'],
                    'weather_condition': record['weather'][0]['main'],
                    'weather_description': record['weather'][0]['description'],
                    'wind_speed': record['wind']['speed'],
                    'cloudiness': record['clouds']['all'],
                    'is_raining': 'rain' in record,
                    'extraction_time': datetime.utcnow()
                }
                
                # Data quality checks
                if self._validate_record(transformed):
                    transformed_records.append(transformed)
                else:
                    logger.warning(f"Record validation failed for {transformed['city']}")
                    
            except (KeyError, TypeError) as e:
                logger.error(f"Error transforming record: {str(e)}")
                continue
        
        df = pd.DataFrame(transformed_records)
        logger.info(f"Transformation complete. {len(df)} valid records")
        return df
    
    def load(self, df: pd.DataFrame) -> int:
        """
        Load transformed data into database
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Starting load of {len(df)} records")
        
        try:
            # Load data using pandas to_sql
            records_loaded = df.to_sql(
                'weather_data',
                self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {len(df)} records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise
    
    def run(self, cities: List[str]) -> Dict:
        """
        Run the complete ETL pipeline
        
        Args:
            cities: List of cities to process
            
        Returns:
            Pipeline execution summary
        """
        start_time = datetime.utcnow()
        logger.info("=" * 50)
        logger.info("Starting Weather Data Pipeline")
        logger.info("=" * 50)
        
        try:
            # Extract
            raw_data = self.extract(cities)
            
            # Transform
            df = self.transform(raw_data)
            
            # Load
            records_loaded = self.load(df)
            
            # Summary
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            summary = {
                'status': 'success',
                'cities_requested': len(cities),
                'records_extracted': len(raw_data),
                'records_loaded': records_loaded,
                'duration_seconds': duration,
                'timestamp': end_time.isoformat()
            }
            
            logger.info("=" * 50)
            logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            logger.info(f"Records loaded: {records_loaded}")
            logger.info("=" * 50)
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    @staticmethod
    def _celsius_to_fahrenheit(celsius: float) -> float:
        """Convert Celsius to Fahrenheit"""
        return (celsius * 9/5) + 32
    
    @staticmethod
    def _validate_record(record: Dict) -> bool:
        """
        Validate transformed record
        
        Args:
            record: Transformed data record
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields exist
        required_fields = ['city', 'temperature_celsius', 'humidity']
        if not all(field in record for field in required_fields):
            return False
        
        # Check temperature is reasonable (-100 to 60 Celsius)
        if not -100 <= record['temperature_celsius'] <= 60:
            logger.warning(f"Temperature out of range: {record['temperature_celsius']}°C")
            return False
        
        # Check humidity is valid (0-100%)
        if not 0 <= record['humidity'] <= 100:
            logger.warning(f"Humidity out of range: {record['humidity']}%")
            return False
        
        return True
    
    def close(self):
        """Close database connections"""
        self.session.close()
        self.engine.dispose()


# Example usage
if __name__ == "__main__":
    # Configuration (in production, use environment variables)
    API_KEY = os.getenv('OPENWEATHER_API_KEY', 'your_api_key_here')
    DB_CONNECTION = os.getenv('DB_CONNECTION', 'sqlite:///weather_data.db')
    
    # Cities to monitor
    CITIES = [
        'London',
        'New York',
        'Tokyo',
        'Paris',
        'Sydney',
        'Mumbai',
        'Toronto',
        'Berlin'
    ]
    
    # Run pipeline
    pipeline = WeatherPipeline(API_KEY, DB_CONNECTION)
    
    try:
        result = pipeline.run(CITIES)
        print(json.dumps(result, indent=2))
    finally:
        pipeline.close()