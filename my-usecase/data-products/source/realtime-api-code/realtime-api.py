import nilus
import requests
import urllib.parse
import base64
import logging
from datetime import datetime
from nilus import CustomSource

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Helper function to flatten nested JSON dynamically
def flatten_json(nested_json, parent_key='', separator='_'):
    """
    Flatten a nested JSON object into a single-level dictionary.
    
    Args:
        nested_json: The nested JSON object (dict or list).
        parent_key: The parent key for nested structures (used in recursion).
        separator: The separator for nested keys.
    
    Returns:
        A flattened dictionary.
    """
    items = []
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            if isinstance(value, (dict, list)):
                items.extend(flatten_json(value, new_key, separator).items())
            else:
                items.append((new_key, value))
    elif isinstance(nested_json, list):
        for i, value in enumerate(nested_json):
            new_key = f"{parent_key}{separator}{i}" if parent_key else str(i)
            if isinstance(value, (dict, list)):
                items.extend(flatten_json(value, new_key, separator).items())
            else:
                items.append((new_key, value))
    return dict(items)

@nilus.source
def weather_source(base_url: str, location: str, units: str = None, apikey: str = None, table: str = None):
    """
    Retrieves realtime weather data from the Tomorrow.io Weather API and appends it to the specified table.
    This source fetches weather data for the given location, which can be in formats like latitude,longitude
    (e.g., '12.9155151,77.6158726'), city name (e.g., 'new york'), US zip code (e.g., '10001'), or UK postcode
    (e.g., 'SW1'). It extracts latitude and longitude from the API response's 'location' field, adds a 'load_datetime'
    column (UTC timestamp), a 'units' column (metric, imperial, or default metric if unspecified), and includes the
    'time' field from the API response. The data is appended to the sink without incremental state management.

    Args:
        base_url (str): Base URL for the Tomorrow.io API (e.g., https://api.tomorrow.io/v4/weather/realtime).
        location (str): Location for weather data (e.g., '12.9155151,77.6158726', 'new york', '10001', 'SW1').
        units (str, optional): Units for the weather data ('metric' or 'imperial', defaults to 'metric' if not specified).
        apikey (str): Decoded API key for Tomorrow.io.
        table (str): Table name to store the weather data (e.g., 'realtime_data01').
    """
    if not table or not isinstance(table, str):
        raise ValueError("table must be a non-empty string")
    if not base_url or not isinstance(base_url, str):
        raise ValueError("base_url must be a non-empty string")
    if not location or not isinstance(location, str):
        raise ValueError("location must be a non-empty string")
    if not apikey or not isinstance(apikey, str):
        raise ValueError("apikey must be a non-empty string")
    
    # Construct the API URL with parameters in order: base_url, location, units, apikey
    query_params = []
    query_params.append(f"location={location}")  # Avoid encoding location to prevent double-encoding
    if units:
        query_params.append(f"units={units}")
    query_params.append(f"apikey={apikey}")
    query_string = "&".join(query_params)
    full_url = f"{base_url}?{query_string}"
    
    # Redact apikey for logging
    redacted_url = f"{base_url}?location={location}&units={units}&apikey=***" if units else f"{base_url}?location={location}&apikey=***"
    logger.info(f"Fetching weather data from: {redacted_url}")
    
    def weather_records():
        load_datetime = datetime.utcnow().isoformat() + 'Z'  # UTC datetime in ISO format, same for all records
        
        try:
            # Make the API request
            response = requests.get(full_url)
            response.raise_for_status()
            data = response.json()
            
            # Log the raw response keys for debugging
            logger.info(f"API response keys: {list(data.keys())}")
            
            # For realtime API, extract the single data point
            record = data.get('data', {})
            if not record:
                logger.warning(f"No data found in response for location: {location}")
                return
            
            # Extract latitude and longitude from API response
            location_info = data.get('location', {})
            latitude = location_info.get('lat')
            longitude = location_info.get('lon')  # Tomorrow.io uses 'lon' for longitude
            
            if latitude is None or longitude is None:
                logger.warning(f"Latitude or longitude not found in API response for location: {location}")
            
            # Extract time from API response
            record_time = record.get('time')
            if not record_time:
                logger.warning(f"No time found in API response for location: {location}")
            
            # Flatten the 'values' field
            values = record.get('values', {})
            flattened_record = flatten_json(values)
            
            # Add additional columns
            flattened_record['latitude'] = latitude
            flattened_record['longitude'] = longitude
            flattened_record['time'] = record_time
            flattened_record['load_datetime'] = load_datetime
            flattened_record['units'] = units if units else 'metric'  # Default to 'metric' per API documentation
            
            logger.info(f"Yielding flattened record for table {table}: {flattened_record}")
            yield flattened_record
                
        except requests.RequestException as e:
            # Redact apikey in error message
            redacted_error = str(e).replace(apikey, '***') if apikey in str(e) else str(e)
            logger.error(f"Failed to fetch weather data for location: {location}: {redacted_error}")
            raise ValueError(f"Failed to fetch weather data: {redacted_error}")
        except Exception as e:
            # Redact apikey in error message
            redacted_error = str(e).replace(apikey, '***') if apikey in str(e) else str(e)
            logger.error(f"Error processing API response for location: {location}: {redacted_error}")
            raise ValueError(f"Error processing API response: {redacted_error}")
    
    # Yield the resource with specified parameters, ensuring table is last
    yield nilus.resource(
        weather_records,
        name="weather_api",
        table_name=table  # Table is the last parameter in the source configuration
    )

class WeatherRealtimeApiSource(CustomSource):
    def handles_incrementality(self) -> bool:
        # This source does not support incremental loading; it fetches and appends all data each time
        return False

    def nilus_source(self, uri: str, table: str, **kwargs):
        """
        Constructs the Nilus source from a URI containing the full API URL with plain-text location and units,
        and base64-encoded API key.

        Args:
            uri (str): URI containing the full weather API URL as a query param
                       (e.g., custom://WeatherRealtimeApiSource?url=https://api.tomorrow.io/v4/weather/realtime?location=12.9155151,77.6158726&units=metric&apikey={encoded_key}).
            table (str): Table name to store the weather data (e.g., 'realtime_data01').

        Returns:
            The Nilus source for the weather API data.

        Raises:
            ValueError: If URI is invalid, required parameters are missing, or apikey decoding fails.
        """
        # Redact apikey in URI for logging
        redacted_uri = uri
        if 'apikey=' in uri:
            redacted_uri = uri[:uri.index('apikey=')] + 'apikey=***'
        logger.info(f"Parsing URI: {redacted_uri}")

        try:
            # Parse the URI and extract full_url manually
            parsed_uri = urllib.parse.urlparse(uri)
            if not parsed_uri.query.startswith('url='):
                logger.error(f"Expected 'url=' as query parameter in URI: {redacted_uri}")
                raise ValueError("Missing 'url' parameter in URI")
            
            # Extract everything after 'url=' and unquote it
            full_url = urllib.parse.unquote(parsed_uri.query[4:])
            if not full_url:
                logger.error(f"Empty 'url' value in URI: {redacted_uri}")
                raise ValueError("Empty 'url' value in URI")
            
            # Redact apikey in full_url for logging
            redacted_full_url = full_url
            if 'apikey=' in full_url:
                redacted_full_url = full_url[:full_url.index('apikey=')] + 'apikey=***'
            logger.info(f"Extracted full_url: {redacted_full_url}")

            # Parse the full_url to extract base_url, location, units, and apikey
            parsed_full_url = urllib.parse.urlparse(full_url)
            url_query_params = urllib.parse.parse_qs(parsed_full_url.query, keep_blank_values=True)
            
            # Extract parameters in order: base_url, location, units, apikey
            location = url_query_params.get("location", [None])[0]
            units = url_query_params.get("units", [None])[0]
            encoded_apikey = url_query_params.get("apikey", [None])[0]
            
            if not location:
                logger.error(f"No 'location' parameter found in URL query: {redacted_full_url}")
                raise ValueError("Missing required 'location' in URL query parameters")
            if not encoded_apikey:
                logger.error(f"No 'apikey' parameter found in URL query: {redacted_full_url}")
                raise ValueError("Missing required 'apikey' in URL query parameters")
            
            # Clean location (plain text, not base64-encoded)
            location = urllib.parse.unquote(location).strip()
            if not location:
                logger.error(f"Location is empty after cleaning: {location}")
                raise ValueError("Location is empty after cleaning")
            
            # Construct base_url by removing location, units, and apikey from query
            clean_query = {k: v for k, v in url_query_params.items() if k not in ["location", "units", "apikey"]}
            clean_query_string = urllib.parse.urlencode(clean_query, doseq=True)
            base_url = f"{parsed_full_url.scheme}://{parsed_full_url.netloc}{parsed_full_url.path}"
            if clean_query_string:
                base_url += f"?{clean_query_string}"
            
            if not base_url:
                logger.error(f"Failed to extract base_url from: {redacted_full_url}")
                raise ValueError("Invalid base_url in URL")
            
            # Decode base64-encoded apikey
            try:
                apikey = base64.b64decode(encoded_apikey).decode("utf-8").strip()
                logger.info(f"Weather API credentials decoded successfully for location: {location}")
            except (TypeError, base64.binascii.Error, UnicodeDecodeError) as e:
                # Redact apikey in error message
                redacted_error = str(e).replace(encoded_apikey, '***') if encoded_apikey in str(e) else str(e)
                logger.error(f"Failed to decode base64-encoded apikey for location: {location}: {redacted_error}")
                raise ValueError(f"Invalid or missing base64-encoded apikey in URL: {redacted_error}")
                
        except Exception as e:
            # Redact apikey in error message
            redacted_error = str(e).replace(encoded_apikey, '***') if 'encoded_apikey' in locals() and encoded_apikey in str(e) else str(e)
            logger.error(f"Failed to parse URI: {redacted_uri}, error: {redacted_error}")
            raise ValueError(f"Invalid or missing parameters in URI: {redacted_error}")
        
        logger.info(f"Creating Weather API source for table: {table} with location: {location}")
        return weather_source(
            base_url=base_url,
            location=location,
            units=units,
            apikey=apikey,
            table=table
        )