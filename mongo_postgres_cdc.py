import json
import logging
import time
import signal
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional, Union
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
    'topics': os.getenv('KAFKA_TOPICS', 'default-topic').split(','),
    'group_id': os.getenv('KAFKA_GROUP_ID', 'postgresql-consumer-group'),
    'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
    'consumer_timeout_ms': int(os.getenv('KAFKA_CONSUMER_TIMEOUT_MS', '10000')),
    'max_poll_records': int(os.getenv('KAFKA_MAX_POLL_RECORDS', '100'))
}

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DATABASE', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# Validate required environment variables
required_env_vars = ['POSTGRES_PASSWORD']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]

if missing_vars:
    logger.error(f"Missing required environment variables: {missing_vars}")
    sys.exit(1)

logger.info("Configuration loaded successfully")
logger.info(f"Kafka topics: {KAFKA_CONFIG['topics']}")

class GenericKafkaPostgreSQLConsumer:
    def __init__(self):
        self.pg_conn = None
        self.consumer = None
        self.running = False
        
        # Generic processing configuration
        self.max_nesting_depth = int(os.getenv('MAX_NESTING_DEPTH', '2'))
        self.max_array_elements = int(os.getenv('MAX_ARRAY_ELEMENTS', '10'))
        self.created_tables = set()  # Track created tables
        
    def connect_postgresql(self):
        """Establish PostgreSQL connection"""
        try:
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.pg_conn.autocommit = False  # Use transactions
            logger.info("Connected to PostgreSQL successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def get_table_name(self, topic: str) -> str:
        """Convert topic name to table name generically"""
        # Clean and convert topic to valid PostgreSQL table name
        table_name = topic.replace('.', '_').replace('-', '_').lower()
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        
        # Handle specific known patterns (optional - you can remove this)
        if 'circoposts' in table_name:
            return 'posts'
        elif 'users' in table_name:
            return 'users'
        
        return table_name or 'kafka_data'
    
    def extract_all_data(self, data: Dict[str, Any], prefix: str = "", depth: int = 0) -> Dict[str, Any]:
        """Generic extraction of all data with dynamic flattening"""
        extracted = {}
        
        for key, value in data.items():
            column_name = f"{prefix}{key}" if prefix else key
            
            try:
                processed_value = self._process_any_value(column_name, value, depth)
                
                # Handle flattened nested objects
                if isinstance(processed_value, dict) and processed_value.get('_is_flattened'):
                    for nested_key, nested_value in processed_value.items():
                        if nested_key != '_is_flattened':
                            extracted[nested_key] = nested_value
                else:
                    extracted[column_name] = processed_value
                    
            except Exception as e:
                logger.warning(f"Error processing {column_name}: {e}")
                extracted[column_name] = json.dumps(value) if value is not None else None
        
        return extracted

    def _process_any_value(self, field_name: str, value: Any, depth: int = 0) -> Any:
        """Generic value processing for any data type"""
        
        if value is None:
            return None
        
        # Handle primitives
        if isinstance(value, (str, int, float, bool)):
            return self._convert_primitive(value)
        
        # Handle nested objects
        elif isinstance(value, dict):
            return self._handle_any_object(field_name, value, depth)
        
        # Handle arrays
        elif isinstance(value, list):
            return self._handle_any_array(field_name, value, depth)
        
        # Fallback
        else:
            return str(value)

    def _convert_primitive(self, value: Any) -> Any:
        """Convert primitive values with smart type detection"""
        
        if isinstance(value, str):
            if not value.strip():
                return None
            return value
        
        elif isinstance(value, int):
            # Smart timestamp detection
            if 1000000000 <= value <= 9999999999:  # 10-digit seconds
                try:
                    return datetime.fromtimestamp(value)
                except (ValueError, OSError):
                    pass
            elif 1000000000000 <= value <= 9999999999999:  # 13-digit milliseconds
                try:
                    return datetime.fromtimestamp(value / 1000)
                except (ValueError, OSError):
                    pass
            return value
        
        elif isinstance(value, float):
            if value != value:  # NaN check
                return None
            return value
        
        return value

    def _handle_any_object(self, field_name: str, obj: Dict[str, Any], depth: int) -> Any:
        """Generic nested object handling"""
        
        if not obj:
            return None
        
        # Decide whether to flatten based on complexity
        should_flatten = (
            depth < self.max_nesting_depth and
            len(obj) <= 5 and  # Simple objects only
            all(isinstance(v, (str, int, float, bool, type(None))) for v in obj.values())
        )
        
        if should_flatten:
            # Flatten the object
            flattened = {'_is_flattened': True}
            for key, value in obj.items():
                new_key = f"{field_name}_{key}"
                processed_value = self._process_any_value(new_key, value, depth + 1)
                flattened[new_key] = processed_value
            return flattened
        else:
            # Keep as JSON
            return json.dumps(obj)

    def _handle_any_array(self, field_name: str, arr: list, depth: int) -> Any:
        """Generic array handling"""
        
        if not arr:
            return None
        
        # Always store arrays as JSON for simplicity
        return json.dumps(arr)

    def _extract_primary_key(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract primary key using common patterns"""
        
        # Try common primary key field names
        primary_key_candidates = ['_id', 'id', 'uuid', 'key', 'pk', 'objectId']
        
        for candidate in primary_key_candidates:
            if candidate in data and data[candidate]:
                return str(data[candidate])
        
        # Find any field ending with 'id'
        for key, value in data.items():
            if key.lower().endswith('id') and value:
                return str(value)
        
        return None

    def _clean_column_name(self, name: str) -> str:
        """Clean column names for PostgreSQL compatibility"""
        cleaned = name.lower().replace('-', '_').replace(' ', '_').replace('.', '_')
        cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
        
        if cleaned and cleaned[0].isdigit():
            cleaned = f"col_{cleaned}"
        
        return cleaned or "unknown_column"

    def _get_postgres_type(self, value: Any) -> str:
        """Determine PostgreSQL column type from value"""
        
        if value is None:
            return "TEXT"
        
        if isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "BIGINT" 
        elif isinstance(value, float):
            return "DOUBLE PRECISION"
        elif isinstance(value, datetime):
            return "TIMESTAMP"
        else:
            return "TEXT"

    def create_table_dynamically(self, cursor, table_name: str, sample_data: Dict[str, Any]):
        """Create table based on data structure"""
        
        if table_name in self.created_tables:
            return  # Already created
        
        columns_def = ['primary_key TEXT PRIMARY KEY']
        
        for key, value in sample_data.items():
            if key == 'primary_key':
                continue
                
            clean_key = self._clean_column_name(key)
            pg_type = self._get_postgres_type(value)
            columns_def.append(f'"{clean_key}" {pg_type}')
        
        # Add metadata columns
        columns_def.extend([
            'raw_data JSONB',  # Always store original JSON
            'kafka_topic TEXT',
            'processed_at TIMESTAMP DEFAULT NOW()',
            'updated_at TIMESTAMP DEFAULT NOW()'
        ])
        
        create_query = f'''
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {', '.join(columns_def)}
        )
        '''
        
        try:
            cursor.execute(create_query)
            
            # Create index on primary_key
            cursor.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_primary_key" ON "{table_name}" (primary_key)')
            
            # Create index on kafka_topic
            cursor.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_topic" ON "{table_name}" (kafka_topic)')
            
            self.created_tables.add(table_name)
            logger.info(f"Created table {table_name} with {len(columns_def)} columns")
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def process_message(self, topic: str, key: bytes, value: bytes) -> bool:
        """Process any Kafka message structure dynamically"""
        cursor = self.pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # Parse message
            value_data = json.loads(value.decode('utf-8')) if value else {}
            
            if not value_data:
                logger.warning(f"Empty value data for topic {topic}")
                return False
            
            # Extract primary key
            primary_key = self._extract_primary_key(value_data)
            if not primary_key:
                logger.warning(f"No primary key found in message from topic {topic}")
                return False
            
            # Extract all columns generically
            extracted_data = self.extract_all_data(value_data)
            
            # Handle column conflicts generically
            reserved_columns = {
                'kafka_primary_key', 'kafka_raw_data', 'kafka_topic', 
                'kafka_updated_at'
            }
            
            clean_data = {}
            for key_name, value_item in extracted_data.items():
                clean_key = self._clean_column_name(key_name)
                
                # Handle conflicts with reserved names
                original_key = clean_key
                counter = 1
                
                while clean_key in reserved_columns or clean_key in clean_data:
                    clean_key = f"{original_key}_{counter}"
                    counter += 1
                
                # Log if we had to rename
                if clean_key != original_key:
                    logger.debug(f"Renamed conflicting column {original_key} to {clean_key}")
                
                clean_data[clean_key] = value_item
            
            # Add kafka metadata
            clean_data['kafka_primary_key'] = primary_key
            clean_data['kafka_raw_data'] = json.dumps(value_data)
            clean_data['kafka_topic'] = topic
            clean_data['kafka_updated_at'] = datetime.now()
            
            # Get table name and process
            table_name = self.get_table_name(topic)
            
            if table_name not in self.created_tables:
                self.create_table_dynamically(cursor, table_name, clean_data)
            
            self._upsert_record(cursor, table_name, clean_data)
            
            self.pg_conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error processing message from topic {topic}: {e}")
            self.pg_conn.rollback()
            return False
        finally:
            cursor.close()


    def _upsert_record(self, cursor, table_name: str, data: Dict[str, Any]):
        """Generic upsert using kafka_primary_key as the conflict key"""
        
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ['%s'] * len(values)
        
        columns_str = ', '.join(f'"{col}"' for col in columns)
        placeholders_str = ', '.join(placeholders)
        
        # Update all columns except the primary key
        update_columns = [col for col in columns if col != 'kafka_primary_key']
        update_set = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        
        upsert_query = f"""
        INSERT INTO "{table_name}" ({columns_str})
        VALUES ({placeholders_str})
        ON CONFLICT (kafka_primary_key) 
        DO UPDATE SET {update_set}
        """
        
        cursor.execute(upsert_query, values)

    def setup_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                *KAFKA_CONFIG['topics'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id=KAFKA_CONFIG['group_id'],
                auto_offset_reset=KAFKA_CONFIG['auto_offset_reset'],
                consumer_timeout_ms=KAFKA_CONFIG['consumer_timeout_ms'],
                max_poll_records=KAFKA_CONFIG['max_poll_records'],
                value_deserializer=lambda x: x,
                key_deserializer=lambda x: x
            )
            
            logger.info(f"Kafka consumer created for topics: {KAFKA_CONFIG['topics']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return False
    
    def signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def run(self):
        """Main consumer loop"""
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Connect to PostgreSQL
        if not self.connect_postgresql():
            return 1
        
        # Setup Kafka consumer
        if not self.setup_kafka_consumer():
            return 1
        
        # Start processing
        logger.info("Starting generic message processing...")
        self.running = True
        processed_count = 0
        error_count = 0
        
        try:
            while self.running:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                    
                    # Process each message
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                success = self.process_message(
                                    message.topic,
                                    message.key,
                                    message.value
                                )
                                
                                if success:
                                    processed_count += 1
                                else:
                                    error_count += 1
                                    
                            except Exception as e:
                                logger.error(f"Error processing individual message: {e}")
                                error_count += 1
                    
                    # Commit offsets after successful batch
                    self.consumer.commit()
                    
                    # Log progress
                    if processed_count % 100 == 0 and processed_count > 0:
                        logger.info(f"Processed: {processed_count}, Errors: {error_count}, Tables created: {len(self.created_tables)}")
                        
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    time.sleep(5)
                    
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        
        finally:
            # Cleanup
            if self.consumer:
                self.consumer.close()
            if self.pg_conn:
                self.pg_conn.close()
            
            logger.info(f"Consumer stopped. Final stats - Processed: {processed_count}, Errors: {error_count}, Tables: {len(self.created_tables)}")
        
        return 0

def main():
    """Entry point"""
    logger.info("Starting Generic Kafka-PostgreSQL Consumer")
    
    consumer = GenericKafkaPostgreSQLConsumer()
    exit_code = consumer.run()
    
    logger.info("Consumer finished")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
