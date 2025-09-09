"""
Enhanced file processor that supports multiple file formats.
Handles CSV, JSON, Excel, Parquet, and other formats with robust error handling.
"""

import pandas as pd
import json
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import chardet
from io import StringIO, BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileProcessor:
    """Enhanced file processor supporting multiple formats with error handling"""
    
    SUPPORTED_FORMATS = {
        'csv': 'Comma Separated Values',
        'json': 'JavaScript Object Notation',
        'jsonl': 'JSON Lines',
        'xlsx': 'Excel Spreadsheet',
        'xls': 'Excel Spreadsheet (Legacy)',
        'parquet': 'Apache Parquet',
        'txt': 'Text File',
        'tsv': 'Tab Separated Values',
        'dat': 'Data File',
        'xml': 'XML Document'
    }
    
    def __init__(self):
        self.encoding_fallbacks = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'iso-8859-1']
    
    def detect_encoding(self, file_path: str) -> str:
        """Detect file encoding using chardet"""
        try:
            with open(file_path, 'rb') as file:
                raw_data = file.read(10000)  # Read first 10KB for detection
                result = chardet.detect(raw_data)
                detected_encoding = result.get('encoding', 'utf-8')
                confidence = result.get('confidence', 0)
                
                logger.info(f"Detected encoding: {detected_encoding} (confidence: {confidence:.2f})")
                
                # Use utf-8 as fallback for low confidence
                if confidence < 0.7:
                    logger.warning(f"Low confidence in encoding detection, using utf-8 as fallback")
                    return 'utf-8'
                
                return detected_encoding
        except Exception as e:
            logger.warning(f"Encoding detection failed: {str(e)}, using utf-8")
            return 'utf-8'
    
    def read_file_with_encoding_fallback(self, file_path: str, read_func, **kwargs) -> pd.DataFrame:
        """Try reading file with multiple encodings"""
        encodings_to_try = [self.detect_encoding(file_path)] + self.encoding_fallbacks
        
        for encoding in encodings_to_try:
            try:
                logger.info(f"Attempting to read file with encoding: {encoding}")
                if 'encoding' in kwargs:
                    kwargs['encoding'] = encoding
                return read_func(file_path, **kwargs)
            except UnicodeDecodeError as e:
                logger.warning(f"Encoding {encoding} failed: {str(e)}")
                continue
            except Exception as e:
                # If it's not an encoding error, re-raise
                if 'encoding' not in str(e).lower():
                    raise e
                logger.warning(f"Error with encoding {encoding}: {str(e)}")
                continue
        
        raise ValueError(f"Unable to read file {file_path} with any supported encoding")
    
    def read_csv_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read CSV file with enhanced error handling"""
        default_kwargs = {
            'encoding': 'utf-8',
            'low_memory': False,
            'na_values': ['', 'NULL', 'null', 'None', 'N/A', 'n/a', '#N/A'],
            'keep_default_na': True
        }
        default_kwargs.update(kwargs)
        
        try:
            return self.read_file_with_encoding_fallback(file_path, pd.read_csv, **default_kwargs)
        except Exception as e:
            # Try with different separators
            for sep in [',', ';', '\t', '|']:
                try:
                    logger.info(f"Trying separator: '{sep}'")
                    default_kwargs['sep'] = sep
                    return self.read_file_with_encoding_fallback(file_path, pd.read_csv, **default_kwargs)
                except Exception:
                    continue
            raise ValueError(f"Unable to read CSV file {file_path}: {str(e)}")
    
    def read_json_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read JSON file with multiple format support"""
        try:
            # Try reading as standard JSON first
            encoding = self.detect_encoding(file_path)
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                # Array of objects
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                # Single object or nested structure
                if all(isinstance(v, (dict, list)) for v in data.values()):
                    # Nested structure - try to normalize
                    return pd.json_normalize(data)
                else:
                    # Single record
                    return pd.DataFrame([data])
            else:
                raise ValueError("Unsupported JSON structure")
                
        except json.JSONDecodeError:
            # Try reading as JSON Lines format
            try:
                logger.info("Attempting to read as JSON Lines format")
                return pd.read_json(file_path, lines=True, **kwargs)
            except Exception as e:
                raise ValueError(f"Unable to parse JSON file {file_path}: {str(e)}")
    
    def read_excel_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read Excel file with sheet detection"""
        default_kwargs = {
            'engine': 'openpyxl' if file_path.endswith('.xlsx') else 'xlrd',
            'na_values': ['', 'NULL', 'null', 'None', 'N/A', 'n/a', '#N/A']
        }
        default_kwargs.update(kwargs)
        
        try:
            # Get all sheet names first
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            logger.info(f"Found sheets: {sheet_names}")
            
            # If no sheet specified, use the first sheet
            if 'sheet_name' not in default_kwargs:
                default_kwargs['sheet_name'] = sheet_names[0]
                logger.info(f"Using sheet: {sheet_names[0]}")
            
            return pd.read_excel(file_path, **default_kwargs)
            
        except Exception as e:
            raise ValueError(f"Unable to read Excel file {file_path}: {str(e)}")
    
    def read_parquet_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read Parquet file"""
        try:
            return pd.read_parquet(file_path, **kwargs)
        except Exception as e:
            raise ValueError(f"Unable to read Parquet file {file_path}: {str(e)}")
    
    def read_text_file(self, file_path: str, delimiter: str = '\t', **kwargs) -> pd.DataFrame:
        """Read delimited text file"""
        default_kwargs = {
            'sep': delimiter,
            'encoding': 'utf-8',
            'low_memory': False,
            'na_values': ['', 'NULL', 'null', 'None', 'N/A', 'n/a', '#N/A']
        }
        default_kwargs.update(kwargs)
        
        return self.read_file_with_encoding_fallback(file_path, pd.read_csv, **default_kwargs)
    
    def read_xml_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Read XML file (requires lxml)"""
        try:
            return pd.read_xml(file_path, **kwargs)
        except ImportError:
            raise ValueError("XML support requires lxml library. Install with: pip install lxml")
        except Exception as e:
            raise ValueError(f"Unable to read XML file {file_path}: {str(e)}")
    
    def load_data_from_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Load data from file automatically detecting format
        
        Args:
            file_path: Path to the data file
            **kwargs: Additional arguments passed to specific readers
            
        Returns:
            pd.DataFrame: Loaded data
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Get file extension
        ext = file_path.suffix.lower().lstrip('.')
        
        if ext not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported file format: {ext}. Supported formats: {list(self.SUPPORTED_FORMATS.keys())}")
        
        logger.info(f"Loading {self.SUPPORTED_FORMATS[ext]} file: {file_path}")
        
        try:
            # Route to appropriate reader
            if ext == 'csv':
                df = self.read_csv_file(str(file_path), **kwargs)
            elif ext in ['json', 'jsonl']:
                df = self.read_json_file(str(file_path), **kwargs)
            elif ext in ['xlsx', 'xls']:
                df = self.read_excel_file(str(file_path), **kwargs)
            elif ext == 'parquet':
                df = self.read_parquet_file(str(file_path), **kwargs)
            elif ext in ['txt', 'tsv', 'dat']:
                delimiter = '\t' if ext == 'tsv' else kwargs.get('delimiter', '\t')
                df = self.read_text_file(str(file_path), delimiter=delimiter, **kwargs)
            elif ext == 'xml':
                df = self.read_xml_file(str(file_path), **kwargs)
            else:
                raise ValueError(f"Handler not implemented for format: {ext}")
            
            logger.info(f"Successfully loaded {len(df)} records from {file_path}")
            logger.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {str(e)}")
            raise
    
    def convert_dataframe_to_records(self, df: pd.DataFrame, 
                                   clean_data: bool = True, 
                                   drop_empty_rows: bool = True) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to list of dictionaries with optional cleaning
        
        Args:
            df: Input DataFrame
            clean_data: Whether to clean/normalize data
            drop_empty_rows: Whether to drop rows with all NaN values
            
        Returns:
            List[Dict[str, Any]]: List of record dictionaries
        """
        if clean_data:
            df = self._clean_dataframe(df)
        
        if drop_empty_rows:
            df = df.dropna(how='all')
        
        # Convert to records
        records = df.to_dict('records')
        
        logger.info(f"Converted DataFrame to {len(records)} records")
        return records
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame by standardizing column names and handling missing values"""
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = df_clean.columns.str.strip()  # Remove whitespace
        df_clean.columns = df_clean.columns.str.replace(' ', '_')  # Replace spaces with underscores
        
        # Handle missing values
        df_clean = df_clean.replace(['', 'NULL', 'null', 'None'], pd.NA)
        
        # Strip whitespace from string columns
        string_columns = df_clean.select_dtypes(include=['object']).columns
        for col in string_columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            # Convert back to NaN where appropriate
            df_clean[col] = df_clean[col].replace(['nan', 'None'], pd.NA)
        
        logger.info("DataFrame cleaned and standardized")
        return df_clean
    
    def save_dataframe(self, df: pd.DataFrame, output_path: str, format: str = None, **kwargs):
        """
        Save DataFrame to file in specified format
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            format: Output format (auto-detected if None)
            **kwargs: Additional arguments for save functions
        """
        output_path = Path(output_path)
        
        if format is None:
            format = output_path.suffix.lower().lstrip('.')
        
        logger.info(f"Saving DataFrame to {output_path} in {format} format")
        
        # Create directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            if format == 'csv':
                df.to_csv(output_path, index=False, **kwargs)
            elif format == 'json':
                df.to_json(output_path, orient='records', indent=2, **kwargs)
            elif format in ['xlsx', 'xls']:
                df.to_excel(output_path, index=False, **kwargs)
            elif format == 'parquet':
                df.to_parquet(output_path, index=False, **kwargs)
            else:
                raise ValueError(f"Unsupported output format: {format}")
            
            logger.info(f"Successfully saved {len(df)} records to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving file {output_path}: {str(e)}")
            raise
    
    @classmethod
    def get_supported_formats(cls) -> Dict[str, str]:
        """Get dictionary of supported file formats"""
        return cls.SUPPORTED_FORMATS.copy()
