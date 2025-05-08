import os
import asyncio
import logging
import json

import aioboto3
from dotenv import load_dotenv
from botocore.client import Config
from botocore.exceptions import ClientError
# Import necessary components from docling library
from docling.document_converter import DocumentConverter, ConversionStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Create a logger for this application
logger = logging.getLogger('invoice-json-formatter')

load_dotenv()

# ----- Configuration from Environment -----
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
REGION       = os.getenv("S3_DEFAULT_REGION", "us-east-1")
ACCESS_KEY   = os.getenv("S3_ACCESS_KEY_ID")
SECRET_KEY   = os.getenv("S3_SECRET_ACCESS_KEY")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 3))

# Default target columns if not specified in environment
DEFAULT_TARGET_COLUMNS = ["Description", "Gross worth"]
# Parse comma-separated list of target columns from environment variable
TARGET_COLUMNS = os.getenv("TARGET_COLUMNS", ",".join(DEFAULT_TARGET_COLUMNS)).split(",")
# Strip whitespace from each column name
TARGET_COLUMNS = [col.strip() for col in TARGET_COLUMNS]

SOURCE_BUCKET = "invoices"
ERROR_PREFIX  = "error/"      # Prefix for files that failed processing
JSON_INPUT   = "json/"       # Prefix for retrieving JSON output
JSON_OUTPUT   = "json-line-items/"     # Prefix for storing JSON output

logger.info(f"ENDPOINT_URL: {ENDPOINT_URL}")
logger.info(f"REGION: {REGION}")
logger.info(f"ACCESS_KEY: {ACCESS_KEY}")
logger.info(f"SECRET_KEY: {SECRET_KEY}")
logger.info(f"POLL_INTERVAL: {POLL_INTERVAL}")
logger.info(f"TARGET_COLUMNS: {TARGET_COLUMNS}")


def extract_table_columns(json_data: dict, target_column_names: list[str]):
    """
    Parses a DoclingDocument JSON data, finds tables containing specified
    column headers, and extracts the data from those columns for each row.

    Args:
        json_data (dict): The JSON data to process.
        target_column_names (list[str]): A list of column header names to extract.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents a row
                    containing the data for the target columns from all relevant tables.
                    Returns an empty list if the data is not valid or the required
                    columns are not found in any table.
    """
    all_extracted_rows = []
    
    # Process the JSON data directly
    tables = json_data.get('tables', [])
    if not tables:
        logger.warning("No tables found in the JSON data.")
        return all_extracted_rows

    logger.info(f"Found {len(tables)} table(s). Searching for columns: {', '.join(target_column_names)}...")

    for table_index, table in enumerate(tables):
        grid = table.get('data', {}).get('grid')
        if not grid or not isinstance(grid, list) or len(grid) < 1:
            logger.debug(f"Skipping table {table_index}: No valid grid data found.")
            continue

        header_row = grid[0]
        column_indices = {} # Dictionary to store {column_name: index}

        # Find the column indices for all target headers
        for col_idx, header_cell in enumerate(header_row):
            if isinstance(header_cell, dict):
                header_text = header_cell.get('text')
                if header_text in target_column_names:
                    column_indices[header_text] = col_idx
                    logger.debug(f"Found '{header_text}' header in table {table_index} at column index {col_idx}.")

        # Check if all target columns were found in this table's header
        if len(column_indices) == len(target_column_names):
            logger.info(f"Found all target columns in table {table_index}. Extracting data...")
            table_rows = []
            # Start from the second row (index 1) as the first is the header
            for row_idx, data_row in enumerate(grid[1:], start=1):
                row_data = {}
                valid_row = True
                for col_name, col_idx in column_indices.items():
                    if isinstance(data_row, list) and len(data_row) > col_idx:
                        cell = data_row[col_idx]
                        if isinstance(cell, dict):
                            row_data[col_name] = cell.get('text', '') # Get text, default to empty string
                        else:
                            logger.warning(f"Unexpected cell format in table {table_index}, row {row_idx}, col {col_idx}: {cell}")
                            row_data[col_name] = None # Or some indicator of missing data
                            valid_row = False # Or decide how to handle partial rows
                    else:
                        logger.warning(f"Row {row_idx} in table {table_index} does not have index {col_idx} or is not a list.")
                        row_data[col_name] = None
                        valid_row = False # Or decide how to handle partial rows
                        break # Stop processing this row if a column is missing

                if valid_row and row_data: # Only add if row was processed correctly and has data
                     table_rows.append(row_data)

            if table_rows:
                logger.info(f"Extracted {len(table_rows)} rows from table {table_index}.")
                all_extracted_rows.extend(table_rows) # Add rows from this table to the main list
            else:
                logger.warning(f"Found headers in table {table_index} but no valid data rows extracted.")
        else:
            missing_cols = set(target_column_names) - set(column_indices.keys())
            if missing_cols:
                 logger.debug(f"Skipping table {table_index}: Did not find all target columns. Missing: {', '.join(missing_cols)}")


    return all_extracted_rows

def extract_invoice_number(json_data: dict) -> str:
    """
    Extracts the invoice number from the DoclingDocument JSON data.
    
    Args:
        json_data (dict): The JSON data to process.
        
    Returns:
        str: The extracted invoice number, or an empty string if not found.
    """
    # Look for invoice number in the texts array
    texts = json_data.get('texts', [])
    
    for text_entry in texts:
        text = text_entry.get('text', '')
        
        # Check if this text contains an invoice number
        if 'invoice no:' in text.lower() or 'invoice number:' in text.lower():
            # Extract the invoice number using string manipulation
            parts = text.split(':', 1)
            if len(parts) > 1:
                # Return the invoice number part, stripped of whitespace
                return parts[1].strip()
    
    # If no invoice number was found, log a warning and return empty string
    logger.warning("No invoice number found in the document")
    return ""

# --- Main Processing Function ---
async def process_file(key: str, data: bytes):
    """
    Process a file from the S3 bucket.
    
    Args:
        key (str): The S3 object key
        data (bytes): The file content as bytes
        
    Returns:
        dict: A dictionary containing the invoice number and extracted line items,
              or None if processing failed
    """
    logger.info(f"Processing {key}: {len(data)} bytes")
    
    # Get the filename from the key
    filename = os.path.basename(key)
    
    # Check if this is a JSON file
    if filename.lower().endswith('.json'):
        try:
            # Parse JSON data from bytes
            json_data = json.loads(data.decode('utf-8'))
            logger.info(f"Successfully parsed JSON from {filename}")
            
            # Extract invoice number
            invoice_number = extract_invoice_number(json_data)
            if invoice_number:
                logger.info(f"Extracted invoice number: {invoice_number}")
            
            # Extract line items using configured target columns
            line_items = extract_table_columns(json_data, TARGET_COLUMNS)
            
            if not line_items:
                logger.warning(f"No line items found in {filename}")
            else:
                logger.info(f"Extracted {len(line_items)} line items from {filename}")
            
            # Return structured result with invoice number and line items
            return {
                "invoice_number": invoice_number,
                "items": line_items
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {filename}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error processing {filename}: {str(e)}")
            raise
    else:
        logger.warning(f"File {filename} is not a JSON file, skipping")
        return None

async def watch_and_transfer():
    """
    Main function that watches the S3 bucket for new files,
    processes them, and moves them to the appropriate destination.
    
    This function runs in an infinite loop, polling the bucket
    at regular intervals defined by POLL_INTERVAL.
    """ 
    logger.info(f"JSON files consumed from {SOURCE_BUCKET}/{JSON_INPUT}")
    logger.info(f"extracted JSON files to {SOURCE_BUCKET}/{JSON_OUTPUT}")
    logger.info(f"Failed files will be moved to {SOURCE_BUCKET}/{ERROR_PREFIX}")

    
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version="s3v4"),
        verify=False  # set True if you have valid SSL certs
    ) as s3:
        while True:
            try:
                # List objects in the intake prefix
                resp = await s3.list_objects_v2(
                    Bucket=SOURCE_BUCKET,
                    Prefix=JSON_INPUT
                )
                
                # Process each file found
                for obj in resp.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/") or key.endswith(".in-use"):
                        continue  # skip folder markers and files already being processed
                    
                    logger.info(f"Found file to process: {key}")
                    
                    try:
                        # Rename the file by adding ".in-use" suffix to indicate it's being processed
                        filename = os.path.basename(key)
                        in_use_key = f"{JSON_INPUT}{filename}.in-use"
                        
                        logger.info(f"Marking file as in-use: {in_use_key}")
                        await s3.copy_object(
                            Bucket=SOURCE_BUCKET,
                            CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
                            Key=in_use_key
                        )
                        
                        # Delete the original file now that we have the .in-use version
                        await s3.delete_object(Bucket=SOURCE_BUCKET, Key=key)
                        logger.info(f"Removed original file: {key}")
                        
                        # Download and process the file (now using the in-use key)
                        download = await s3.get_object(Bucket=SOURCE_BUCKET, Key=in_use_key)
                        body = await download["Body"].read()
                        
                        # Extract the original filename (without the .in-use suffix)
                        filename = os.path.basename(in_use_key).replace(".in-use", "")
                        
                        # Process the file and get the JSON result
                        # Pass the original key for better logging, but use the in-memory data
                        start_time = asyncio.get_event_loop().time()
                        json_result = await process_file(key, body)
                        processing_time = asyncio.get_event_loop().time() - start_time
                        logger.info(f"Processing completed in {processing_time:.2f} seconds")
                        
                        if filename.lower().endswith('.json') and json_result:
                            # Create a JSON file with the same base name
                            json_output_key = f"{JSON_OUTPUT}{os.path.splitext(filename)[0]}.json"
                            logger.info(f"Storing JSON result to {json_output_key}")
                            
                            # Convert the extracted line items to JSON
                            json_content = json.dumps(json_result, indent=2)
                            
                            # Upload the JSON file
                            try:
                                await s3.put_object(
                                    Bucket=SOURCE_BUCKET,
                                    Key=json_output_key,
                                    Body=json_content
                                )
                                logger.info(f"Successfully uploaded result to {SOURCE_BUCKET}/{json_output_key}")
                            except Exception as upload_error:
                                logger.error(f"Failed to upload result to {json_output_key}: {str(upload_error)}")
                                raise
                                                
                    except Exception as e:
                        # On error, move to error prefix
                        original_filename = os.path.basename(in_use_key).replace(".in-use", "")
                        error_key = f"{ERROR_PREFIX}{original_filename}"
                        logger.error(f"Error processing {in_use_key}: {str(e)}", exc_info=True)
                        
                        try:
                            # Try to copy to error prefix
                            await s3.copy_object(
                                Bucket=SOURCE_BUCKET,
                                CopySource={"Bucket": SOURCE_BUCKET, "Key": in_use_key},
                                Key=error_key
                            )
                            logger.info(f"Moved failed file to {SOURCE_BUCKET}/{error_key}")
                        except ClientError as copy_error:
                            # Handle specific S3 errors
                            error_code = copy_error.response.get('Error', {}).get('Code', 'Unknown')
                            logger.error(f"S3 error moving file to error prefix: {error_code} - {str(copy_error)}")
                        except Exception as copy_error:
                            # If the in-use file no longer exists (perhaps due to an earlier error),
                            # log the issue but don't fail
                            logger.error(f"Could not move file to error prefix: {str(copy_error)}")
                    finally:
                        # Delete the in-use file in all cases
                        try:
                            await s3.delete_object(Bucket=SOURCE_BUCKET, Key=in_use_key)
                            logger.info(f"Removed {in_use_key} from source")
                        except ClientError as del_error:
                            # Handle specific S3 errors
                            error_code = del_error.response.get('Error', {}).get('Code', 'Unknown')
                            logger.warning(f"S3 error deleting in-use file: {error_code} - {str(del_error)}")
                        except Exception as del_error:
                            logger.warning(f"Could not delete in-use file {in_use_key}: {str(del_error)}")
                
                # Wait before next poll
                await asyncio.sleep(POLL_INTERVAL)
                
            except ClientError as e:
                # Handle specific S3 errors
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                logger.error(f"S3 error in main loop: {error_code} - {str(e)}")
                # Wait before retry
                await asyncio.sleep(POLL_INTERVAL)
            except asyncio.CancelledError:
                # Handle cancellation (e.g., when the program is being shut down)
                logger.info("Main loop cancelled, shutting down...")
                break
            except Exception as e:
                # Catch any unexpected errors in the main loop
                logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
                # Wait before retry
                await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        logger.info("Starting invoice json formatter")
        logger.info(f"Watching for JSON files in {SOURCE_BUCKET}/{JSON_INPUT}")
        logger.info(f"Extracting columns: {', '.join(TARGET_COLUMNS)}")
        asyncio.run(watch_and_transfer())
    except KeyboardInterrupt:
        logger.info("Invoice json formatter stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        import sys
        sys.exit(1)