import asyncio
import json
import os
from invoice_json_formatter import process_file, extract_invoice_number, extract_table_columns

async def test_process_file():
    # Load the test file
    with open('invoice_3.json', 'rb') as f:
        data = f.read()
    
    # Process the file
    result = await process_file('invoice_2.json', data)
    
    # Print the result
    print(f"Invoice Number: {result['invoice_number']}")
    print(f"Number of line items: {len(result['items'])}")
    
    # Save the result to a file
    with open('test_output_3.json', 'w') as f:
        json.dump(result, f, indent=2)
    
    print(f"Result saved to test_output.json")

if __name__ == "__main__":
    asyncio.run(test_process_file())