## ai-invoice-json-formatter

Follows ai-invoice-extractor. Takes the huge JSON produced by docling and whittles it down to just the line-items on the invoice. 


JSON_INPUT   = "json/"       # Prefix for retrieving JSON output
JSON_OUTPUT   = "json-line-items/"     # Prefix for storing JSON output


Loads files from `JSON_INPUT` and produces new files for `JSON_OUTPUT`

