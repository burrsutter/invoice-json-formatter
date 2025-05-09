while true; do
  clear
  echo "File count in invoices/json-header:"
  mc find myminio/invoices/json-header | grep -v '/$' | wc -l
  sleep 3
done