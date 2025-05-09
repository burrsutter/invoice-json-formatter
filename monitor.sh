while true; do
  clear
  echo "File count in invoices/json-line-items:"
  mc find myminio/invoices/json-line-items | grep -v '/$' | wc -l
  sleep 3
done