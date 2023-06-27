

# Indexes on the imported columns

As of 2.5.0, the imported CSV may be covered by an index by the underlying HSQLDB.  
This speeds up joins across large data sets significantly.  
Example: 10.000 x 10.000 rows join took around 30 minutes. With the indexes it is within seconds.  

Usage example:

`invoices.csv`
```csv
# id, whenSend, totalAmount, ...
1001, ...
```

`invoiceLines.csv`
```csv
# id, invoice_id, description, unit, qty, unit_price, amount, ...
20002, 1001, ...
```

```bash
./crunch \
   -in invoices.csv -indexed id \
   -in invoiceLines.csv -indexed id,invoice_id \
   -sql "SELECT invoices AS i LEFT JOIN invoiceLines AS il ON (il.invoice_id = i.id)"
   -out joined.csv
```

With the added indexes, such query will now execute much faster.

For now, the indexes need to be added using `-indexed`.  
Later on, this could happen automatically for the columns appearing in `JOIN`, `WHERE`, and `GROUP BY` clauses.
