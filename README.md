# burning-bigquery
## Example
### Execute a query simply
```java
BurningBigQuery bbq = new BurningBigQuery("project-id"
        , "servic-account"
        , new File("path/file.p12"));
Iterable result = bbq.query("select * from dataset.table where condition=11").asIterable();
for (TableRow row : result) {
    System.out.println("row=" + row.getF());
}
for (TableRow row : tableRows) {
    for (TableCell cell : row.getF()) {
        System.out.print(cell.getV().toString() + ",");
    }
    System.out.println();
}
```

### Execute a query and get too learge results
```java
Iterable result = bbq.query("select * from dataset.table where condition=11")
    .asIterableViaGcs("temp_dataset", "temp_gcs_buckt");
```
