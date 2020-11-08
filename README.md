# spark-google-spreadsheet-datasource

A spark datasource for read and write Google Spreadsheet.

# How to use
## read data

```scala
    val data = spark.read.format("google-spreadsheet")
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName1)
      .load()
```

## write data

```scala
    df.write.format("google-spreadsheet")
      .option("credentialsPath", credentialFile)
      .option("spreadsheetId", spreadsheetId)
      .option("sheetName", sheetName)
      .mode(SaveMode.Overwrite)
      .save()
```