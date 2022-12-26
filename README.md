# What is?
This utility helps you to convert json files to a [Parquet](https://parquet.apache.org) or an [ORC](https://orc.apache.org/) file to be used on a Cloudera cluster, Snowflake, Databricks or others for example.

# How to use it 
## Parquet
```
python3 json2tables.py -i ./input -o ./output/cfdi.parquet
```
## ORC
```
python3 json2tables.py -i ./input -o ./output/cfdi.orc
```