
## Table of Contents  
- [Documentation](#Documentation)    
- [Best practices](#Best-Practices)   

## Documentation 
 Quick hack to load files directly to GCS, without Airflow. Downloads csv files from https://nyc-tlc.s3.amazonaws.com/trip+data/ and uploads them to your Cloud Storage Account as parquet files.

1. Install pre-reqs (more info in `web_to_gcs.py` script)
2. Run: `python web_to_gcs.py`

## Best Practices
1.Avoid using "select star(*)" when selecting from a specific table
2.Price queries before running them - The price is visible at the top right corner
3.Benefits of clustering and partitioning is great aid in reducing costs.
4.Use streaming inserts with caution - If you use streaming inserts, be careful as they can significantly raise your costs.
5.It is advisable to break down queries into different stages and materialise them. This is especially important when using a CTE in multiple stages, as it may be beneficial to materialise the queries before using them in multiple locations

