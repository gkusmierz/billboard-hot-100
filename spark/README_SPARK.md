# Billboard Hot 100 PySpark Data Processing

This project provides a comprehensive PySpark job for processing Billboard Hot 100 chart data stored in JSON files. The data is transformed from nested JSON format into structured RDDs and DataFrames for analysis.

## 📁 Data Structure

The input data consists of JSON files in the `date/` folder, each named by date (e.g., `1958-08-04.json`). Each file contains:

```json
{
  "date": "1958-08-04",
  "data": [
    {
      "song": "Poor Little Fool",
      "artist": "Ricky Nelson",
      "this_week": 1,
      "last_week": null,
      "peak_position": 1,
      "weeks_on_chart": 1
    },
    // ... more songs
  ]
}
```

## 🚀 Features

### Data Processing Pipeline
- **JSON Parsing**: Reads and parses all JSON files from the date folder
- **Data Flattening**: Converts nested JSON structure to flat records
- **Data Cleaning**: Handles null values, data type conversions, and validation
- **Date Processing**: Extracts year and month from chart dates
- **Multiple Output Formats**: Saves processed data as Parquet, CSV, and summary statistics

### Analysis Capabilities
- Chart statistics and trends
- Artist and song popularity metrics
- Historical analysis by decade
- Chart position movements and jumps
- Longest-running songs analysis

### RDD Operations
- Advanced filtering and transformations
- Cross-decade artist analysis
- Chart position calculations
- Custom aggregations and joins

## 📋 Requirements

```bash
pip install -r requirements.txt
```

Required packages:
- `pyspark>=3.4.0`
- `py4j>=0.10.9`

## 🔧 Installation & Setup

1. **Clone or download the project files**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Ensure your data is in the correct structure**:
   ```
   project/
   ├── date/
   │   ├── 1958-08-04.json
   │   ├── 1958-09-29.json
   │   └── ... (more JSON files)
   └── spark/
       ├── billboard_spark_job.py
       ├── run_example.py
       ├── test_spark_job.py
       ├── requirements.txt
       ├── README_SPARK.md
       └── SPARK_JOB_SUMMARY.md
   ```

## 💻 Usage

### Basic Usage

```python
from billboard_spark_job import BillboardSparkProcessor

# Initialize processor
processor = BillboardSparkProcessor()

# Run complete pipeline
df = processor.run_complete_pipeline(
    date_folder_path="../date",
    output_path="../processed_billboard_data"
)
```

### Running the Example Script

```bash
python run_example.py
```

The example script provides three options:
1. **Basic Analysis**: DataFrame operations with built-in analytics
2. **Advanced RDD Operations**: Custom RDD transformations and analysis
3. **Both**: Runs both analysis types

### Custom Analysis

```python
from billboard_spark_job import BillboardSparkProcessor

processor = BillboardSparkProcessor()

# Read and process data
json_rdd = processor.read_json_files("../date")
records_rdd = processor.parse_json_content(json_rdd)
cleaned_rdd = processor.clean_and_transform_data(records_rdd)

# Convert to DataFrame for SQL operations
df = processor.create_dataframe_from_rdd(cleaned_rdd)

# Custom analysis
top_artists = df.groupBy("artist") \
    .agg(count("*").alias("chart_appearances")) \
    .orderBy(desc("chart_appearances")) \
    .limit(10)

top_artists.show()
```

## 📊 Output Files

The pipeline generates several output files:

### 1. Processed Data
- **Parquet format**: `./processed_billboard_data/parquet/`
  - Efficient columnar format for analytics
  - Preserves schema and data types
  
- **CSV format**: `./processed_billboard_data/csv/`
  - Human-readable format
  - Includes headers

### 2. Summary Statistics
- **Summary stats**: `./processed_billboard_data/summary/`
  - Statistical summaries of numeric columns
  - Count, mean, stddev, min, max values

## 🔍 Data Schema

After processing, the data has the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `chart_date` | String | Original chart date (YYYY-MM-DD) |
| `song` | String | Song title |
| `artist` | String | Artist name |
| `this_week` | Integer | Current week position (1-100) |
| `last_week` | Integer | Previous week position (null if new entry) |
| `peak_position` | Integer | Highest position achieved |
| `weeks_on_chart` | Integer | Total weeks on chart |
| `year` | Integer | Chart year |
| `month` | Integer | Chart month |

## 📈 Analysis Examples

### 1. Top Artists by Chart Appearances
```python
df.groupBy("artist") \
  .agg(count("*").alias("appearances")) \
  .orderBy(desc("appearances")) \
  .show(10)
```

### 2. Number One Hits by Decade
```python
df.filter(col("this_week") == 1) \
  .withColumn("decade", (floor(col("year") / 10) * 10)) \
  .groupBy("decade") \
  .agg(count("*").alias("number_one_hits")) \
  .orderBy("decade") \
  .show()
```

### 3. Longest Running Songs
```python
df.groupBy("song", "artist") \
  .agg(max("weeks_on_chart").alias("max_weeks")) \
  .orderBy(desc("max_weeks")) \
  .show(10)
```

### 4. RDD Operations - Cross-Decade Artists
```python
# Find artists active in both 1960s and 2000s
sixties_artists = cleaned_rdd.filter(lambda x: 1960 <= x['year'] <= 1969) \
    .map(lambda x: x['artist']).distinct()

two_thousands_artists = cleaned_rdd.filter(lambda x: 2000 <= x['year'] <= 2009) \
    .map(lambda x: x['artist']).distinct()

cross_decade_artists = sixties_artists.intersection(two_thousands_artists)
```

## ⚙️ Configuration Options

### Spark Configuration
The processor includes optimized Spark settings:
- Adaptive Query Execution enabled
- Partition coalescing for better performance
- Reduced logging verbosity

### Custom Configuration
```python
processor = BillboardSparkProcessor(app_name="CustomBillboardAnalysis")

# Access Spark context for custom configurations
processor.sc.setCheckpointDir("./checkpoints")
processor.spark.conf.set("spark.sql.shuffle.partitions", "200")
```

## 🐛 Troubleshooting

### Common Issues

1. **Memory Issues**
   - Increase driver memory: `--driver-memory 4g`
   - Increase executor memory: `--executor-memory 4g`

2. **File Not Found Errors**
   - Ensure the `date/` folder exists and contains JSON files
   - Check file permissions

3. **JSON Parsing Errors**
   - Verify JSON file format matches expected structure
   - Check for corrupted files

### Performance Optimization

1. **For Large Datasets**:
   ```python
   # Increase parallelism
   processor.spark.conf.set("spark.sql.shuffle.partitions", "400")
   
   # Enable dynamic allocation
   processor.spark.conf.set("spark.dynamicAllocation.enabled", "true")
   ```

2. **For Memory Constraints**:
   ```python
   # Process data in smaller batches
   # Use repartition() to control partition size
   df.repartition(10).write.parquet("output")
   ```

## 📝 Data Modifications Applied

The PySpark job applies several modifications to prepare the raw JSON data for RDD processing:

1. **Structure Flattening**: Converts nested JSON to flat records
2. **Data Type Conversion**: Ensures proper integer types for numeric fields
3. **Null Handling**: Manages null values in `last_week` and other fields
4. **Date Parsing**: Extracts year and month components
5. **Data Validation**: Filters out records with invalid chart positions
6. **String Cleaning**: Trims whitespace from song and artist names

## 🤝 Contributing

To extend the analysis:

1. Add new transformation functions to `BillboardSparkProcessor`
2. Create custom RDD operations for specific analysis needs
3. Extend the schema for additional derived fields
4. Add new output formats or visualization capabilities

## 📄 License

This project is provided as-is for educational and analysis purposes.