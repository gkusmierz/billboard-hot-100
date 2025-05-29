"""
PySpark Job for Processing Billboard Hot 100 Data

This job reads JSON files from the date folder, processes the nested structure,
and creates RDDs for analysis of Billboard Hot 100 chart data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
from datetime import datetime

class BillboardSparkProcessor:
    def __init__(self, app_name="BillboardHot100Processor"):
        """Initialize Spark session and configuration"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")  # Reduce verbose logging
        
    def read_json_files(self, date_folder_path="../date"):
        """
        Read all JSON files from the date folder and return as RDD
        
        Args:
            date_folder_path (str): Path to the folder containing JSON files
            
        Returns:
            RDD: Raw JSON content as strings
        """
        # Get all JSON files in the date folder
        json_files = []
        for filename in os.listdir(date_folder_path):
            if filename.endswith('.json'):
                json_files.append(os.path.join(date_folder_path, filename))
        
        print(f"Found {len(json_files)} JSON files to process")
        
        # Read all JSON files as text files
        if json_files:
            # Use wholeTextFiles to read complete files
            files_rdd = self.sc.wholeTextFiles(",".join(json_files))
            # Extract just the content (second element of tuple)
            json_content_rdd = files_rdd.map(lambda x: x[1])
            return json_content_rdd
        else:
            return self.sc.emptyRDD()
    
    def parse_json_content(self, json_rdd):
        """
        Parse JSON content and flatten the nested structure
        
        Args:
            json_rdd: RDD containing JSON strings
            
        Returns:
            RDD: Flattened song records
        """
        def parse_and_flatten(json_str):
            """Parse JSON and flatten the data structure"""
            try:
                data = json.loads(json_str.strip())
                chart_date = data.get('date', '')
                songs = data.get('data', [])
                
                # Flatten each song record with the chart date
                flattened_records = []
                for song in songs:
                    record = {
                        'chart_date': chart_date,
                        'song': song.get('song', ''),
                        'artist': song.get('artist', ''),
                        'this_week': song.get('this_week'),
                        'last_week': song.get('last_week'),
                        'peak_position': song.get('peak_position'),
                        'weeks_on_chart': song.get('weeks_on_chart')
                    }
                    flattened_records.append(record)
                
                return flattened_records
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                return []
            except Exception as e:
                print(f"Unexpected error: {e}")
                return []
        
        # Parse JSON and flatten, then flatten the list of lists
        parsed_rdd = json_rdd.flatMap(parse_and_flatten)
        return parsed_rdd
    
    def clean_and_transform_data(self, records_rdd):
        """
        Clean and transform the data for analysis
        
        Args:
            records_rdd: RDD of song records
            
        Returns:
            RDD: Cleaned and transformed records
        """
        def clean_record(record):
            """Clean individual record"""
            # Clean song and artist names
            cleaned_record = record.copy()
            
            # Strip whitespace and handle empty strings
            cleaned_record['song'] = record['song'].strip() if record['song'] else 'Unknown'
            cleaned_record['artist'] = record['artist'].strip() if record['artist'] else 'Unknown'
            
            # Handle null values for numeric fields
            for field in ['this_week', 'last_week', 'peak_position', 'weeks_on_chart']:
                if record[field] is None:
                    cleaned_record[field] = None
                else:
                    try:
                        cleaned_record[field] = int(record[field])
                    except (ValueError, TypeError):
                        cleaned_record[field] = None
            
            # Parse chart date
            try:
                cleaned_record['chart_date_parsed'] = datetime.strptime(record['chart_date'], '%Y-%m-%d')
                cleaned_record['year'] = cleaned_record['chart_date_parsed'].year
                cleaned_record['month'] = cleaned_record['chart_date_parsed'].month
            except ValueError:
                cleaned_record['chart_date_parsed'] = None
                cleaned_record['year'] = None
                cleaned_record['month'] = None
            
            return cleaned_record
        
        # Apply cleaning transformations
        cleaned_rdd = records_rdd.map(clean_record)
        
        # Filter out records with invalid data
        valid_rdd = cleaned_rdd.filter(lambda x: x['this_week'] is not None and x['chart_date_parsed'] is not None)
        
        return valid_rdd
    
    def create_dataframe_from_rdd(self, cleaned_rdd):
        """
        Convert RDD to DataFrame for SQL operations
        
        Args:
            cleaned_rdd: Cleaned RDD
            
        Returns:
            DataFrame: Spark DataFrame
        """
        # Define schema
        schema = StructType([
            StructField("chart_date", StringType(), True),
            StructField("song", StringType(), True),
            StructField("artist", StringType(), True),
            StructField("this_week", IntegerType(), True),
            StructField("last_week", IntegerType(), True),
            StructField("peak_position", IntegerType(), True),
            StructField("weeks_on_chart", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True)
        ])
        
        # Convert RDD to DataFrame
        # First convert to Row objects
        from pyspark.sql import Row
        
        def to_row(record):
            return Row(
                chart_date=record['chart_date'],
                song=record['song'],
                artist=record['artist'],
                this_week=record['this_week'],
                last_week=record['last_week'],
                peak_position=record['peak_position'],
                weeks_on_chart=record['weeks_on_chart'],
                year=record['year'],
                month=record['month']
            )
        
        row_rdd = cleaned_rdd.map(to_row)
        df = self.spark.createDataFrame(row_rdd, schema)
        
        return df
    
    def analyze_data(self, df):
        """
        Perform basic analysis on the data
        
        Args:
            df: Spark DataFrame
        """
        print("=== BILLBOARD HOT 100 DATA ANALYSIS ===\n")
        
        # Basic statistics
        total_records = df.count()
        print(f"Total records processed: {total_records:,}")
        
        # Date range
        date_range = df.select(min("chart_date").alias("earliest"), max("chart_date").alias("latest")).collect()[0]
        print(f"Date range: {date_range['earliest']} to {date_range['latest']}")
        
        # Unique songs and artists
        unique_songs = df.select("song", "artist").distinct().count()
        unique_artists = df.select("artist").distinct().count()
        print(f"Unique songs: {unique_songs:,}")
        print(f"Unique artists: {unique_artists:,}")
        
        print("\n=== TOP 10 ARTISTS BY CHART APPEARANCES ===")
        artist_appearances = df.groupBy("artist") \
            .agg(count("*").alias("appearances")) \
            .orderBy(desc("appearances")) \
            .limit(10)
        artist_appearances.show(truncate=False)
        
        print("\n=== TOP 10 SONGS BY WEEKS ON CHART ===")
        top_songs_by_weeks = df.groupBy("song", "artist") \
            .agg(max("weeks_on_chart").alias("max_weeks_on_chart")) \
            .orderBy(desc("max_weeks_on_chart")) \
            .limit(10)
        top_songs_by_weeks.show(truncate=False)
        
        print("\n=== NUMBER ONE HITS BY DECADE ===")
        number_ones = df.filter(col("this_week") == 1) \
            .withColumn("decade", (floor(col("year") / 10) * 10).cast("int")) \
            .groupBy("decade") \
            .agg(count("*").alias("number_one_hits")) \
            .orderBy("decade")
        number_ones.show()
        
        return df
    
    def save_processed_data(self, df, output_path="../processed_billboard_data"):
        """
        Save processed data to various formats
        
        Args:
            df: Spark DataFrame
            output_path: Base path for output files
        """
        print(f"\n=== SAVING PROCESSED DATA ===")
        
        # Save as Parquet (efficient for analytics)
        parquet_path = f"{output_path}/parquet"
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Saved as Parquet: {parquet_path}")
        
        # Save as CSV (human readable)
        csv_path = f"{output_path}/csv"
        df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(csv_path)
        print(f"Saved as CSV: {csv_path}")
        
        # Save summary statistics
        summary_path = f"{output_path}/summary"
        summary_df = df.describe()
        summary_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(summary_path)
        print(f"Saved summary statistics: {summary_path}")
    
    def run_complete_pipeline(self, date_folder_path="../date", output_path="../processed_billboard_data"):
        """
        Run the complete data processing pipeline
        
        Args:
            date_folder_path: Path to JSON files
            output_path: Path for output files
        """
        print("Starting Billboard Hot 100 data processing pipeline...")
        
        try:
            # Step 1: Read JSON files
            print("\n1. Reading JSON files...")
            json_rdd = self.read_json_files(date_folder_path)
            
            if json_rdd.isEmpty():
                print("No JSON files found. Exiting.")
                return
            
            # Step 2: Parse and flatten JSON
            print("2. Parsing and flattening JSON data...")
            records_rdd = self.parse_json_content(json_rdd)
            print(f"Parsed {records_rdd.count():,} song records")
            
            # Step 3: Clean and transform data
            print("3. Cleaning and transforming data...")
            cleaned_rdd = self.clean_and_transform_data(records_rdd)
            print(f"Cleaned records: {cleaned_rdd.count():,}")
            
            # Step 4: Create DataFrame
            print("4. Creating DataFrame...")
            df = self.create_dataframe_from_rdd(cleaned_rdd)
            
            # Step 5: Analyze data
            print("5. Analyzing data...")
            df = self.analyze_data(df)
            
            # Step 6: Save processed data
            print("6. Saving processed data...")
            self.save_processed_data(df, output_path)
            
            print("\n✅ Pipeline completed successfully!")
            
            return df
            
        except Exception as e:
            print(f"❌ Error in pipeline: {e}")
            raise

def main():
    """Main function to run the Billboard data processing job"""
    processor = BillboardSparkProcessor()
    
    # Run the complete pipeline
    df = processor.run_complete_pipeline()
    
    # Optional: Return DataFrame for further analysis
    return df

if __name__ == "__main__":
    main()