"""
Example script demonstrating how to use the Billboard PySpark job
"""

from billboard_spark_job import BillboardSparkProcessor
from pyspark.sql.functions import col, count, countDistinct, max, size, floor, desc

def run_basic_analysis():
    """Run basic analysis on Billboard data"""
    
    # Initialize the processor
    processor = BillboardSparkProcessor(app_name="BillboardAnalysisExample")
    
    try:
        print("🎵 Starting Billboard Hot 100 Analysis...")
        
        # Process data but don't stop Spark context yet
        json_rdd = processor.read_json_files("../date")
        records_rdd = processor.parse_json_content(json_rdd)
        cleaned_rdd = processor.clean_and_transform_data(records_rdd)
        df = processor.create_dataframe_from_rdd(cleaned_rdd)
        
        # Run analysis without stopping Spark
        df = processor.analyze_data(df)
        processor.save_processed_data(df, "../output")
        
        # Additional custom analysis examples
        print("\n=== CUSTOM ANALYSIS EXAMPLES ===")
        
        # Example 1: Find songs that reached #1 in multiple decades
        print("\n📊 Songs that reached #1 in multiple decades:")
        multi_decade_hits = df.filter(df.this_week == 1) \
            .withColumn("decade", (floor(df.year / 10) * 10).cast("int")) \
            .groupBy("song", "artist") \
            .agg({"decade": "collect_set"}) \
            .filter(size(col("collect_set(decade)")) > 1) \
            .select("song", "artist", col("collect_set(decade)").alias("decades"))
        
        if multi_decade_hits.count() > 0:
            multi_decade_hits.show(truncate=False)
        else:
            print("No songs found that reached #1 in multiple decades")
        
        # Example 2: Artists with the most #1 hits
        print("\n🏆 Artists with most #1 hits:")
        number_one_artists = df.filter(df.this_week == 1) \
            .groupBy("artist") \
            .agg(count("*").alias("number_one_hits")) \
            .orderBy(desc("number_one_hits")) \
            .limit(15)
        number_one_artists.show(truncate=False)
        
        # Example 3: Longest running songs on the chart
        print("\n⏰ Longest running songs on the chart:")
        longest_running = df.groupBy("song", "artist") \
            .agg(max("weeks_on_chart").alias("max_weeks")) \
            .orderBy(desc("max_weeks")) \
            .limit(10)
        longest_running.show(truncate=False)
        
        # Example 4: Chart activity by year
        print("\n📈 Chart activity by year (number of unique songs):")
        yearly_activity = df.groupBy("year") \
            .agg(countDistinct("song", "artist").alias("unique_songs")) \
            .orderBy("year") \
            .filter(df.year.isNotNull())
        yearly_activity.show(50)
        
        return df
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        raise
    finally:
        processor.spark.stop()

def run_advanced_rdd_operations():
    """Demonstrate advanced RDD operations on the Billboard data"""
    
    processor = BillboardSparkProcessor(app_name="BillboardRDDOperations")
    
    try:
        print("\n🔧 Advanced RDD Operations Example...")
        
        # Read and parse data
        json_rdd = processor.read_json_files("../date")
        records_rdd = processor.parse_json_content(json_rdd)
        cleaned_rdd = processor.clean_and_transform_data(records_rdd)
        
        # RDD Operation 1: Find artists who had songs in both the 1960s and 2000s
        print("\n🎭 Artists active in both 1960s and 2000s:")
        
        # Filter for 1960s
        sixties_artists = cleaned_rdd.filter(lambda x: x['year'] and 1960 <= x['year'] <= 1969) \
            .map(lambda x: x['artist']) \
            .distinct()
        
        # Filter for 2000s
        two_thousands_artists = cleaned_rdd.filter(lambda x: x['year'] and 2000 <= x['year'] <= 2009) \
            .map(lambda x: x['artist']) \
            .distinct()
        
        # Find intersection
        cross_decade_artists = sixties_artists.intersection(two_thousands_artists)
        cross_decade_list = cross_decade_artists.collect()
        
        if cross_decade_list:
            for artist in sorted(cross_decade_list):
                print(f"  • {artist}")
        else:
            print("  No artists found active in both decades")
        
        # RDD Operation 2: Calculate average chart position by decade
        print("\n📊 Average chart position by decade:")
        
        decade_positions = cleaned_rdd.filter(lambda x: x['year'] and x['this_week']) \
            .map(lambda x: ((x['year'] // 10) * 10, (x['this_week'], 1))) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .map(lambda x: (x[0], x[1][0] / x[1][1])) \
            .sortByKey()
        
        for decade, avg_position in decade_positions.collect():
            print(f"  {decade}s: {avg_position:.2f}")
        
        # RDD Operation 3: Find songs with the biggest jumps in chart position
        print("\n🚀 Biggest positive chart jumps (last_week to this_week):")
        
        biggest_jumps = cleaned_rdd.filter(lambda x: x['last_week'] and x['this_week']) \
            .map(lambda x: (x['last_week'] - x['this_week'], x['song'], x['artist'], x['chart_date'])) \
            .filter(lambda x: x[0] > 0) \
            .sortBy(lambda x: x[0], ascending=False) \
            .take(10)
        
        for jump, song, artist, date in biggest_jumps:
            print(f"  +{jump} positions: '{song}' by {artist} ({date})")
        
        print("\n✅ RDD operations completed!")
        
    except Exception as e:
        print(f"❌ Error during RDD operations: {e}")
        raise
    finally:
        processor.spark.stop()

if __name__ == "__main__":
    print("Choose an example to run:")
    print("1. Basic Analysis (DataFrame operations)")
    print("2. Advanced RDD Operations")
    print("3. Both")
    
    choice = input("Enter your choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        run_basic_analysis()
    elif choice == "2":
        run_advanced_rdd_operations()
    elif choice == "3":
        run_basic_analysis()
        print("\n" + "="*50)
        run_advanced_rdd_operations()
    else:
        print("Invalid choice. Running basic analysis...")
        run_basic_analysis()