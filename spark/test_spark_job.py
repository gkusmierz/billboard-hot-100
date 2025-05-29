"""
Test script for the Billboard PySpark job
"""

import os
import sys
from billboard_spark_job import BillboardSparkProcessor

def test_data_availability():
    """Test if the required data files are available"""
    print("🔍 Testing data availability...")
    
    date_folder = "../date"
    if not os.path.exists(date_folder):
        print(f"❌ Date folder not found: {date_folder}")
        return False
    
    json_files = [f for f in os.listdir(date_folder) if f.endswith('.json')]
    if not json_files:
        print(f"❌ No JSON files found in {date_folder}")
        return False
    
    print(f"✅ Found {len(json_files)} JSON files")
    print(f"   Sample files: {json_files[:5]}")
    return True

def test_json_parsing():
    """Test JSON parsing functionality"""
    print("\n🔍 Testing JSON parsing...")
    
    try:
        processor = BillboardSparkProcessor(app_name="TestJSONParsing")
        
        # Read a small sample
        json_rdd = processor.read_json_files("../date")
        if json_rdd.isEmpty():
            print("❌ No JSON content read")
            return False
        
        # Test parsing
        records_rdd = processor.parse_json_content(json_rdd)
        sample_records = records_rdd.take(5)
        
        if not sample_records:
            print("❌ No records parsed from JSON")
            return False
        
        print(f"✅ Successfully parsed records")
        print(f"   Sample record keys: {list(sample_records[0].keys())}")
        print(f"   Total records parsed: {records_rdd.count()}")
        
        processor.spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ JSON parsing failed: {e}")
        return False

def test_data_cleaning():
    """Test data cleaning functionality"""
    print("\n🔍 Testing data cleaning...")
    
    try:
        processor = BillboardSparkProcessor(app_name="TestDataCleaning")
        
        # Process data through cleaning
        json_rdd = processor.read_json_files("../date")
        records_rdd = processor.parse_json_content(json_rdd)
        cleaned_rdd = processor.clean_and_transform_data(records_rdd)
        
        # Check cleaned data
        sample_cleaned = cleaned_rdd.take(3)
        total_cleaned = cleaned_rdd.count()
        
        if not sample_cleaned:
            print("❌ No cleaned records produced")
            return False
        
        print(f"✅ Data cleaning successful")
        print(f"   Cleaned records: {total_cleaned}")
        print(f"   Sample cleaned record:")
        for key, value in sample_cleaned[0].items():
            print(f"     {key}: {value} ({type(value).__name__})")
        
        processor.spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Data cleaning failed: {e}")
        return False

def test_dataframe_creation():
    """Test DataFrame creation and basic operations"""
    print("\n🔍 Testing DataFrame creation...")
    
    try:
        processor = BillboardSparkProcessor(app_name="TestDataFrame")
        
        # Process through to DataFrame
        json_rdd = processor.read_json_files("../date")
        records_rdd = processor.parse_json_content(json_rdd)
        cleaned_rdd = processor.clean_and_transform_data(records_rdd)
        df = processor.create_dataframe_from_rdd(cleaned_rdd)
        
        # Test DataFrame operations
        row_count = df.count()
        columns = df.columns
        
        print(f"✅ DataFrame created successfully")
        print(f"   Rows: {row_count:,}")
        print(f"   Columns: {len(columns)} - {columns}")
        
        # Test a simple query
        sample_data = df.limit(3).collect()
        print(f"   Sample data preview:")
        for i, row in enumerate(sample_data):
            print(f"     Row {i+1}: {row.song} by {row.artist} (#{row.this_week})")
        
        processor.spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ DataFrame creation failed: {e}")
        return False

def test_basic_analysis():
    """Test basic analysis functions"""
    print("\n🔍 Testing basic analysis...")
    
    try:
        processor = BillboardSparkProcessor(app_name="TestAnalysis")
        
        # Run through to analysis
        json_rdd = processor.read_json_files("../date")
        records_rdd = processor.parse_json_content(json_rdd)
        cleaned_rdd = processor.clean_and_transform_data(records_rdd)
        df = processor.create_dataframe_from_rdd(cleaned_rdd)
        
        # Test some basic analysis operations
        total_records = df.count()
        unique_artists = df.select("artist").distinct().count()
        from pyspark.sql.functions import min, max
        date_range = df.select("chart_date").agg(min("chart_date").alias("min_date"), max("chart_date").alias("max_date")).collect()[0]
        
        print(f"✅ Basic analysis successful")
        print(f"   Total records: {total_records:,}")
        print(f"   Unique artists: {unique_artists:,}")
        print(f"   Date range: {date_range.min_date} to {date_range.max_date}")
        
        # Test number one hits
        number_ones = df.filter(df.this_week == 1).count()
        print(f"   Number one hits: {number_ones:,}")
        
        processor.spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Basic analysis failed: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    print("🧪 Running Billboard PySpark Job Tests\n")
    
    tests = [
        ("Data Availability", test_data_availability),
        ("JSON Parsing", test_json_parsing),
        ("Data Cleaning", test_data_cleaning),
        ("DataFrame Creation", test_dataframe_creation),
        ("Basic Analysis", test_basic_analysis)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*50)
    print("📊 TEST RESULTS SUMMARY")
    print("="*50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All tests passed! The PySpark job is ready to use.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)