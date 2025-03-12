from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# 1. Khởi tạo Spark Session
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# 2. Đọc dữ liệu từ file CSV
file_path = "C:/bigdata/weatherHistory.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 3. Hiển thị thông tin dữ liệu
df.printSchema()
df.show(5)

# 4. Kiểm tra số lượng bản ghi
total_rows = df.count()
print(f"Total rows: {total_rows}")

# 5. Thống kê mô tả dữ liệu và lưu vào CSV
numerical_cols = ["Temperature (C)", "Apparent Temperature (C)", "Humidity", 
                  "Wind Speed (km/h)", "Wind Bearing (degrees)", "Visibility (km)", 
                  "Loud Cover", "Pressure (millibars)"]

summary_df = df.select(numerical_cols).describe()
summary_output_path = "C:/bigdata/output/summary_statistics"
summary_df.write.csv(summary_output_path, header=True, mode="overwrite")

# 6. Kiểm tra giá trị thiếu và lưu vào CSV
missing_values = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
missing_values_output_path = "C:/bigdata/output/missing_values"
missing_values.write.csv(missing_values_output_path, header=True, mode="overwrite")

# 7. Tính nhiệt độ trung bình theo từng loại thời tiết và lưu vào CSV
avg_temp_output_path = "C:/bigdata/output/avg_temperature_by_summary"
df.groupBy("Summary").avg("Temperature (C)").write.csv(avg_temp_output_path, header=True, mode="overwrite")

# 8. Lưu toàn bộ dữ liệu gốc vào CSV
output_path = "C:/bigdata/output/weather_data"
df.write.csv(output_path, header=True, mode="overwrite")

# 9. Dừng Spark Session
input("Press Enter to exit...")  
spark.stop()

