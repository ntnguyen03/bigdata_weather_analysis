from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# 1. Khởi tạo Spark Session
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# 2. Đọc dữ liệu từ file CSV
file_path = "C:/bigdata/weatherHistory.csv" 

spark.sparkContext.setJobGroup("Data Preprocessing", "Reading and Inspecting Data")
spark.sparkContext.setJobDescription("Loading CSV into DataFrame")
df = spark.read.csv(file_path, header=True, inferSchema=True)

spark.sparkContext.setJobDescription("Displaying Schema and Sample Data")
df.printSchema()
df.show(5)

# 3. Kiểm tra số lượng bản ghi
spark.sparkContext.setJobDescription("Counting Total Rows")
print(f"Total rows: {df.count()}")

# 4. Thống kê mô tả dữ liệu
numerical_cols = ["Temperature (C)", "Apparent Temperature (C)", "Humidity", 
                  "Wind Speed (km/h)", "Wind Bearing (degrees)", "Visibility (km)", 
                  "Loud Cover", "Pressure (millibars)"]

spark.sparkContext.setJobDescription("Generating Summary Statistics")
df.select(numerical_cols).describe().show()

# 5. Kiểm tra giá trị thiếu
spark.sparkContext.setJobDescription("Checking Missing Values")
missing_values = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
missing_values.show()

# 6. Tính nhiệt độ trung bình theo từng loại thời tiết
spark.sparkContext.setJobDescription("Calculating Average Temperature by Weather Condition")
df.groupBy("Summary").avg("Temperature (C)").show()

# 7. Dừng Spark Session
input("Press Enter to exit...")  
spark.stop()

