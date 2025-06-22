from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

Initialize Spark Session
spark = SparkSession.builder.appName("NaturalJoinUsingMapReduce").getOrCreate()

#conf = SparkConf().set("spark.python.worker.memory", "512m")
#sc = SparkContext(conf=conf)

# Load the CSV files into RDDs
dataset1_path = "C:\\path\\path\\path\\dept_data.csv"
dataset2_path = "C:\\path\\path\\path\\employee_data.csv"


# Read the CSV files as RDDs by skipping the header
rdd1 = spark.sparkContext.textFile(dataset1_path).map(lambda line: line.split(","))
header1 = rdd1.first()  # Extract header
rdd1 = rdd1.filter(lambda row: row != header1)  # Remove header

rdd2 = spark.sparkContext.textFile(dataset2_path).map(lambda line: line.split(","))
header2 = rdd2.first()  # Extract header
rdd2 = rdd2.filter(lambda row: row != header2)  # Remove header

# Create key-value pairs for the Map step
# For dataset1: Key is Employee_Id, value is the rest (Department, Position)
rdd1_mapped = rdd1.map(lambda row: (row[1], (row[0], row[2])))  # (Employee_Id, (Dep_name, Position))

# For dataset2: Key is Employee_Id, value is the rest (Name, City, Salary)
rdd2_mapped = rdd2.map(lambda row: (row[0], (row[1], row[2], row[3])))  # (Employee_Id, (Name, City, Salary))

# Perform the Reduce step (Join based on Employee_Id)
# We use a full outer join here to simulate a "natural join"
joined_rdd = rdd1_mapped.join(rdd2_mapped)

# Process the joined data
# The result of the join will be in the format (Employee_Id, ((Dep_name, Position), (Name, City, Salary)))
joined_result = joined_rdd.map(lambda row: (row[0], row[1][0][0], row[1][0][1], row[1][1][0], row[1][1][1], row[1][1][2]))

# Collect and print the final result
for row in joined_result.collect():
    print(row)

# Stop the Spark session
spark.stop()
