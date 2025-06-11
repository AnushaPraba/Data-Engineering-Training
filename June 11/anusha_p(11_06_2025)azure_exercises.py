
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

data = [
    ("Ananya", "HR", 52000),
    ("Rahul", "Engineering", 65000),
    ("Priya", "Engineering", 60000),
    ("Zoya", "Marketing", 48000),
    ("Karan", "HR", 53000),
    ("Naveen", "Engineering", 70000),
    ("Fatima", "Marketing", 45000)
]
columns = ["Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)

"""Exercise Set 1: Basics"""

# 1. Display all records in the DataFrame.
df.show()

# 2. Print the schema of the DataFrame.
df.printSchema()

# 3. Count total number of employees.
df.count()

"""Exercise Set 2: Column Operations"""

# 4. Add a new column Bonus which is 15% of Salary.
from pyspark.sql.functions import col
df_bonus = df.withColumn("Bonus", col("Salary") * 0.15)
df_bonus.show()

# 5. Add a new column NetPay = Salary + Bonus.
df_netpay = df_bonus.withColumn("NetPay", col("Salary") + col("Bonus"))
df_netpay.show()

"""Exercise Set 3: Filtering and Conditions"""

# 6. Display only employees from the “Engineering” department.
df.filter(col("Department") == "Engineering").show()

# 7. Display employees whose salary is greater than 60000.
df.filter(col("Salary") > 60000).show()

# 8. Display employees who are not in the “Marketing” department.
df.filter(col("Department") != "Marketing").show()

"""Exercise Set 4: Sorting and Limiting"""

# 9. Show top 3 highest paid employees.
df.orderBy(col("Salary").desc()).show(3)

# 10. Sort the data by Department ascending and Salary descending.
df.orderBy(col("Department").asc(), col("Salary").desc()).show()

"""Exercise Set 5: String and Case Logic"""

# 11. Add a new column Level :
# “Senior” if salary > 60000
# “Mid” if salary between 50000 and 60000
# “Junior” otherwise
from pyspark.sql.functions import when
df_level = df.withColumn("Level", when(col("Salary") > 60000, "Senior")
                        .when((col("Salary") >= 50000) & (col("Salary") <= 60000), "Mid")
                        .otherwise("Junior"))
df_level.show()

# 12. Convert all names to uppercase.
from pyspark.sql.functions import upper
df_upper = df_level.withColumn("Name_Upper", upper(col("Name")))
df_upper.show()