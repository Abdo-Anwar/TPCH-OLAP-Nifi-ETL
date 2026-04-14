import time
from pyspark.sql import SparkSession

# بناء الجلسة
spark = SparkSession.builder \
    .appName("Lab1_OLAP_Spark_Benchmark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Loading Parquet files...")
# لاحظ إننا بنقرأ من مسار الـ Volume الخاص بـ Spark جوة الكونتينر
df_fact = spark.read.parquet("/data/parquet/fact_lineitem")
df_cust = spark.read.parquet("/data/parquet/dim_customer")
df_supp = spark.read.parquet("/data/parquet/dim_supplier")

df_fact.createOrReplaceTempView("fact_lineitem")
df_cust.createOrReplaceTempView("dim_customer")
df_supp.createOrReplaceTempView("dim_supplier")

# الاستعلام على الـ Star Schema
star_schema_query = """
SELECT
    c.n_name,
    s.s_name,
    sum(f.l_quantity) as sum_qty,
    sum(f.l_extendedprice) as sum_base_price,
    sum(f.l_extendedprice * (1 - f.l_discount)) as sum_disc_price,
    sum(f.l_extendedprice * (1 - f.l_discount) * (1 + f.l_tax)) as sum_charge,
    avg(f.l_quantity) as avg_qty,
    avg(f.l_extendedprice) as avg_price,
    avg(f.l_discount) as avg_disc,
    count(*) as count_order
FROM fact_lineitem f
JOIN dim_customer c ON f.c_custkey = c.c_custkey
JOIN dim_supplier s ON f.s_suppkey = s.s_suppkey
WHERE f.l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY c.n_name, s.s_name
"""

execution_times = []
print("Starting Spark Benchmark (8 Runs)...")

for i in range(1, 9):
    start_time = time.time()
    result = spark.sql(star_schema_query).collect()
    time_taken = time.time() - start_time
    execution_times.append(time_taken)
    print(f"Run {i}: {time_taken:.4f} seconds")

avg_time = sum(execution_times) / len(execution_times)
print("-" * 30)
print(f"Average Spark Execution Time: {avg_time:.4f} seconds")

spark.stop()