import time
import mysql.connector

db_config = {
    'host': '127.0.0.1', 
    'user': 'tpch_user',      
    'password': 'tpch1234',   
    'database': 'tpch'        
}

relational_query = """
SELECT
    n.N_NAME as n_name,
    s.S_NAME as s_name,
    sum(l.l_quantity) as sum_qty,
    sum(l.l_extendedprice) as sum_base_price,
    sum(l.l_extendedprice * (1 - l.l_discount)) as sum_disc_price,
    sum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) as sum_charge,
    avg(l.l_quantity) as avg_qty,
    avg(l.l_extendedprice) as avg_price,
    avg(l.l_discount) as avg_disc,
    count(*) as count_order
FROM LINEITEM l
JOIN ORDERS o ON l.l_orderkey = o.o_orderkey
JOIN CUSTOMER c ON o.o_custkey = c.c_custkey
JOIN NATION n ON c.c_nationkey = n.n_nationkey
JOIN PARTSUPP ps ON l.l_partkey = ps.ps_partkey AND l.L_SUPPKEY = ps.ps_suppkey
JOIN SUPPLIER s ON ps.ps_suppkey = s.s_suppkey
WHERE l.l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY n.N_NAME, s.S_NAME;
"""

try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    execution_times = []
    print("Starting MySQL Benchmark (8 Runs)...")
    
    
    for i in range(1, 9):
        start_time = time.time()
        cursor.execute(relational_query)
        result = cursor.fetchall() 
        time_taken = time.time() - start_time
        execution_times.append(time_taken)
        print(f"Run {i}: {time_taken:.4f} seconds")

    avg_time = sum(execution_times) / len(execution_times)
    print("-" * 30)
    print(f"Average MySQL Execution Time: {avg_time:.4f} seconds")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()