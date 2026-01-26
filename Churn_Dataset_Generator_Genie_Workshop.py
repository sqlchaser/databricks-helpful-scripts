# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom Churn Dataset Generator
# MAGIC
# MAGIC **Parameterized Setup - No Code Editing Required!**
# MAGIC
# MAGIC This notebook creates a complete synthetic churn dataset for Genie Space demos.
# MAGIC
# MAGIC **Just configure the widgets above and click "Run All"**
# MAGIC
# MAGIC What you'll get:
# MAGIC - 7 tables with 337K+ records
# MAGIC - Realistic churn patterns (~37% churn rate)
# MAGIC - Ready for Genie Spaces and Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration
# MAGIC
# MAGIC **→ Fill in the widgets that appear at the top of this notebook**

# COMMAND ----------

# Create configuration widgets
dbutils.widgets.text("catalog_name", "sandbox", "01. Catalog Name")
dbutils.widgets.text("schema_name", "churn_genie_demo", "02. Schema Name")

# Get and display configuration
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("="*70)
print(" CONFIGURATION")
print("="*70)
print(f" Catalog:    {catalog}")
print(f" Schema:     {schema}")
print(f" Full path:  {catalog}.{schema}")
print("="*70)
print()
print("✅ Configuration loaded!")
print("📝 Change values in the widgets above if needed")
print("🚀 Run the next cells to create your dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Setup & Cleanup

# COMMAND ----------

# Setup catalog and schema
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print(f"Setting up {catalog}.{schema}...")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✅ Using {catalog}.{schema}")

# COMMAND ----------

# Drop existing objects if they exist
tables = ['customers', 'accounts', 'usage_data', 'support_tickets', 
          'billing_payments', 'network_quality', 'churn_labels']
views = ['customer_support_summary', 'latest_usage', 'customer_360', 'high_risk_customers']

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Cleaning up existing tables/views...")
for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")
for view in views:
    spark.sql(f"DROP VIEW IF EXISTS {catalog}.{schema}.{view}")
    
print("✅ Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Tables
# MAGIC
# MAGIC This will take 2-3 minutes total

# COMMAND ----------

# DBTITLE 1,1. Customers Table (5,000 records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating customers...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.customers AS
WITH customer_ids AS (
  SELECT 
    CONCAT('CUST', LPAD(CAST(id AS STRING), 6, '0')) AS customer_id, 
    id,
    ABS(HASH(id, 1)) % 100 AS rand1,
    ABS(HASH(id, 2)) % 100 AS rand2,
    ABS(HASH(id, 3)) % 100 AS rand3,
    ABS(HASH(id, 4)) % 100 AS rand4,
    ABS(HASH(id, 5)) % 100 AS rand5
  FROM RANGE(1, 5001)
)
SELECT 
  customer_id,
  CASE WHEN rand1 < 6 THEN 'John' WHEN rand1 < 12 THEN 'Jane' WHEN rand1 < 19 THEN 'Michael'
       WHEN rand1 < 25 THEN 'Sarah' WHEN rand1 < 31 THEN 'David' WHEN rand1 < 37 THEN 'Emma'
       WHEN rand1 < 44 THEN 'Robert' WHEN rand1 < 50 THEN 'Lisa' WHEN rand1 < 56 THEN 'James'
       WHEN rand1 < 62 THEN 'Maria' WHEN rand1 < 69 THEN 'William' WHEN rand1 < 75 THEN 'Jennifer'
       WHEN rand1 < 81 THEN 'Daniel' WHEN rand1 < 87 THEN 'Jessica' WHEN rand1 < 94 THEN 'Joseph' ELSE 'Ashley' END AS first_name,
  CASE WHEN rand2 < 7 THEN 'Smith' WHEN rand2 < 13 THEN 'Johnson' WHEN rand2 < 20 THEN 'Williams'
       WHEN rand2 < 27 THEN 'Brown' WHEN rand2 < 33 THEN 'Jones' WHEN rand2 < 40 THEN 'Garcia'
       WHEN rand2 < 47 THEN 'Miller' WHEN rand2 < 53 THEN 'Davis' WHEN rand2 < 60 THEN 'Rodriguez'
       WHEN rand2 < 67 THEN 'Martinez' WHEN rand2 < 73 THEN 'Hernandez' WHEN rand2 < 80 THEN 'Lopez'
       WHEN rand2 < 87 THEN 'Wilson' WHEN rand2 < 93 THEN 'Anderson' ELSE 'Thomas' END AS last_name,
  CAST(18 + (ABS(HASH(id, 6)) % 57) AS INT) AS age,
  CASE WHEN rand3 < 48 THEN 'M' WHEN rand3 < 96 THEN 'F' ELSE 'Other' END AS gender,
  CASE WHEN rand4 < 7 THEN 'CA' WHEN rand4 < 13 THEN 'TX' WHEN rand4 < 20 THEN 'FL' WHEN rand4 < 27 THEN 'NY'
       WHEN rand4 < 33 THEN 'PA' WHEN rand4 < 40 THEN 'IL' WHEN rand4 < 47 THEN 'OH' WHEN rand4 < 53 THEN 'GA'
       WHEN rand4 < 60 THEN 'NC' WHEN rand4 < 67 THEN 'MI' WHEN rand4 < 73 THEN 'NJ' WHEN rand4 < 80 THEN 'VA'
       WHEN rand4 < 87 THEN 'WA' WHEN rand4 < 93 THEN 'AZ' ELSE 'MA' END AS state,
  CASE WHEN rand5 < 7 THEN 'Los Angeles' WHEN rand5 < 13 THEN 'Houston' WHEN rand5 < 20 THEN 'Phoenix'
       WHEN rand5 < 27 THEN 'San Antonio' WHEN rand5 < 33 THEN 'San Diego' WHEN rand5 < 40 THEN 'Dallas'
       WHEN rand5 < 47 THEN 'San Jose' WHEN rand5 < 53 THEN 'Austin' WHEN rand5 < 60 THEN 'Jacksonville'
       WHEN rand5 < 67 THEN 'Fort Worth' WHEN rand5 < 73 THEN 'Columbus' WHEN rand5 < 80 THEN 'Charlotte'
       WHEN rand5 < 87 THEN 'Seattle' WHEN rand5 < 93 THEN 'Denver' ELSE 'Boston' END AS city,
  CAST(300 + (ABS(HASH(id, 7)) % 550) AS INT) AS credit_score,
  DATE_ADD('2023-01-01', CAST(ABS(HASH(id, 8)) % 730 AS INT)) AS registration_date
FROM customer_ids
""")

count = spark.table(f"{catalog}.{schema}.customers").count()
print(f"✅ customers: {count:,} records")

# COMMAND ----------

# DBTITLE 1,2. Accounts Table (5,000 records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating accounts...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.accounts AS
WITH customer_base AS (
  SELECT customer_id, CAST(SUBSTRING(customer_id, 5) AS BIGINT) AS id
  FROM {catalog}.{schema}.customers
)
SELECT 
  CONCAT('ACC', LPAD(CAST(id AS STRING), 6, '0')) AS account_id,
  customer_id,
  CASE WHEN ABS(HASH(id, 10)) % 100 < 25 THEN 'Basic' WHEN ABS(HASH(id, 11)) % 100 < 60 THEN 'Standard'
       WHEN ABS(HASH(id, 12)) % 100 < 85 THEN 'Premium' ELSE 'Unlimited' END AS plan_type,
  ROUND(30 + ((ABS(HASH(id, 13)) % 10000) / 10000.0) * 120, 2) AS monthly_charge,
  CASE WHEN ABS(HASH(id, 14)) % 100 < 50 THEN 'Month-to-Month' WHEN ABS(HASH(id, 15)) % 100 < 80 THEN 'One Year' ELSE 'Two Year' END AS contract_type,
  CASE WHEN ABS(HASH(id, 16)) % 100 < 60 THEN TRUE ELSE FALSE END AS paperless_billing,
  CASE WHEN ABS(HASH(id, 17)) % 100 < 40 THEN 'Credit Card' WHEN ABS(HASH(id, 18)) % 100 < 70 THEN 'Bank Transfer'
       WHEN ABS(HASH(id, 19)) % 100 < 90 THEN 'Electronic Check' ELSE 'Mailed Check' END AS payment_method,
  CASE WHEN ABS(HASH(id, 20)) % 100 < 65 THEN TRUE ELSE FALSE END AS autopay_enabled,
  CAST(1 + (ABS(HASH(id, 21)) % 71) AS INT) AS tenure_months,
  ROUND(100 + ((ABS(HASH(id, 22)) % 10000) / 10000.0) * 9900, 2) AS total_charges,
  CASE WHEN ABS(HASH(id, 23)) % 100 < 40 THEN TRUE ELSE FALSE END AS device_protection,
  CASE WHEN ABS(HASH(id, 24)) % 100 < 35 THEN TRUE ELSE FALSE END AS tech_support,
  CASE WHEN ABS(HASH(id, 25)) % 100 < 30 THEN TRUE ELSE FALSE END AS family_plan,
  CASE WHEN ABS(HASH(id, 26)) % 100 < 40 THEN 1 WHEN ABS(HASH(id, 27)) % 100 < 70 THEN 2
       WHEN ABS(HASH(id, 28)) % 100 < 85 THEN 3 WHEN ABS(HASH(id, 29)) % 100 < 95 THEN 4 ELSE 5 END AS num_lines
FROM customer_base
""")

count = spark.table(f"{catalog}.{schema}.accounts").count()
print(f"✅ accounts: {count:,} records")

# COMMAND ----------

# DBTITLE 1,3. Usage Data (~65K records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating usage_data (this may take a minute)...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.usage_data AS
WITH customer_base AS (
  SELECT customer_id, CAST(SUBSTRING(customer_id, 5) AS BIGINT) AS id FROM {catalog}.{schema}.customers
),
month_sequence AS (
  SELECT customer_id, id, month_offset,
    DATE_FORMAT(ADD_MONTHS(DATE_ADD('2023-01-01', CAST(ABS(HASH(id, 30)) % 300 AS INT)), month_offset), 'yyyy-MM') AS usage_month
  FROM customer_base
  LATERAL VIEW EXPLODE(SEQUENCE(0, CAST(3 + (ABS(HASH(id, 31)) % 20) AS INT))) AS month_offset
  WHERE month_offset <= 23
)
SELECT 
  CONCAT('USG', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY customer_id, month_offset) AS STRING), 8, '0')) AS usage_id,
  customer_id, usage_month,
  CAST(ABS(HASH(customer_id, usage_month, 1)) % 3000 AS INT) AS voice_minutes,
  CAST(ABS(HASH(customer_id, usage_month, 2)) % 1000 AS INT) AS sms_count,
  ROUND((ABS(HASH(customer_id, usage_month, 3)) % 10000) / 100.0, 2) AS data_usage_gb,
  ROUND((ABS(HASH(customer_id, usage_month, 4)) % 5000) / 100.0, 2) AS roaming_charges,
  ROUND((ABS(HASH(customer_id, usage_month, 5)) % 3000) / 100.0, 2) AS overage_charges,
  CAST(ABS(HASH(customer_id, usage_month, 6)) % 100 AS INT) AS international_calls,
  CAST(ABS(HASH(customer_id, usage_month, 7)) % 200 AS INT) AS peak_usage_hours
FROM month_sequence
""")

count = spark.table(f"{catalog}.{schema}.usage_data").count()
print(f"✅ usage_data: {count:,} records")

# COMMAND ----------

# DBTITLE 1,4. Support Tickets (~9K records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating support_tickets...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.support_tickets AS
WITH customer_sample AS (
  SELECT customer_id, CAST(SUBSTRING(customer_id, 5) AS BIGINT) AS id
  FROM {catalog}.{schema}.customers WHERE ABS(HASH(customer_id, 40)) % 100 < 60
),
ticket_sequence AS (
  SELECT customer_id, id, ticket_num
  FROM customer_sample
  LATERAL VIEW EXPLODE(SEQUENCE(1, CAST(1 + (ABS(HASH(id, 41)) % 5) AS INT))) AS ticket_num
),
ticket_data AS (
  SELECT ROW_NUMBER() OVER (ORDER BY customer_id, ticket_num) AS row_num,
    customer_id, ticket_num, id,
    ABS(HASH(id, ticket_num, 1)) % 100 AS type_rand,
    ABS(HASH(id, ticket_num, 2)) % 100 AS priority_rand,
    ABS(HASH(id, ticket_num, 3)) % 100 AS status_rand,
    ABS(HASH(id, ticket_num, 4)) % 680 AS date_offset,
    1 + (ABS(HASH(id, ticket_num, 5)) % 14) AS resolution_days,
    ABS(HASH(id, ticket_num, 6)) % 100 AS resolved_rand,
    ABS(HASH(id, ticket_num, 7)) % 100 AS satisfaction_rand,
    ABS(HASH(id, ticket_num, 8)) % 100 AS escalated_rand
  FROM ticket_sequence
)
SELECT 
  CONCAT('TKT', LPAD(CAST(row_num AS STRING), 7, '0')) AS ticket_id, customer_id,
  CASE WHEN type_rand < 12 THEN 'Technical Issue' WHEN type_rand < 25 THEN 'Billing Question'
       WHEN type_rand < 37 THEN 'Service Outage' WHEN type_rand < 50 THEN 'Plan Change Request'
       WHEN type_rand < 62 THEN 'Device Support' WHEN type_rand < 75 THEN 'Network Quality'
       WHEN type_rand < 87 THEN 'Account Access' ELSE 'Feature Request' END AS ticket_type,
  CASE WHEN priority_rand < 25 THEN 'Low' WHEN priority_rand < 50 THEN 'Medium' WHEN priority_rand < 75 THEN 'High' ELSE 'Critical' END AS priority,
  CASE WHEN status_rand < 25 THEN 'Open' WHEN status_rand < 50 THEN 'In Progress' WHEN status_rand < 75 THEN 'Resolved' ELSE 'Closed' END AS status,
  DATE_ADD('2023-01-01', date_offset) AS created_date,
  CASE WHEN resolved_rand > 20 THEN DATE_ADD(DATE_ADD('2023-01-01', date_offset), resolution_days) ELSE NULL END AS resolved_date,
  CASE WHEN resolved_rand > 20 THEN resolution_days ELSE NULL END AS resolution_time_days,
  CASE WHEN satisfaction_rand > 30 THEN CAST(1 + (satisfaction_rand % 5) AS INT) ELSE NULL END AS satisfaction_score,
  CASE WHEN escalated_rand < 20 THEN TRUE ELSE FALSE END AS escalated
FROM ticket_data
""")

count = spark.table(f"{catalog}.{schema}.support_tickets").count()
print(f"✅ support_tickets: {count:,} records")

# COMMAND ----------

# DBTITLE 1,5. Billing Payments (~75K records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating billing_payments...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.billing_payments AS
WITH customer_base AS (
  SELECT customer_id, CAST(SUBSTRING(customer_id, 5) AS BIGINT) AS id FROM {catalog}.{schema}.customers
),
payment_sequence AS (
  SELECT customer_id, id, payment_num,
    DATE_ADD(DATE_ADD('2023-01-01', CAST(ABS(HASH(id, 50)) % 300 AS INT)), payment_num * 30) AS payment_date
  FROM customer_base
  LATERAL VIEW EXPLODE(SEQUENCE(0, CAST(6 + (ABS(HASH(id, 51)) % 18) AS INT))) AS payment_num
  WHERE payment_num <= 23
)
SELECT 
  CONCAT('PAY', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY customer_id, payment_num) AS STRING), 8, '0')) AS payment_id,
  customer_id, payment_date,
  ROUND(30 + ((ABS(HASH(customer_id, payment_date, 1)) % 10000) / 10000.0) * 120, 2) AS amount,
  CASE WHEN ABS(HASH(customer_id, payment_date, 2)) % 100 < 5 THEN 'Failed' ELSE 'Success' END AS payment_status,
  CASE WHEN ABS(HASH(customer_id, payment_date, 3)) % 100 < 10 AND ABS(HASH(customer_id, payment_date, 2)) % 100 >= 5 THEN TRUE ELSE FALSE END AS late_payment,
  CASE WHEN ABS(HASH(customer_id, payment_date, 3)) % 100 < 10 AND ABS(HASH(customer_id, payment_date, 2)) % 100 >= 5
       THEN CAST(1 + (ABS(HASH(customer_id, payment_date, 4)) % 30) AS INT) ELSE 0 END AS days_late,
  CASE WHEN ABS(HASH(customer_id, payment_date, 5)) % 100 < 40 THEN 'Credit Card' WHEN ABS(HASH(customer_id, payment_date, 5)) % 100 < 70 THEN 'Bank Transfer' ELSE 'Electronic Check' END AS payment_method
FROM payment_sequence
""")

count = spark.table(f"{catalog}.{schema}.billing_payments").count()
print(f"✅ billing_payments: {count:,} records")

# COMMAND ----------

# DBTITLE 1,6. Network Quality (~179K records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating network_quality (this may take a minute)...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.network_quality AS
WITH customer_sample AS (
  SELECT customer_id, CAST(SUBSTRING(customer_id, 5) AS BIGINT) AS id
  FROM {catalog}.{schema}.customers WHERE ABS(HASH(customer_id, 60)) % 100 < 80
),
week_sequence AS (
  SELECT customer_id, id, week_num,
    DATE_ADD(DATE_ADD('2023-01-01', CAST(ABS(HASH(id, 61)) % 300 AS INT)), week_num * 7) AS measurement_date
  FROM customer_sample
  LATERAL VIEW EXPLODE(SEQUENCE(0, CAST(10 + (ABS(HASH(id, 62)) % 70) AS INT))) AS week_num
  WHERE week_num <= 79
)
SELECT 
  CONCAT('QUA', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY customer_id, week_num) AS STRING), 8, '0')) AS quality_id,
  customer_id, measurement_date,
  ROUND(10 + ((ABS(HASH(customer_id, measurement_date, 1)) % 10000) / 10000.0) * 190, 2) AS avg_download_speed_mbps,
  ROUND(5 + ((ABS(HASH(customer_id, measurement_date, 2)) % 10000) / 10000.0) * 45, 2) AS avg_upload_speed_mbps,
  ROUND(-110 + ((ABS(HASH(customer_id, measurement_date, 3)) % 10000) / 10000.0) * 60, 2) AS signal_strength,
  ROUND((ABS(HASH(customer_id, measurement_date, 4)) % 1000) / 100.0, 2) AS call_drop_rate,
  ROUND(95 + ((ABS(HASH(customer_id, measurement_date, 5)) % 500) / 100.0), 2) AS network_availability,
  ROUND(10 + ((ABS(HASH(customer_id, measurement_date, 6)) % 10000) / 10000.0) * 140, 2) AS latency_ms,
  ROUND(0.5 + ((ABS(HASH(customer_id, measurement_date, 7)) % 10000) / 10000.0) * 14.5, 2) AS tower_distance_km
FROM week_sequence
""")

count = spark.table(f"{catalog}.{schema}.network_quality").count()
print(f"✅ network_quality: {count:,} records")

# COMMAND ----------

# DBTITLE 1,7. Churn Labels (5,000 records)
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating churn_labels...")

spark.sql(f"""
CREATE TABLE {catalog}.{schema}.churn_labels AS
WITH customer_accounts AS (
  SELECT c.customer_id, a.account_id, a.contract_type, a.tenure_months, a.monthly_charge, a.autopay_enabled,
    CAST(SUBSTRING(c.customer_id, 5) AS BIGINT) AS id
  FROM {catalog}.{schema}.customers c JOIN {catalog}.{schema}.accounts a ON c.customer_id = a.customer_id
),
churn_probabilities AS (
  SELECT customer_id, account_id, id,
    LEAST(0.85, 0.15 + 
      CASE WHEN contract_type = 'Month-to-Month' THEN 0.25 WHEN contract_type = 'One Year' THEN 0.10 ELSE 0 END +
      CASE WHEN tenure_months < 6 THEN 0.25 WHEN tenure_months < 12 THEN 0.15 WHEN tenure_months > 36 THEN -0.15 ELSE 0 END +
      CASE WHEN monthly_charge > 100 THEN 0.15 ELSE 0 END +
      CASE WHEN NOT autopay_enabled THEN 0.10 ELSE 0 END
    ) AS churn_probability
  FROM customer_accounts
),
churn_outcomes AS (
  SELECT customer_id, account_id, id, churn_probability,
    CASE WHEN (ABS(HASH(id, 70)) % 10000) / 10000.0 < churn_probability THEN 1 ELSE 0 END AS churned
  FROM churn_probabilities
)
SELECT customer_id, account_id, churned,
  CASE WHEN churned = 1 THEN DATE_SUB('2024-10-31', CAST(1 + (ABS(HASH(id, 71)) % 180) AS INT)) ELSE NULL END AS churn_date,
  CASE WHEN churned = 1 THEN
    CASE WHEN ABS(HASH(id, 72)) % 100 < 14 THEN 'Competitor Offer' WHEN ABS(HASH(id, 73)) % 100 < 29 THEN 'Price Too High'
         WHEN ABS(HASH(id, 74)) % 100 < 43 THEN 'Poor Service Quality' WHEN ABS(HASH(id, 75)) % 100 < 57 THEN 'Moved to Different Area'
         WHEN ABS(HASH(id, 76)) % 100 < 71 THEN 'Dissatisfied with Support' WHEN ABS(HASH(id, 77)) % 100 < 86 THEN 'Found Better Plan'
         ELSE 'Network Issues' END
  ELSE NULL END AS churn_reason,
  ROUND(churn_probability * 100, 2) AS churn_probability_score
FROM churn_outcomes
""")

count = spark.table(f"{catalog}.{schema}.churn_labels").count()
print(f"✅ churn_labels: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Views

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

print("Creating views...")

# Customer Support Summary
spark.sql(f"""
CREATE VIEW {catalog}.{schema}.customer_support_summary AS
SELECT customer_id, COUNT(*) as total_tickets, AVG(satisfaction_score) as avg_satisfaction,
  SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open_tickets,
  SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed_tickets,
  AVG(resolution_time_days) as avg_resolution_days,
  SUM(CASE WHEN escalated THEN 1 ELSE 0 END) as escalated_tickets
FROM {catalog}.{schema}.support_tickets GROUP BY customer_id
""")

# Latest Usage
spark.sql(f"""
CREATE VIEW {catalog}.{schema}.latest_usage AS
SELECT customer_id, MAX(usage_month) as latest_month,
  AVG(voice_minutes) as avg_voice_minutes, AVG(sms_count) as avg_sms_count,
  AVG(data_usage_gb) as avg_data_usage_gb, AVG(roaming_charges) as avg_roaming_charges,
  AVG(overage_charges) as avg_overage_charges
FROM {catalog}.{schema}.usage_data GROUP BY customer_id
""")

# Customer 360
spark.sql(f"""
CREATE VIEW {catalog}.{schema}.customer_360 AS
SELECT c.customer_id, c.first_name, c.last_name, c.age, c.gender, c.state, c.city, c.credit_score,
  a.plan_type, a.monthly_charge, a.contract_type, a.tenure_months, a.autopay_enabled,
  cl.churned, cl.churn_probability_score, cl.churn_reason
FROM {catalog}.{schema}.customers c 
JOIN {catalog}.{schema}.accounts a ON c.customer_id = a.customer_id
JOIN {catalog}.{schema}.churn_labels cl ON c.customer_id = cl.customer_id
""")

# High Risk Customers
spark.sql(f"""
CREATE VIEW {catalog}.{schema}.high_risk_customers AS
SELECT cv.customer_id, cv.first_name, cv.last_name, cv.plan_type, cv.monthly_charge,
  cv.tenure_months, cv.churn_probability_score, css.total_tickets, css.avg_satisfaction
FROM {catalog}.{schema}.customer_360 cv
LEFT JOIN {catalog}.{schema}.customer_support_summary css ON cv.customer_id = css.customer_id
WHERE cv.churned = 0 AND cv.churn_probability_score > 60
ORDER BY cv.churn_probability_score DESC
""")

print("✅ All 4 views created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!

# COMMAND ----------

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

# Get counts
counts = {}
for table in ['customers', 'accounts', 'usage_data', 'support_tickets', 'billing_payments', 'network_quality', 'churn_labels']:
    counts[table] = spark.table(f"{catalog}.{schema}.{table}").count()

# Get churn stats
churn_stats = spark.sql(f"""
  SELECT COUNT(*) as total, SUM(churned) as churned,
    ROUND(100.0 * SUM(churned) / COUNT(*), 2) as churn_rate
  FROM {catalog}.{schema}.churn_labels
""").collect()[0]

# Display summary
print("="*70)
print("🎉 SETUP COMPLETE!")
print("="*70)
print(f"\n📍 Location: {catalog}.{schema}")
print(f"\n📊 Tables Created:")
for table, count in counts.items():
    print(f"   {table}: {count:,}")
print(f"\n   TOTAL: {sum(counts.values()):,} records")
print(f"\n📈 Churn Statistics:")
print(f"   Total customers: {churn_stats['total']:,}")
print(f"   Churned: {churn_stats['churned']:,}")
print(f"   Active: {churn_stats['total'] - churn_stats['churned']:,}")
print(f"   Churn rate: {churn_stats['churn_rate']}%")
print("\n"+"="*70)
print("🚀 NEXT STEPS:")
print("="*70)
print(f"1. Create a Genie Space")
print(f"2. Point it to: {catalog}.{schema}")
print(f"3. Test with: 'What's the churn rate by plan type?'")
print("="*70)