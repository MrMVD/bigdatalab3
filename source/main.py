from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import os

# Инициализация спарка
spark = SparkSession.builder \
    .appName("CSV Data Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Загрузка данных
file_path = "../data/Employee_Salaries_-_2023.csv"  
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Называем таблицу
data.createOrReplaceTempView("employee_salaries")

# Мини обзор данных
print("Схема:")
data.printSchema()
print(f"Количество строк: {data.count()}")
data.show(5)

print("Общая статистика оклада:")
stats = spark.sql("""
    SELECT 
        COUNT(*) AS total_records,
        AVG(CAST(Base_Salary AS DOUBLE)) AS AVG_Base_Salary,
        MIN(CAST(Base_Salary AS DOUBLE)) AS MIN_Base_Salary,
        MAX(CAST(Base_Salary AS DOUBLE)) AS MAX_Base_Salary
    FROM employee_salaries
""")
stats.show()

print("Топ 5 работников по окладу:")
top_paid = spark.sql("""
    SELECT Division, Gender, Base_Salary
    FROM employee_salaries
    ORDER BY CAST(Base_Salary AS DOUBLE) DESC
    LIMIT 5
""")
top_paid.show()

# Гистограмма
top_paid_pd = top_paid.toPandas()
division = top_paid_pd['Division'].tolist()
base_salary = top_paid_pd["Base_Salary"].astype(float).tolist()

plt.figure(figsize=(12, 6))
plt.bar(division, base_salary, color='skyblue')
plt.title("Top Paid Divisions by Base Salary", fontsize=16)
plt.ylabel("Base Salary", fontsize=14)
plt.xlabel("Division", fontsize=14)
plt.xticks(rotation=5, ha='right') 
plt.grid(axis='y', linestyle='--', alpha=0.7)

directory_path = "../images"
os.makedirs(directory_path, exist_ok=True)
plt.savefig(directory_path+"/"+"base_salary_by_division.png")


print("Средний оклад по гендеру:")
avg_salary_job = spark.sql("""
    SELECT Gender, AVG(CAST(Base_Salary AS DOUBLE)) AS AVG_Base_Salary
    FROM employee_salaries
    GROUP BY Gender
    ORDER BY AVG_Base_Salary DESC
""")
avg_salary_job.show()

print("Количество работников в департаментах:")
employee_count_department = spark.sql("""
    SELECT Department_Name, COUNT(*) AS Employee_Count
    FROM employee_salaries
    GROUP BY Department_Name
    ORDER BY Employee_Count DESC
""")
employee_count_department.show()

# Круговая диограмма
employee_count_department_pd = employee_count_department.toPandas()
plt.figure(figsize=(8, 8))
plt.pie(employee_count_department_pd["Employee_Count"], 
    labels=employee_count_department_pd["Department_Name"],
    autopct='%1.1f%%', 
    startangle=140)
plt.title("Employee Distribution by Department")
directory_path = "../images"
os.makedirs(directory_path, exist_ok=True)
plt.savefig(directory_path+"/"+"employee_distribution_by_department.png")

print("Статистика оклада по департаменту:")
salary_dist_department = spark.sql("""
    SELECT 
        Department_Name, 
        AVG(CAST(Base_Salary AS DOUBLE)) AS AVG_Base_Salary,
        MIN(CAST(Base_Salary AS DOUBLE)) AS MIN_Base_Salary,
        MAX(CAST(Base_Salary AS DOUBLE)) AS MAX_Base_Salary
    FROM employee_salaries
    GROUP BY Department_Name
""")
salary_dist_department.show()

# Диаграма "Свечки"
salary_dist_department_pd = salary_dist_department.toPandas()

departments = salary_dist_department_pd["Department_Name"]
combined_salaries = [
    salary_dist_department_pd.loc[i, ["MIN_Base_Salary", "MAX_Base_Salary", "AVG_Base_Salary"]].values 
    for i in range(len(salary_dist_department_pd))
]

plt.figure(figsize=(12, 6))
plt.boxplot(combined_salaries, tick_labels=departments, widths=0.6, patch_artist=True,
            boxprops=dict(facecolor='lightblue', color='black'),
            medianprops=dict(color='red'),
            showfliers=False)

plt.title("Salary Distribution by Department")
plt.xlabel("Department")
plt.ylabel("Salary")
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
directory_path = "../images"
os.makedirs(directory_path, exist_ok=True)
plt.savefig(directory_path+"/"+"salary_distribution_by_department.png")


# Остановка спарка
spark.stop()
