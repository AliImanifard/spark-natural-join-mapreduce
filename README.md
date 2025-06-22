# Natural Join Using MapReduce in PySpark

This repository contains a PySpark program that performs a natural join operation between two CSV datasets using the MapReduce paradigm. It leverages Apache Spark's RDD transformations to simulate the behavior of a relational join.

## 📚 Overview

This code was developed as part of an academic project to understand and apply distributed data processing using Apache Hadoop and Spark. The goal was to simulate a **natural join** operation—combining records from two datasets based on a common key (`Employee_Id`).

## 📂 Input Data

- **`dept_data.csv`**: Contains department information (Department Name, Employee ID, Position).
- **`employee_data.csv`**: Contains employee information (Employee ID, Name, City, Salary).  

## 🛠️ How It Works

1. **Initialize Spark Session**  
   A Spark session is created using `SparkSession.builder`.

2. **Load CSV files into RDDs**  
   Each dataset is loaded and converted into an RDD, skipping the headers.

3. **Map Phase**  
   - The department dataset is mapped to key-value pairs where the key is `Employee_Id`.
   - The employee dataset is similarly mapped using the same key.

4. **Reduce Phase (Join)**  
   A join operation is performed on the two datasets based on the common key.

5. **Result Processing**  
   The joined result is collected and printed.

## 🧪 Sample Output Format

```text
('101', 'Sales', 'Manager', 'Alice', 'New York', '75000')
('102', 'HR', 'Executive', 'Bob', 'Chicago', '65000')
````

## 🚀 Getting Started

### Prerequisites

* Python 3.x
* Apache Spark
* `pyspark` installed:

  ```
  pip install pyspark
  ```

### Running the Script

Update the dataset paths in the script:

```python
dataset1_path = "path/to/dept_data.csv"
dataset2_path = "path/to/employee_data.csv"
```

Then run:

```bash
python natural_join_mapreduce.py
```

## 📖 Academic Context

This project was created as part of an academic course to explore how classical MapReduce principles can be adapted to Spark’s RDD model for real-world data operations.

## 📄 License

This project is licensed for academic and personal learning purposes.

