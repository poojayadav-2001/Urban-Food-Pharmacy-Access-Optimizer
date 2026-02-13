# Urban Food & Pharmacy Access Optimizer  
A data-driven analysis to evaluate urban food and pharmacy accessibility using SQL ETL and Power BI visualization.

---

## Table of Contents
- <a href="#overview">Overview</a>
- <a href="#problem">Problem Statement</a>
- <a href="#tools">Tools and Technologies</a>
- <a href="#method">Method</a>
- <a href="#dashboard">Dashboard</a>
- <a href="#result">Result and Conclusion</a>
- <a href="#author">Author and Contact</a>

---

<h2><a class="anchor" id="overview"></a>Overview</h2>

This project provides a structured analysis of urban food and pharmacy accessibility using real-world store-level datasets. The objective is to clean, standardize, and consolidate grocery, supermarket, and pharmacy data to identify accessibility gaps and improve urban resource planning.

Using SQL for ETL (Extract, Transform, Load) and Power BI for interactive visualization, the project demonstrates a complete end-to-end data analytics workflow.

Key components include:
- Data cleaning and null value handling
- Duplicate record removal
- Data consolidation using SQL views
- Geographic validation of store coordinates
- Category-wise and city-wise accessibility analysis

---

<h2><a class="anchor" id="problem"></a>Problem Statement</h2>

Urban areas often face unequal access to essential services such as grocery stores and pharmacies.  
Without proper data cleaning and consolidation, it becomes difficult to analyze resource distribution accurately.

This project addresses that problem by:
- Standardizing inconsistent store data
- Validating missing values
- Creating a unified analytical dataset
- Enabling visual insights through Power BI dashboards

---

<h2><a class="anchor" id="tools"></a>Tools and Technologies</h2>

- **SQL (MySQL):** Data cleaning, ETL pipeline, view creation
- **Microsoft Excel:** Initial data collection and formatting
- **Microsoft Power BI:** Interactive dashboard and geospatial visualization
- **Git & GitHub:** Version control and project documentation

---

<h2><a class="anchor" id="method"></a>Method or Framework Followed in this Project</h2>

The project followed a 4-step analytical framework:

1. **Data Collection:** Gathering grocery, pharmacy, and supermarket datasets in Excel format.
2. **Data Cleaning (SQL ETL):**
   - Handling NULL and blank store names
   - Standardizing inconsistent entries
   - Removing duplicate records
   - Validating latitude & longitude values
3. **Data Consolidation:**
   - Combining multiple tables using `UNION ALL`
   - Creating analytical SQL views
   - Generating a final cleaned dataset
4. **Visualization & Insights:**
   - Category-wise store distribution
   - City-level accessibility comparison
   - Geospatial store mapping
   - Data validation reporting

---

<h2><a class="anchor" id="dashboard"></a>Dashboard</h2>

### 📊 Overview Dashboard

![Overview Dashboard](https://github.com/poojayadav-2001/Urban-Food-Pharmacy-Access-Optimizer/blob/85648f161c3e54b91664151b41ce5471d585d115/Dashboard%20Image/Overview.png)

### 📈 Area & Category Analysis

![Category Analysis](https://github.com/poojayadav-2001/Urban-Food-Pharmacy-Access-Optimizer/blob/85648f161c3e54b91664151b41ce5471d585d115/Dashboard%20Image/Area%20%26%20Category%20Analysis.png)

### 🏙️ City-wise Distribution

![City-wise Distribution](https://github.com/poojayadav-2001/Urban-Food-Pharmacy-Access-Optimizer/blob/85648f161c3e54b91664151b41ce5471d585d115/Dashboard%20Image/Map%20%26%20coverage.png)

### 🌍 Geospatial Map

![Geospatial Map](https://github.com/poojayadav-2001/Urban-Food-Pharmacy-Access-Optimizer/blob/85648f161c3e54b91664151b41ce5471d585d115/Dashboard%20Image/Insi.png)

---

<h2><a class="anchor" id="result"></a>Result and Conclusion</h2>

* **Key Finding:** The analysis identified inconsistencies in store naming and missing geographic values, which were standardized through SQL ETL processes.
* **Impact:** Consolidated datasets enabled accurate accessibility analysis and clear visualization of urban resource distribution.
* **Final Word:** Structured data cleaning and transformation are essential for reliable urban planning insights and decision-making.

---

## 👤 Author
* **Name:** Pooja Yadav  
* **Role:** Data Analyst / Business Analyst  

## 📩 Contact
* **LinkedIn:** [www.linkedin.com/in/pooja-yadav-59440a296]
* **Email:** [pooja7821yadav@gmail.com]
* **GitHub:** [https://github.com/poojayadav-2001/Urban-Food-Pharmacy-Access-Optimizer.git]

