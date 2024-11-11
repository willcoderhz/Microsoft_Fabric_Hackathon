# Sales and Gross Profit Margin Analysis using PySpark and Time Series Forecasting

## Project Overview
This project demonstrates how to perform comprehensive sales analysis, customer insights, and gross profit margin analysis using PySpark, Pandas, and statistical modeling techniques. It includes the extraction and transformation of Delta table data, detailed customer purchase behavior analysis, and time series forecasting to predict future trends.

## Project Structure
The project is divided into the following key sections:
1. **Loading and Preprocessing Data**: Using PySpark and Pandas to load Delta tables and prepare data for analysis.
2. **Sales Analysis**: Aggregating and visualizing monthly sales data.
3. **Customer Analysis**: Identifying top customers, including those who have become inactive, and reaching out to them.
4. **Gross Profit Margin Analysis**: Calculating monthly gross profit margins and forecasting future trends using time series analysis.
5. **Visualization**: Generating plots to visualize sales and gross profit trends.

## Technologies Used
- **PySpark**: For reading and processing large-scale data from Delta tables.
- **Pandas**: For data manipulation and analysis.
- **Matplotlib**: For data visualization.
- **Statsmodels**: For time series forecasting using the Holt-Winters Exponential Smoothing model.

## Prerequisites
Ensure you have the following installed:
- Python 3.x
- PySpark
- Pandas
- Matplotlib
- Statsmodels

## Installation
1. Clone this repository or download the source code.
2. Install the required Python packages:
   ```bash
   pip install pyspark pandas matplotlib statsmodels
   
## Usage

### 1. Load and Preprocess Data
The project starts by initializing a PySpark session and reading Delta table data from Azure Data Lake Storage (ADLS). The data is converted to Pandas DataFrames for further processing.


- ### Initialize Spark session
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

- ### Load Delta table
df_order_spark = spark.read.format("delta").load("<your-delta-table-path>")
df_order = df_order_spark.toPandas()

- ### Convert dates and add 'Year-Month' column
df_order['订单创建时间'] = pd.to_datetime(df_order['订单创建时间'])
df_order['Year-Month'] = df_order['订单创建时间'].dt.to_period('M')

### 2. Sales Analysis
Aggregate monthly sales data and visualize it using Matplotlib.

```python
# Aggregate sales by month
monthly_sales = df_order.groupby('Year-Month')['商品金额合计'].sum().reset_index()
monthly_sales['Year-Month'] = pd.to_datetime(monthly_sales['Year-Month'].astype(str))
monthly_sales.set_index('Year-Month', inplace=True)
```
Plot actual sales
```python
import matplotlib.pyplot as plt

plt.figure(figsize=(14, 7))
plt.plot(monthly_sales.index, monthly_sales['商品金额合计'], label='Actual Sales', color='blue')
plt.title('Monthly Sales Analysis')
plt.xlabel('Year-Month')
plt.ylabel('Sales Amount (Million RMB)')
plt.grid(True)
plt.show()
```
### 3. Customer Analysis
Identify the top 200 customers by total sales and those who have not made a purchase in the last 3 months.

```python
# Identify top 200 customers
top_customers = df_order.groupby('收货人/提货人')['商品金额合计'].sum().reset_index()
top_customers_sorted = top_customers.sort_values(by='商品金额合计', ascending=False).head(200)

# Find customers who have not purchased in the last 3 months
recent_orders = df_order[df_order['订单创建时间'] >= (df_order['订单创建时间'].max() - pd.DateOffset(months=3))]
recent_customers = recent_orders['收货人/提货人'].unique()
inactive_customers = top_customers_sorted[~top_customers_sorted['收货人/提货人'].isin(recent_customers)]
```

### 4. Gross Profit Margin Analysis
Calculate and visualize monthly gross profit margin.

```python
# Calculate total sales and cost by month
df_goods['Year-Month'] = pd.to_datetime(df_goods['商品发货时间'], errors='coerce').dt.to_period('M')
monthly_summary = df_goods.groupby('Year-Month').apply(lambda x: pd.Series({
    'Total Sales': (x['商品单价'] * x['商品数量']).sum(),
    'Total Cost': (x['商品成本价'] * x['商品数量']).sum()
})).reset_index()

monthly_summary['Gross Profit Margin (%)'] = ((monthly_summary['Total Sales'] - monthly_summary['Total Cost']) / monthly_summary['Total Sales']) * 100
monthly_summary['Year-Month'] = pd.to_datetime(monthly_summary['Year-Month'].astype(str))
monthly_summary.set_index('Year-Month', inplace=True)

# Plot gross profit margin
plt.figure(figsize=(14, 7))
plt.plot(monthly_summary.index, monthly_summary['Gross Profit Margin (%)'], label='Gross Profit Margin', color='blue')
plt.title('Monthly Gross Profit Margin')
plt.xlabel('Year-Month')
plt.ylabel('Gross Profit Margin (%)')
plt.grid(True)
plt.show()
```

### 4. Forecasting
Use the Holt-Winters Exponential Smoothing model to forecast future sales and profit margins.

```python
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Sales forecast
model = ExponentialSmoothing(monthly_sales['商品金额合计'], seasonal='add', seasonal_periods=12)
fit_model = model.fit()
future_sales = fit_model.forecast(steps=12)

# Plot forecasted sales
plt.figure(figsize=(14, 7))
plt.plot(monthly_sales.index, monthly_sales['商品金额合计'], label='Actual Sales', color='blue')
plt.plot(future_sales.index, future_sales, label='Forecasted Sales', linestyle='--', color='red')
plt.title('12-Month Sales Forecast')
plt.xlabel('Year-Month')
plt.ylabel('Sales Amount (Million RMB)')
plt.grid(True)
plt.show()
```

## Results
- **Top 200 Customers Identified**: The analysis identified customers with the highest total sales.
- **Inactive Customers**: Highlighted customers who have not made a purchase in the last 3 months.
- **Gross Profit Margin Calculated**: Monthly analysis was conducted, and 10-month forecasts were generated to predict future performance.
- **Visualized Trends**: Clear plots were created to showcase actual and forecasted data, facilitating better decision-making.

## Conclusion
This project provided valuable insights into customer purchasing behavior and gross profit margins, while also offering tools to forecast future trends. These analyses can help guide strategic business decisions and improve profitability.

## License
This project is licensed under the MIT License.


