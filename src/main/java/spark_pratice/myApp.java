package spark_pratice;


import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.expressions.*;
//import scala.collection.mutable.ListMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class myApp 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
							.config("spark.master","local")
							.getOrCreate();

		//Read csv files from Amazon S3.
		String data_source = "s3a://pf-new-hire/hero_xu/employees_tables";
		Dataset<Row> dept_emp =  spark.read()
			.schema("emp_no STRING ,dept_no STRING, from_date DATE, to_date DATE")
			.csv(data_source + "/dept_emp.csv");
		Dataset<Row> employees = spark.read()
			.schema("emp_no STRING, birth_date DATE, first_name STRING, last_name STRING, gender STRING, hire_date DATE")
			.csv(data_source + "/employees.csv");
		Dataset<Row> salaries = spark.read()
			.schema("emp_no STRING ,salary INT, from_date DATE, to_date DATE")
			.csv(data_source + "/salaries.csv");
		Dataset<Row> titles = spark.read()
			.schema("emp_no STRING ,title STRING, from_date DATE, to_date DATE")
			.csv(data_source + "/titles.csv");

//============================================Q1=====================================================//
//============================================Q1=====================================================//
		Dataset<Row> date_of_latest_salaries = 
			salaries.selectExpr("emp_no as s_emp_no", "from_date").groupBy("s_emp_no").agg(max("from_date"));
		
		Dataset<Row> date_of_latest_department = 
			dept_emp.selectExpr("emp_no as d_emp_no", "from_date").groupBy("d_emp_no").agg(max("from_date"));

		Dataset<Row> latest_salaries = date_of_latest_salaries.join(
			salaries, 
			date_of_latest_salaries.col("s_emp_no").equalTo(salaries.col("emp_no")),
			"INNER"
		).filter(
			col("max(from_date)").equalTo(col("from_date"))
		).selectExpr("emp_no","salary")
		.orderBy("emp_no");

		Dataset<Row> latest_department = date_of_latest_department.join(
			dept_emp, 
			date_of_latest_department.col("d_emp_no").equalTo(dept_emp.col("emp_no")),
			"INNER"
		).filter(
			col("max(from_date)").equalTo(col("from_date"))
		).selectExpr("d_emp_no","dept_no")
		.orderBy("d_emp_no");
//result
		latest_salaries.join(
			latest_department,
			latest_salaries.col("emp_no").equalTo(latest_department.col("d_emp_no")),
			"INNER"
		).selectExpr("dept_no","salary")
		.groupBy("dept_no").agg(avg("salary").cast("decimal(38,2)").alias("salary"))
		.orderBy(desc("salary"))
		.limit(1);//.coalesce(1).write().csv("/home/ubuntu/hero_xu/data/csv/Q1");

//============================================Q2=====================================================//

		Dataset<Row> latest_salaries_with_titles = 
			latest_salaries.selectExpr("emp_no as ls_emp_no","salary").join(
				titles,
				col("ls_emp_no").equalTo(col("emp_no")),
				"INNER"
			);

		Dataset<Row> rank_salaries_of_dif_title = 
			latest_salaries_with_titles
			.withColumn(
				"index", 
				row_number().over(Window.partitionBy("title").orderBy("salary"))
			);

		Dataset<Row> median_of_salaries = 
			latest_salaries_with_titles
			.groupBy(col("title")).agg(expr("COUNT('*')").alias("cnt"))
			.selectExpr("title as cnt_title","cnt")
			.join(
				rank_salaries_of_dif_title,
				col("title").equalTo(col("cnt_title")),
				"INNER"
			).filter(
				expr("(cnt%2=0 and (cnt/2=index or cnt/2+1=index)) or (cnt%2=1 and (cnt+1)/2=index)")
			).groupBy(col("title")).agg(avg("salary").alias("Median"))
			.selectExpr("title as m_title","Median");

		Dataset<Row> summary_of_salaries = 
			latest_salaries.selectExpr("emp_no as ls_emp_no","salary")
			.join(
				titles,
				col("ls_emp_no").equalTo(col("emp_no")),
				"INNER"
			).groupBy("title")
			.agg(
				expr("AVG(salary)").cast("decimal(38,2)").alias("AVG"),
				expr("STD(salary)").cast("decimal(38,2)").alias("STD"),
				expr("STD(salary)*STD(salary)").cast("decimal(38,2)").alias("VAR")
			)
			.join(
				median_of_salaries,
				col("title").equalTo(col("m_title"))
			);
		summary_of_salaries
		.selectExpr("title","AVG","Median","STD","VAR");//.coalesce(1).write().csv("/home/ubuntu/hero_xu/data/csv/Q2");

//============================================Q3=====================================================//
//============================================Q3=====================================================//
	Dataset<Row> gender_and_salary_of_employees = 
		latest_department.join(
			latest_salaries,
			latest_salaries.col("emp_no").equalTo(latest_department.col("d_emp_no")),
			"INNER"
		).selectExpr("d_emp_no","dept_no","salary").join(
			employees,
			col("d_emp_no").equalTo(employees.col("emp_no"))
		).selectExpr("dept_no","gender","salary");

	Dataset<Row> salaries_groupBy_dept_and_gender = 
		gender_and_salary_of_employees
		.groupBy("dept_no","gender").agg(avg("salary").alias("salary"))
		.orderBy("dept_no")
		.union(
			gender_and_salary_of_employees
			.groupBy("gender").agg(avg("salary").alias("salary"))
			.selectExpr("'Total'","gender","salary")
		);

	Dataset<Row> male_salary = 
		salaries_groupBy_dept_and_gender.filter(col("gender").equalTo("M"))
		.selectExpr("dept_no as m_dept_no","gender","salary as m_salary");

	Dataset<Row> female_salary = 
		salaries_groupBy_dept_and_gender.filter(col("gender").equalTo("F"));


//result
	male_salary.join(
		female_salary,
		col("m_dept_no").equalTo(col("dept_no")),
		"INNER"
	)
	.select(
		expr("dept_no"),
		expr("m_salary - salary as diff").cast("decimal(38,2)")
	)
	.orderBy("dept_no");//.coalesce(1).write().csv("/home/ubuntu/hero_xu/data/csv/Q3");


//============================================Q4=====================================================//
//============================================Q4=====================================================//

		employees.selectExpr(
			"emp_no as e_emp_no", 
			"hire_date",
			"birth_date",
			"gender")
		.join(
			salaries,
			col("e_emp_no").equalTo(col("emp_no"))
		).select(
			col("salary"),
			expr("DATEDIFF(CURRENT_DATE(),hire_date) as seniority").cast("decimal(38,4)"),
			expr("DATEDIFF(CURRENT_DATE(),birth_date) as age").cast("decimal(38,4)"),
			expr("IF(gender='M',0,1) as gender").cast("decimal(38,4)")
		)
		.select(
			col("salary"),
            col("seniority"),
            col("age"),
            col("gender"),
            expr("salary*seniority ss").cast("decimal(38,4)"),
            expr("salary*age sa").cast("decimal(38,4)"),
            expr("salary*gender sg").cast("decimal(38,4)")
		)
		.select(
			expr("AVG(salary) avg_sa").cast("decimal(38,4)"),
			expr("AVG(seniority) avg_se").cast("decimal(38,4)"), 
			expr("AVG(age) avg_age").cast("decimal(38,4)"), 
			expr("AVG(gender) avg_g").cast("decimal(38,4)"),
			expr("SUM(ss) sss").cast("decimal(38,4)"), 
			expr("SUM(sa) ssa").cast("decimal(38,4)"), 
			expr("SUM(sg) ssg").cast("decimal(38,4)"),
			expr("STD(salary) std_sa").cast("decimal(38,4)"), 
			expr("STD(seniority) std_se").cast("decimal(38,4)"), 
			expr("STD(age) std_age").cast("decimal(38,4)"),
			expr("STD(gender) std_g").cast("decimal(38,4)"),
			expr("COUNT(*) n").cast("decimal(38,0)")
			
		).select(
			expr("(sss-n*avg_sa*avg_se)/(n*std_sa*std_se) c_ss").cast("decimal(38,8)"),
    		expr("(ssa-n*avg_sa*avg_age)/(n*std_sa*std_age) c_sa").cast("decimal(38,8)"),
    		expr("(ssg-n*avg_sa*avg_g)/(n*std_sa*std_g) c_sg").cast("decimal(38,8)")
		).show();//.coalesce(1).write().csv("/home/ubuntu/hero_xu/data/csv/Q4");

        spark.stop();
    }
}
