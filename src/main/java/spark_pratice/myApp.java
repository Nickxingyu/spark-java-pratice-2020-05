package spark_pratice;

import java.io.Serializable;
import java.lang.Math;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.*;
import static org.apache.spark.sql.functions.*;

public class myApp 
{

	public static class Related_Data implements Serializable{
		private long x;
		private long y;
		public void setx(long x){
			this.x = x;
		};
		public void sety(long y){
			this.y = y;
		};
		public long getx(){
			return this.x;
		}
		public long gety(){
			return this.y;
		}
	}

	public static class Argument_of_cc implements Serializable{
		private long sum_pow_x;
		private long sum_pow_y;
		private long sum_xy;
		private long sum_x;
		private long sum_y;
		private long cnt_x;
		private long cnt_y;

		public Argument_of_cc(){

		}

		public Argument_of_cc(
			long sum_pow_x, long sum_pow_y, long sum_xy, long sum_x,
			long sum_y, long cnt_x, long cnt_y
		){
			this.sum_pow_x = sum_pow_x;
			this.sum_pow_y = sum_pow_y;
			this.sum_xy = sum_xy;
			this.sum_x = sum_x;
			this.sum_y = sum_y;
			this.cnt_x = cnt_x;
			this.cnt_y = cnt_y;
		}
		public long get_sum_pow_x(){
			return sum_pow_x;
		};
		public long get_sum_pow_y(){
			return sum_pow_y;
		};
		public long get_sum_xy(){
			return sum_xy;
		};
		public long get_sum_x(){
			return sum_x;
		};
		public long get_sum_y(){
			return sum_y;
		};
		public long get_cnt_x(){
			return cnt_x;
		};
		public long get_cnt_y(){
			return cnt_y;
		};
		public void set_sum_pow_x(long sum_pow_x){
			this.sum_pow_x =sum_pow_x;
		};
		public void set_sum_pow_y(long sum_pow_y){
			this.sum_pow_y =sum_pow_y;
		};
		public void set_sum_xy(long sum_xy){
			this.sum_xy =sum_xy;
		};
		public void set_sum_x(long sum_x){
			this.sum_x =sum_x;
		};
		public void set_sum_y(long sum_y){
			this.sum_y =sum_y;
		};
		public void set_cnt_x(long cnt_x){
			this.cnt_x =cnt_x;
		};
		public void set_cnt_y(long cnt_y){
			this.cnt_y =cnt_y;
		}
	}
	public static class Correlation_Coefficient extends Aggregator<Related_Data,Argument_of_cc,Double>{
		public Argument_of_cc zero(){
			return new Argument_of_cc(0,0,0,0,0,0,0);
		};
		public Argument_of_cc reduce(Argument_of_cc buffer, Related_Data data){
			buffer.set_cnt_x(buffer.get_cnt_x() + 1);
			buffer.set_cnt_y(buffer.get_cnt_y() + 1);
			buffer.set_sum_x(buffer.get_sum_x() + data.getx());
			buffer.set_sum_y(buffer.get_sum_y() + data.gety());
			buffer.set_sum_xy(buffer.get_sum_xy() + data.getx()*data.gety());
			buffer.set_sum_pow_x(buffer.get_sum_pow_x() + data.getx()*data.getx());
			buffer.set_sum_pow_y(buffer.get_sum_pow_y() + data.gety()*data.gety());
			return buffer;
		}
		public Argument_of_cc merge(Argument_of_cc b1, Argument_of_cc b2){
			b1.set_cnt_x(b1.get_cnt_x() + b2.get_cnt_x());
			b1.set_cnt_y(b1.get_cnt_y() + b2.get_cnt_y());
			b1.set_sum_x(b1.get_sum_x() + b2.get_sum_x());
			b1.set_sum_y(b1.get_sum_y() + b2.get_sum_y());
			b1.set_sum_xy(b1.get_sum_xy() + b2.get_sum_xy());
			b1.set_sum_pow_x(b1.get_sum_pow_x() + b2.get_sum_pow_x());
			b1.set_sum_pow_y(b1.get_sum_pow_y() + b2.get_sum_pow_y());

			return b1;
		}
		public Double finish(Argument_of_cc reduction){
			long cnt_x = reduction.get_cnt_x();
			long cnt_y = reduction.get_cnt_y();
			long sum_x = reduction.get_sum_x();
			long sum_y = reduction.get_sum_y();
			long sum_xy = reduction.get_sum_xy();
			long sum_pow_x = reduction.get_sum_pow_x();
			long sum_pow_y = reduction.get_sum_pow_y();
			
			double result = (double)(
								(sum_xy - sum_x*sum_y/cnt_y)
								/java.lang.Math.sqrt(sum_pow_x - sum_x*sum_x/cnt_x)
								*java.lang.Math.sqrt(sum_pow_y - sum_y*sum_y/cnt_y)
							);
			
			return result;
		}
		public Encoder<Argument_of_cc> bufferEncoder(){
			return Encoders.bean(Argument_of_cc.class);
		}
		public Encoder<Double> outputEncoder() {
			return Encoders.DOUBLE();
		  }
		
	}
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().appName("Simple Application")
							.config("spark.master","local")
							.getOrCreate();

		//Read csv files from Amazon S3.
		String data_source = "s3a://pf-new-hire/hero_xu/employees_tables";

		Dataset<Row> dept_emp =  spark.read()
			.option("header", "true")
			.option("inferSchema","true")
			.csv(data_source + "/dept_emp.csv");
		Dataset<Row> employees = spark.read()
			.option("header", "true")
			.option("inferSchema","true")
			.csv(data_source + "/employees.csv");
		Dataset<Row> salaries = spark.read()
			.option("header", "true")
			.option("inferSchema","true")
			.csv(data_source + "/salaries.csv");
		Dataset<Row> titles = spark.read()
			.option("header", "true")
			.option("inferSchema","true")
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
			latest_salaries_with_titles
			.groupBy("title")
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



		Dataset<Row> sa_se_age_g = employees.selectExpr(
			"emp_no as e_emp_no", 
			"hire_date",
			"birth_date",
			"gender")
		.join(
			salaries,
			col("e_emp_no").equalTo(col("emp_no"))
		).select(
			col("salary"),
			expr("DATEDIFF(CURRENT_DATE(),hire_date) as seniority"),
			expr("DATEDIFF(CURRENT_DATE(),birth_date) as age"),
			expr("IF(gender='M',0,1) as gender")
		);

		Encoder<Related_Data> related_dataEncoder = Encoders.bean(Related_Data.class);

		Dataset<Related_Data> ss = sa_se_age_g.selectExpr("salary as x","seniority as y").as(related_dataEncoder);
		Dataset<Related_Data> sa = sa_se_age_g.selectExpr("salary as x","age as y").as(related_dataEncoder);
		Dataset<Related_Data> sg = sa_se_age_g.selectExpr("salary as x","gender as y").as(related_dataEncoder);

		Correlation_Coefficient correlation_coefficient = new Correlation_Coefficient();

		TypedColumn<Related_Data, Double> cc = correlation_coefficient.toColumn();

		Dataset<Row> result_ss = ss.select(cc.name("c_ss")).selectExpr("'join_col'","c_ss");
		Dataset<Row> result_sa = sa.select(cc.name("c_sa")).selectExpr("'join_col'","c_sa");;
		Dataset<Row> result_sg = sg.select(cc.name("c_sg")).selectExpr("'join_col'","c_sg");;
		

		result_ss.show();

		/*.select(
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
		);//.coalesce(1).write().csv("/home/ubuntu/hero_xu/data/csv/Q4");
		*/
        spark.stop();
    }
}
