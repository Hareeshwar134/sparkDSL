package sparkdsl.Pack

object sparkObj {
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

 def main(args:Array[String]):Unit={
  
  val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val col_list=List("txnno","txndate","amount","category","product","spendby")
		val df=spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///C:/data/txnsheader")
		df.printSchema()
		df.show(false)
		
		/*val txndf = df.select("category","spendby")
		txndf.show(5)
		val filterdata = df.filter(col("category")==="Gymnastics")
		filterdata.show(5)*/
		
		val df11=df.select(col_list.map(col):_*)
		//Equals and Not equals
		val df1 = df.filter(col("category")==="Gymnastics" && col("spendby")==="cash")
		df1.show(5)
		val df2 = df.filter(col("category")==="Gymnastics" && col("spendby")=!="cash") 
		df2.show(5)
		val df3 = df.filter(col("category")==="Gymnastics" || col("spendby")=!="cash") 
		df3.show(5)
		val df4=df.filter(col("category").isin("Exercise & Fitness","Gymnastics","Team Sports"))
		df4.show(5) 
		val df5 = df.filter(col("product").like("Gymnastics%"))
		df5.show(5) 
		val df6 = df.filter(col("txnno")>5000 && col("spendby")==="cash")
		df6.show(6)
		val df7 = df6.filter(col("product").like("Weightlifting%"))
		df7.show(5) 
		
		//Expression
		val expdf=df.selectExpr("txnno","split(txndate,'-')[2] as year","amount") //Here selectExpr is the expression
		expdf.show(5)
		val transdf1=df.withColumn("txndate",expr("split(txndate,'-')[2]"))
		transdf1.show(5)
		val transdf2=df.withColumn("year",expr("split(txndate,'-')[2]"))
		transdf2.show(5)
		val transdf3=df.withColumn("txndate",expr("split(txndate,'-')[2]")).withColumnRenamed("txndate","year")//Here Renaming column
		transdf3.show(5)
		val transdf4=df.withColumn("category",expr("split(category,' ')[0]"))
		transdf4.show(5)
		val transdf5=df.withColumn("cat1",expr("split(category,' ')[0]"))
		transdf5.show(5)
		val litdf = df.withColumn("check",lit(1)) //Here lit is to hardcode the value in column where we gave column name as check
		litdf.show(5)
		println("----Conditional----")
		val conddf=df.withColumn("check",expr("case when spendby='credit' then 1 else 0 end"))
		conddf.show(50)    
		
		println("----DSL Aggregation----")
		val aggf=df.groupBy("category").agg(max("amount").alias("max_amount"))
		aggf.show()    
}
}