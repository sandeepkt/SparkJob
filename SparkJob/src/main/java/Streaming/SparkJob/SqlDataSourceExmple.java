package Streaming.SparkJob;

import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SqlDataSourceExmple {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("hive.exec.dynamic.partition.mode", "nonstrict")
				.enableHiveSupport()
				.getOrCreate();
		
		BasicCreateSqlExample(spark);
		BasicSaveAsTableExample(spark);
		BasicInsertIntoTableExample(spark);
		BasicPartitionTableExample(spark);
		
		BasicParquetExample(spark);

	}
	
	private static void BasicCreateSqlExample (SparkSession spark) {
		
		Dataset<Row> JsonDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/jsondata.json").cache();
		
		Dataset<Row> JsonDF1 = JsonDF.filter("gender == 'Male'");
		
		Dataset<Row> JsonDF2 = JsonDF1.select("first_name","last_name","country","phone","gender");
		
		JsonDF2.createOrReplaceTempView("save_table_view");
		
		spark.sql("CREATE EXTERNAL TABLE sample_tab1 location '/warehouse/tablespace/external/hive/sample_db.db/sample_tab1' as SELECT * FROM save_table_view");
	}
	
	
	
	private static void BasicSaveAsTableExample(SparkSession spark) {
		
        Dataset<Row> JsonDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/jsondata.json").cache();
		
		Dataset<Row> JsonDF1 = JsonDF.filter("gender == 'Male'");
		
		Dataset<Row> JsonDF2 = JsonDF1.select("first_name","last_name","country","phone","gender");
		
		JsonDF2.createOrReplaceTempView("save_table_view2");
		
		HashMap<String,String> pathmap = new HashMap<String,String>();
		
		pathmap.put("path", "hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal:8020/warehouse/tablespace/external/hive/sample_db.db/sample_tab2");
		
		JsonDF2.write().format("orc").mode(SaveMode.Append).saveAsTable("sample_tab2");
		
	}
	
	private static void BasicInsertIntoTableExample(SparkSession spark) {
		
        Dataset<Row> JsonDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/jsondata.json").cache();
		
		Dataset<Row> JsonDF1 = JsonDF.filter("gender == 'Male'");
		
		Dataset<Row> JsonDF2 = JsonDF1.filter("id > 5");
		
		Dataset<Row> JsonDF3 = JsonDF2.select("first_name","last_name","country","phone","gender");
		
		JsonDF3.write().format("orc").mode(SaveMode.Append).insertInto("sample_tab2");
		
	}
	
	
	private static void BasicPartitionTableExample(SparkSession spark) {
		
        Dataset<Row> JsonDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/jsondata.json").cache();
		
		Dataset<Row> JsonDF1 = JsonDF.filter("gender == 'Male'");
		
		Dataset<Row> JsonDF2 = JsonDF1.filter("id > 5");
		
		Dataset<Row> JsonDF3 = JsonDF2.select("first_name","last_name","country","phone","gender");
		
		JsonDF3.write().format("orc").mode(SaveMode.Append).partitionBy("country").saveAsTable("sample_par_tab3");
	}
	
	
	private static void BasicParquetExample(SparkSession spark) {
		
        Dataset<Row> orderDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/orders.json").cache();
        
        Dataset<Row> peopleDF = spark.read().json("hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal/tmp/people.json").cache();
		Dataset<Row> orderDF1 = orderDF.select("product", "id");
		
		Dataset<Row> peopleDF1 = peopleDF.filter("gender == 'Male'");
		
		Dataset<Row> peopleDF2 = peopleDF1.filter("age >= 25");
				
		//Dataset<Row> joined = peopleDF2.join(orderDF, peopleDF2.col("id").equalTo(orderDF.col("id")));
		
		Dataset<Row> joined1 = peopleDF2.as("a").join(orderDF1.as("b")).where("a.id == b.id");
		
		joined1.printSchema();
		joined1.show(3);
		
        HashMap<String,String> pathmap = new HashMap<String,String>();
		
		
		pathmap.put("path", "hdfs://instance-1.us-east1-b.c.ultra-thought-262118.internal:8020/warehouse/tablespace/external/hive/sample_db.db/sample_joined");
		
		joined1.write().options(pathmap).format("orc").mode(SaveMode.Append).saveAsTable("sample_joined1");
		
		
		
		
		
		
		
		
		
		
		
		
		
	}

}
