package bigData.Spark;


import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import scala.collection.immutable.Map;

public class App {

	public static void main(String[] args) {

		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example")
				.config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();

// 		Per ogni autore, ottengo il tag, numero di tag utilizzati ed il suo id
//		Dataset<Row> ds = spark.sql(
//				"select author, ct_tag, count(ct_tag) as c from itwiki.word_count, itwiki.change_tag where rev_id=ct_rev_id group by author, ct_tag");
//		ds.groupBy("author").max("c").show(5000);

	
		Dataset<Row> ct_table = spark.table("itwiki.change_tag");
		
		Dataset<Row> page2revisionCommentWords_table = spark.table("itwiki.page2revisionCommentWords");
		
		Dataset <Row> joined = ct_table.join(page2revisionCommentWords_table,page2revisionCommentWords_table.col("rev_id").equalTo(ct_table.col("ct_rev_id")));
		
		/**
		 * QUERY 1 SPARK 
		 * 
		 */
		
		//Compute the average number of word in revision comment for each change tag
		
		// change_tag, avg(#word)
		
		Dataset <Row> averaged = joined.groupBy("ct_tag").avg("tot");
		
		averaged.show(5000);
		
		
		
		/**
		 * 
		 * QUERY 2 SPARK
		 * 
		 */
		
		
		
		
//		Dataset <Row> counted = joined.groupBy("author", "ct_tag").count();
//	
//		//TODO groubykey
//		
//		counted.groupBy("author").max("count").show(5000);
//		
	
		
		
		
		
		
		
		
		
		//????
		spark.stop();
	}

}
