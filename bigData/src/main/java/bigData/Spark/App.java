package bigData.Spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

	public static void main(String[] args) throws IOException {

		File outQuery1 = new File("/home/giuseppe/resultQuery1Spark.csv");
		File outQuery2 = new File("/home/giuseppe/resultQuery2Spark.csv");

		SparkSession spark = SparkSession.builder().appName("Java Spark project").enableHiveSupport()
				.getOrCreate();

		Dataset<Row> ct_table = spark.table("itwiki.change_tag");

		Dataset<Row> page2revisionCommentWords_table = spark.table("itwiki.page2revisionCommentWords");

		Dataset<Row> joined = ct_table.join(page2revisionCommentWords_table,
				page2revisionCommentWords_table.col("rev_id").equalTo(ct_table.col("ct_rev_id")));

		joined.cache();

		/**
		 * QUERY 1 SPARK Compute the average number of word in revision comment for each
		 * change tag
		 */
		writeOnFile("ct_tag" + "\t" + "avg(tot)", outQuery1);
		Dataset<Row> averaged = joined.groupBy("ct_tag").avg("tot");
		averaged.foreach(line -> writeOnFile(line.get(0).toString() + " \t" + line.get(1).toString(), outQuery1));
		//averaged.show(100);

		/**
		 * QUERY 2 SPARK Find for each author most used change tag
		 */
		writeOnFile("author" + "\t" + "max(count)", outQuery2);
		Dataset<Row> counted = joined.groupBy("author", "ct_tag").count();
		//TODO groubykey??????????????????????????????????
		counted.groupBy("author", "ct_tag").max("count");
		counted.foreach(line -> writeOnFile(line.get(0).toString() + " \t" + line.get(1).toString(), outQuery2));
		//counted.show(100);

		spark.stop();
	}

	static void writeOnFile(String resultsSingleRow, File out) {

		FileWriter fw;

		try {
			fw = new FileWriter(out, true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(resultsSingleRow + "\n");
			bw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}

/*
 * Dataset<Row> ds = spark.sql( "select author, ct_tag, count(ct_tag) as c from
 * itwiki.word_count, itwiki.change_tag where rev_id=ct_rev_id group by author,
 * ct_tag"); ds.groupBy("author").max("c").show(5000);
 * 
 */