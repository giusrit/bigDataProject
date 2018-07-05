package bigData.Spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class App {

	public static void main(String[] args) throws IOException {

		File outQuery1 = new File("/home/giuseppe/resultQuery1Spark.csv");
		File outQuery2 = new File("/home/giuseppe/resultQuery2Spark.csv");

		SparkSession spark = SparkSession.builder().appName("Java Spark project").enableHiveSupport().getOrCreate();

		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

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
		// averaged.show(100);

		/**
		 * QUERY 2 SPARK Find for each author most used change tag
		 */
		writeOnFile("author" + "\t" + "max(count)" + "\t" + "tag", outQuery2);
		Dataset<Row> counted = joined.groupBy("author", "ct_tag").count().alias("count");

		JavaPairRDD<String, String> jprdd = counted.toJavaRDD()
				.mapToPair(row -> new Tuple2<String, String>(row.get(0).toString(),
						new String(row.get(1).toString() + ";" + row.get(2).toString())));

		for (Tuple2<String, Iterable<String>> author : jprdd.groupByKey().collect()) {
			List<Tuple2<String, Integer>> ct_max = new ArrayList<Tuple2<String, Integer>>();
			String my_ct = "";
			int my_count = 0;

			for (String ct_tag : author._2) {

				my_ct = ct_tag.toString().split(";")[0];
				my_count = Integer.parseInt(ct_tag.toString().split(";")[1]);
				ct_max.add(new Tuple2<String, Integer>(my_ct, my_count));
			}

			context.parallelizePairs(
					context.parallelizePairs(ct_max).mapToPair(item -> item.swap()).sortByKey(false).take(1))
					.foreach(line -> writeOnFile(author._1 + " \t" + line._1 + "\t" + line._2, outQuery2));
		}

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