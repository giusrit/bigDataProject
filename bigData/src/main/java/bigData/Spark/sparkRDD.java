package bigData.Spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class sparkRDD {

	public static void main(String[] args) {

		File outQuery1 = new File("/home/giuseppe/resultQuery1Spark.csv");
		// File outQuery2 = new File("/home/giuseppe/resultQuery2Spark.csv");


		SparkSession sparkSession = SparkSession.builder().appName("Java Spark project").enableHiveSupport()
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		// load the pair <ct_rev_id , ct_tag>
		JavaPairRDD<String, String> change_tag = sparkSession.sql("select * from itwiki.change_tag").toJavaRDD()
				.mapToPair(row -> new Tuple2<String, String>(row.get(3).toString(), row.get(4).toString()));

		
//		JavaRDD<String> page2RevisionCommentWords = sparkSession.sql("selct * from itwiki.page2revisioncommentwords")
//				.toJavaRDD().map(row -> new Turow.get(0).toString(), row.get(1).toString());

		JavaRDD<Page2RevisionCommentWords> peopleRDD = sparkSession
				.sql("select * from itwiki.page2revisioncommentwords").toJavaRDD().map(line -> {
					Page2RevisionCommentWords p = new Page2RevisionCommentWords();
					p.setAuthor(line.get(3).toString());
					p.setRev_id(line.get(0).toString());
					p.setTot(Integer.parseInt(line.get(1).toString()));////////////////////
					return p;
				});
		
		
		
		/**
		 * QUERY 1 SPARK Compute the average number of word in revision comment for each
		 * change tag
		 */
		
		
		
		
		
		jsc.close();
		sparkSession.stop();
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

	public static class Page2RevisionCommentWords {

		private String author;
		private int tot;
		private String rev_id;

		public Page2RevisionCommentWords(String author, int tot, String rev_id) {
			super();
			this.author = author;
			this.tot = tot;
			this.rev_id = rev_id;
		}

		public Page2RevisionCommentWords() {
			super();
		}

		public String getAuthor() {
			return author;
		}

		public void setAuthor(String author) {
			this.author = author;
		}

		public int getTot() {
			return tot;
		}

		public void setTot(int tot) {
			this.tot = tot;
		}

		public String getRev_id() {
			return rev_id;
		}

		public void setRev_id(String rev_id) {
			this.rev_id = rev_id;
		}

	}

}
