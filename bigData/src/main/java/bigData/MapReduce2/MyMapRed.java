package bigData.MapReduce2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MyMapRed {

	public static class MyMapRed_WordSum extends Mapper<LongWritable, Text, Text, IntWritable> {

		HashMap<String, Integer> myHash = new HashMap<>();
		HashMap<String, List<String>> myID = new HashMap<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				InputStream is = new ByteArrayInputStream(value.toString().getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(is);

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("page");

				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);

					if (nNode.getNodeType() == Node.ELEMENT_NODE) {

						Element eElement = (Element) nNode;

						// String id = eElement.getElementsByTagName("id").item(0).getTextContent();
						String comment = eElement.getElementsByTagName("comment").item(0).getTextContent();

						// String cleanedComment =
						// comment.toLowerCase().replaceAll("[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!]\"",
						// " ");

						String cleanedComment = comment.toLowerCase();

						cleanedComment = cleanedComment.replace("/*", " ");
						cleanedComment = cleanedComment.replace("*/", " ");
						cleanedComment = cleanedComment.replace(":", " ");
						cleanedComment = cleanedComment.replace(",", " ");
						cleanedComment = cleanedComment.replace("[", " ");
						cleanedComment = cleanedComment.replace("]", " ");
						cleanedComment = cleanedComment.replace("(", " ");
						cleanedComment = cleanedComment.replace(")", " ");
						cleanedComment = cleanedComment.replace(";", " ");
						cleanedComment = cleanedComment.replace("!", " ");
						cleanedComment = cleanedComment.replace("?", " ");
						cleanedComment = cleanedComment.replace("'", " ");
						cleanedComment = cleanedComment.replace("$", " ");
						cleanedComment = cleanedComment.replace("&", " ");
						cleanedComment = cleanedComment.replace("*", " ");
						cleanedComment = cleanedComment.replace("+", " ");
						cleanedComment = cleanedComment.replace("#", " ");
						cleanedComment = cleanedComment.replace("\"", " ");

						cleanedComment = cleanedComment.trim();

						StringTokenizer str = new StringTokenizer(cleanedComment);

						while (str.hasMoreTokens()) {

							String word = str.nextToken();

							if (!myHash.containsKey(word)) {
								myHash.put(word, 1);

								/* ================================= */
								// List<String> l =new ArrayList<>();
								//
								// l.add(id);
								//
								// myID.put(word, l);
								/* ================================= */

							} else {
								int sum = myHash.get(word) + 1;
								myHash.put(word, sum);

								/* ================================= */
								// myID.get(word).add(id);
								/* ================================= */
							}

						}
						// context.write(new Text(id), new Text(comment));

					}
				}
			} catch (Exception e) {
			}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			for (String word : myHash.keySet()) {

				context.write(new Text(word), new IntWritable(myHash.get(word)));

				/* ================================= */
				// for(int i=0; i<myID.get(word).size(); i++)
				// {
				// context.write(new Text(word), new
				// Text(myHash.get(word)+"\t"+myID.get(word)));//.get(i)));
				// }
				/* ================================= */

			}
		}
	}

	public static class myRed extends Reducer<Text, IntWritable, Text, IntWritable> {

		HashMap<String, List<String>> myRedHashID = new HashMap<>();
		HashMap<String, Integer> myRedHashSum = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
				/*
				 * String id_list = val.toString().split("\t")[1];
				 * 
				 * id_list = id_list.replace("[", ""); id_list = id_list.replace("]", "");
				 * 
				 * id_list = id_list.trim();
				 * 
				 * String[] ids = id_list.split(",");
				 * 
				 * for(int i=0; i<ids.length;i++) { if(!myRedHashID.containsKey(key.toString()))
				 * { List<String> l = new ArrayList<>();
				 * 
				 * l.add(ids[i]);
				 * 
				 * myRedHashID.put(key.toString(), l); }
				 * 
				 * else {
				 * 
				 * myRedHashID.get(key.toString()).add(ids[i]); } }
				 */

			}

			myRedHashSum.put(key.toString(), sum);

			// context.write(new Text(key.toString()), new
			// Text(myRedHashSum.get(key.toString()) + "," +
			// myRedHashID.get(key.toString())));

		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			// context.write(new Text("word ;"), new Text("sum ; page_id"));

			for (String word : myRedHashSum.keySet()) {
				// context.write(new Text(word+";"), new
				// Text(myRedHashSum.get(word)+";"+myRedHashID.get(word)));

				context.write(new Text(word + ";"), new IntWritable(myRedHashSum.get(word)));

			}

		}

	}

}
