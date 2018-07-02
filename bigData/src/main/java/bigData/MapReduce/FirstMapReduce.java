package bigData.MapReduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
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

public class FirstMapReduce {

	public static class FirstMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		HashMap<Integer, HashMap<String, Integer>> myHash = new HashMap<>();
		HashMap<Integer, Integer> idToRevID = new HashMap<>();
		HashMap<Integer, String> idToUsername = new HashMap<>();



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

						String id = eElement.getElementsByTagName("id").item(0).getTextContent();
						String comment = eElement.getElementsByTagName("comment").item(0).getTextContent();
						String idRevString = eElement.getElementsByTagName("id").item(1).getTextContent();
						Integer idRev = Integer.parseInt(idRevString);
						String userName = new String();
						NodeList nl = eElement.getElementsByTagName("username");
						if(nl.getLength()>0) {
							userName =nl.item(0).getTextContent();
						}else {
							userName = "nullato";
						}


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

						int myID = Integer.parseInt(id);

						idToRevID.put(myID, idRev);



						idToUsername.put(myID, userName);


						while(str.hasMoreTokens())
						{

							String word = str.nextToken();

							if(!myHash.containsKey(myID))
							{
								HashMap<String,Integer> h = new HashMap<>();
								h.put(word, 1);
								myHash.put(myID, h);
							}
							else
							{
								if(!myHash.get(myID).keySet().contains(word))
								{
									myHash.get(myID).put(word, 1);
								}
								else
								{
									int sum = myHash.get(myID).get(word) + 1;
									myHash.get(myID).put(word, sum);
								}
							}
						}
					}
				}
			}
			catch(Exception e) {}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {

			for(Integer id : myHash.keySet())
			{
				for(String word : myHash.get(id).keySet())
				{
					context.write(new IntWritable(id), new Text(word + ";"+ myHash.get(id).get(word)+";"+idToRevID.get(id)+";"+idToUsername.get(id)));
				}
			}
		}
	}

	public static class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {

			String str = "";

			int tot = 0;
			int idRev=0;
			String username = new String();

			for(Text tx : values)
			{
				username = tx.toString().split(";")[3];
				idRev=Integer.parseInt(tx.toString().split(";")[2]);
				tot+=Integer.parseInt(tx.toString().split(";")[1]);
				String parola=tx.toString().split(";")[0];
				int countP = Integer.parseInt(tx.toString().split(";")[1]);
				str+=parola+" "+countP+" ";
			}

			context.write(key, new Text(str+";"+tot+";"+idRev+";"+username));
		}

	}

}

