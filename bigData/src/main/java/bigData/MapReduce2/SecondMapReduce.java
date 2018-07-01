package bigData.MapReduce2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondMapReduce {

	public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		HashMap<String, Integer> myHash = new HashMap<>();
		HashMap<String, List<String>> myID = new HashMap<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			
			StringTokenizer str = new StringTokenizer(value.toString().split(";")[1]);
			
			while (str.hasMoreTokens()) {

				String word = str.nextToken();
				String occurences = str.nextToken();
				
				if (!myHash.containsKey(word)) {
					myHash.put(word, Integer.parseInt(occurences));

				} else {
					int sum = myHash.get(word) + Integer.parseInt(occurences);
					myHash.put(word, sum);
				}

			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			for (String word : myHash.keySet()) {
				context.write(new Text(word), new IntWritable(myHash.get(word)));
			}
		}
	}

	public static class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		HashMap<String, List<String>> myRedHashID = new HashMap<>();
		HashMap<String, Integer> myRedHashSum = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			myRedHashSum.put(key.toString(), sum);
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {


			for (String word : myRedHashSum.keySet()) {
				context.write(new Text(word), new IntWritable(myRedHashSum.get(word)));
			}

		}

	}

}
