package bigData.MapReduce;

import java.io.IOException;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigData.MapReduce.MyMapRed_WordCount.MyMap;
import bigData.MapReduce.MyMapRed_WordCount.myRed;
 
public class ParserDriver {
 
    /** Krishna - for processing XML file using Hadoop MapReduce
     * @param args
     * @throws IOException 
     * @throws IllegalArgumentException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
 
            Configuration conf = new Configuration();
            // conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 2048);
 
            // OR alternatively you can set it this way, the name of the
            // property is
            // "mapreduce.input.fixedlengthinputformat.record.length"
            // conf.setInt("mapreduce.input.fixedlengthinputformat.record.length",
            // 2048);
           // String[] arg = new GenericOptionsParser(conf, args).getRemainingArgs();
 
            conf.set("START_TAG_KEY", "<page>");
            conf.set("END_TAG_KEY", "</page>");
            conf.set(TextOutputFormat.SEPERATOR, ";");
 
            Job job = Job.getInstance(conf);
            job.setJarByClass(ParserDriver.class);
            job.setMapperClass(MyMap.class);
            job.setReducerClass(myRed.class);
 
            job.setNumReduceTasks(1);
 
            job.setInputFormatClass(XMLInputFormat.class);
            job.setOutputValueClass(TextOutputFormat.class);
 
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
 
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
 
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
            job.waitForCompletion(true);
 
    }
}