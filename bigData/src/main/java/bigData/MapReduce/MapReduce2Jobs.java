package bigData.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import bigData.MapReduce.FirstMapReduce.FirstMapper;
import bigData.MapReduce.FirstMapReduce.FirstReducer;
import bigData.MapReduce.XMLInputFormat;
import bigData.MapReduce.SecondMapReduce;
import bigData.MapReduce.SecondMapReduce.SecondMapper;
import bigData.MapReduce.SecondMapReduce.SecondReducer;

public class MapReduce2Jobs implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapReduce2Jobs(), args);
		System.exit(exitCode);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public int run(String[] args) throws Exception {
		JobControl jobControl = new JobControl("jobChain");

		Configuration conf1 = new Configuration();
		conf1.set("START_TAG_KEY", "<page>");
		conf1.set("END_TAG_KEY", "</page>");
		conf1.set(TextOutputFormat.SEPERATOR, ";");

		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(MapReduce2Jobs.class);
		job1.setReducerClass(FirstReducer.class);
		job1.setMapperClass(FirstMapper.class);

		job1.setNumReduceTasks(1);

		job1.setInputFormatClass(XMLInputFormat.class);
		job1.setOutputValueClass(TextOutputFormat.class);

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/primoJob"));

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		jobControl.addJob(controlledJob1);

		Configuration conf2 = new Configuration();
		conf2.set(TextOutputFormat.SEPERATOR, ";");

		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(SecondMapReduce.class);

		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/primoJob"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/secondoJob"));

		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);

		// make job2 dependent on job1
		controlledJob2.addDependingJob(controlledJob1);
		// add the job to the job control
		jobControl.addJob(controlledJob2);
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
			System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
			System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
			System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
			try {
				Thread.sleep(5000);
			} catch (Exception e) {

			}

		}
		System.exit(0);
		return (job1.waitForCompletion(true) ? 0 : 1);
	}
}