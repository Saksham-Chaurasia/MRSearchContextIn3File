package com.sc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SCDriver {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("context", args[2]);
		Job job = new Job(conf, "SearchContext");

	

		
		job.setJarByClass(SCDriver.class);

		job.setMapperClass(SCMapper.class);
		job.setReducerClass(SCReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path (args[0]));
		
		for(FileStatus f: files){
			FileInputFormat.addInputPath(job, f.getPath());
		}
			
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
