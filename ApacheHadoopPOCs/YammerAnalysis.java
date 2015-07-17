package com.yammer.analysis;

import com.yammer.mapreduce.YammerMapper;
import com.yammer.mapreduce.YammerReducer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class YammerAnalysis{
	
	public static void main(String... a) throws IOException, InterruptedException, ClassNotFoundException{
	
	// Create a new Job Configuration
        Configuration conf = new Configuration();
        
        // Create Job from configuration
        Job job = Job.getInstance(conf);
        job.setJarByClass(YammerAnalysis.class);
        
        // Specify Job Name
        job.setJobName("Yammer Analysis");
        
        // Set input and output paths for the job
        FileInputFormat.addInputPath(job, new Path(a[0]));
        FileOutputFormat.setOutputPath(job, new Path(a[1]));
        
        // Setting Mapper, Combiner and Reducer for the Job
        job.setMapperClass(YammerMapper.class);
        job.setCombinerClass(YammerReducer.class);
        job.setReducerClass(YammerReducer.class);
        
        // Setting input format and output format of the job
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class); // By default: TextOutputFormat
        
        // We are using TextOutputFormat which emits LongWritable key and Text value by default, but 
        // we are emitting Text as key and IntWritable as value. You need to tell this to the framework.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.waitForCompletion(true);
	}

}
