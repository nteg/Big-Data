package com.yammer.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

import java.io.IOException;

import java.util.StringTokenizer;

public class YammerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private Text text = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
	        String word = itr.nextToken();
		    if("\"sender_id\":".equals(word)){
    			String idOfSender = itr.nextToken();
			    int len = idOfSender.length();
			    text.set(idOfSender.substring(0,len-1));
        		context.write(text, one);
		    }
        }
          
    }
}
