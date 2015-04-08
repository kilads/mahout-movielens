package com.hareesh;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

/*
public  class RatingMapper implements org.apache.hadoop.mapred.Mapper<LongWritable,Text,Text,Text>{
	Text keyEmit = new Text();
	Text valEmit = new Text();
	
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	public void map(LongWritable k, Text v,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		
		String line=v.toString();
		String[] words=line.split("::");
		keyEmit.set(words[1]);
		valEmit.set("#");
		context.collect(keyEmit, valEmit);
	}
	
}
	*/



public  class RatingMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	Text keyEmit = new Text();
	Text valEmit = new Text();

	public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException{
		String line=v.toString();
		String[] words=line.split("::");
		keyEmit.set(words[1]);
		valEmit.set("#");
		System.out.println("map rating pre emit");

		context.write(keyEmit, valEmit);
	}
}
