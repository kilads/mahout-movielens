package com.hareesh;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

/*
public class MovieMapper implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text>{

	Text keyEmit = new Text();
	Text valEmit = new Text();
	
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void map(LongWritable k, Text value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		String line=value.toString();
		String[] words=line.split("::");
		keyEmit.set(words[0]);
		valEmit.set(words[1]);
		context.collect(keyEmit, valEmit);
	}
	
	
}*/


public class MovieMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	Text keyEmit = new Text();
	Text valEmit = new Text();
	
	public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException{
			String line=value.toString();
			String[] words=line.split("::");
			keyEmit.set(words[0]);
			valEmit.set(words[1]);
			System.out.println("map pre emit");

			context.write(keyEmit, valEmit);
	}
}
