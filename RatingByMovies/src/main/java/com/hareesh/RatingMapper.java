package com.hareesh;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;


public  class RatingMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	Text keyEmit = new Text();
	Text valEmit = new Text();

	public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException{
		String line=v.toString();
		String[] words=line.split("::");
		keyEmit.set(words[1]);
		valEmit.set("#");

		context.write(keyEmit, valEmit);
	}
}
