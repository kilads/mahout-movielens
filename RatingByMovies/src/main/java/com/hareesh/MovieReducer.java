package com.hareesh;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

/*
public class MovieReducer implements org.apache.hadoop.mapred.Reducer<Text,Text,Text,IntWritable>
{
	
	Text valTitle = new Text();
	Text valEmit = new Text();
	String merge;

	public void configure(JobConf arg0) {
		
	}

	public void close() throws IOException {
		
	}

	public void reduce(Text k, Iterator<Text> values,
			OutputCollector<Text, IntWritable> context, Reporter arg3)
			throws IOException {
		
		int counter = 0;
		merge = "";
		
		while(values.hasNext()){
			Text value = values.next();
			
			if (value.toString().startsWith("#")){ //from rating
				counter++;
			}
			
			else if ("".equalsIgnoreCase(merge)){// from movies get the title
				merge = value.toString();
			}
		}

		valTitle.set(merge);
		
		context.collect(valTitle, new IntWritable(counter));
		
	}
	
}*/


public class MovieReducer extends Reducer<Text,Text,Text,IntWritable>
{
	Text valTitle = new Text();
	Text valEmit = new Text();
	String merge;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException , InterruptedException{
		System.out.println("reduce begin");

		int counter = 0;
		merge = "";
		
		for(Text value:values){
			
			if (value.toString().startsWith("#")){ //from rating
				counter++;
			}
			
			else if ("".equalsIgnoreCase(merge)){// from movies get the title
				merge = value.toString();
			}
						
		}

		valTitle.set(merge);
		System.out.println("reduce pre emit");

		context.write(valTitle, new IntWritable(counter));
	}
}