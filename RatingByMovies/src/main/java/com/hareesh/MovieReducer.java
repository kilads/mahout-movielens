package com.hareesh;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<Text,Text,Text,IntWritable>
{
	Text valTitle = new Text();
	Text valEmit = new Text();
	String merge;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException , InterruptedException{
		
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
		
		context.write(valTitle, new IntWritable(counter));
	}
}