package com.hareesh;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthReducer implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

    
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		
		int voteCount = 0;
		
		while (values.hasNext()){
			IntWritable val = values.next();
			voteCount += val.get();
		}
        /*
		for(IntWritable value: values){
            voteCount+= value.get();
        }*/
        
		output.collect(key, new IntWritable(voteCount));
	}
}

/*
public class MonthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
        int voteCount = 0;
        for(IntWritable value: values){
            voteCount+= value.get();
        }
        output.write(key, new IntWritable(voteCount));
    }
}*/