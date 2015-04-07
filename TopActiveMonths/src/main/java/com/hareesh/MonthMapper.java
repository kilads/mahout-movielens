package com.hareesh;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MonthMapper implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private String SEPARATOR = "::";
   
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		
		 //If more than one word is present, split using white space.
        String[] words = value.toString().split(SEPARATOR);
        
        String ratingTimeEpoch = words[3];
        
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(Long.parseLong(ratingTimeEpoch) * 1000);
        		
        StringBuffer month = new StringBuffer();
        
        month.append(new SimpleDateFormat("MMMM").format(cal.getTime()));
        
        //Only the first word is the candidate name
        output.collect(new Text(month.toString()), one);
		
	}
}