package com.hareesh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;

public class RatingCounterByMovieMongo extends Configured implements Tool{

	
	public int run(String[] args) throws Exception {
		
		final Configuration conf = getConf();
        conf.set("mongo.output.uri", args[2]);
		
		Path p1=new Path(args[0]);
		Path p2=new Path(args[1]);
		
		Job job = new Job(conf,"Multiple Job");
		job.setJarByClass(RatingCounterByMovieMongo.class);
		
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MovieMapper.class);
		
		job.setReducerClass(MovieReducer.class);
		
		/*job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path p3=new Path("output");
		
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}*/
		
		job.setOutputFormatClass(com.mongodb.hadoop.MongoOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		/*
		
		FileOutputFormat.setOutputPath(job, p3);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
*/
		boolean success = job.waitForCompletion(true);
        	
		if(success)
			System.out.println("test");
		else
			System.out.println("failed");
		
		return success?0:1;
		
		
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
			System.exit(0);
		}
		int res = ToolRunner.run(new Configuration(), new RatingCounterByMovieMongo(), args);
		System.exit(res);

	}
}

/*
public class RatingCounterByMovieMongo extends Configured implements Tool{

	public static void main(String[] args) throws Exception
	{
		if (args.length != 3 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
			System.exit(0);
		}
		int res = ToolRunner.run(new Configuration(), new RatingCounterByMovieMongo(), args);
		System.exit(res);

	}
	
	public int run(String[] args) throws Exception {
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		
		Path p1=new Path(files[0]);
		Path p2=new Path(files[1]);
		Path p3=new Path(files[2]);
		
		FileSystem fs = FileSystem.get(c);
		
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		
		Job job = new Job(c,"Multiple Job");
		job.setJarByClass(RatingCounterByMovieMongo.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, p3);
		boolean success = job.waitForCompletion(true);
		return success?0:1;
	}
}*/
