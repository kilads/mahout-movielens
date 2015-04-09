package com.hareesh;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;

public class MovieToMongo  extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.out.println("usage: [input] [mongo-uri]");
            System.exit(-1);
        }        
        
        int res = ToolRunner.run(new MovieToMongo(), args);
        System.exit(res);
    }
	
	public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("mongo.output.uri", args[1]);

        final JobConf job = new JobConf(conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setJarByClass(MovieToMongo.class);

        job.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        
        job.setMapperClass(MonthMapper.class);
                
        job.setOutputFormat(MongoOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BSONWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        JobClient.runJob(job);

        return 0;
    }
 
}
