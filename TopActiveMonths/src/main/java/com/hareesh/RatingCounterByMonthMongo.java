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

public class RatingCounterByMonthMongo  extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.out.println("usage: [input] [mongo-uri]");
            System.exit(-1);
        }
        
       /* RatingCounterByMonthMongo rm = new RatingCounterByMonthMongo();
        
        final Configuration conf = rm.getConf();
        conf.set("mongo.output.uri", args[1]);
        
        Job job = Job.getInstance(conf);
        job.setMapperClass(MonthMapper.class);
        job.setReducerClass(MonthReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BSONWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJarByClass(RatingCounterByMonth.class);

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
*/        
        int res = ToolRunner.run(new RatingCounterByMonthMongo(), args);
        System.exit(res);
    }
	
	public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("mongo.output.uri", args[1]);

        final JobConf job = new JobConf(conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setJarByClass(RatingCounterByMonthMongo.class);

        job.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        job.setMapperClass(MonthMapper.class);
        job.setReducerClass(MonthReducer.class);
        job.setOutputFormat(MongoOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BSONWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        JobClient.runJob(job);

        return 0;
    }
 
}
