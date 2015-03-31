package com.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Reco {

	
	
	public static void main(String[] args) throws Exception {
	
		MongoClient mongo = new MongoClient("localhost", 27017);
		DB db = mongo.getDB("mydb");
		
		 DBCollection coll = db.getCollection("cobaid");
		 
		 BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
		 builder.insert(new BasicDBObject("_id", 1));
		 builder.insert(new BasicDBObject("_id", 2));
		 builder.insert(new BasicDBObject("_id", 3));
		 
		 BulkWriteResult result = builder.execute();
		 
		 result.getInsertedCount();
	}
}
