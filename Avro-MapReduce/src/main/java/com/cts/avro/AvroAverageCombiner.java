package com.cts.avro;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cts.avro.IntPair;
import com.cts.avro.student_marks;
public class AvroAverageCombiner extends 
Reducer<IntWritable, IntPair, IntWritable, IntPair> {
	IntPair p_sum_count = new IntPair();
	Integer p_sum = new Integer(0);
	Integer p_count = new Integer(0);
	protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) 
			throws IOException, InterruptedException {
		p_sum = 0;
		p_count = 0;
		for (IntPair value : values) {
			p_sum += value.getFirstInt();
			p_count += value.getSecondInt();
		}
		p_sum_count.set(new IntWritable(p_sum), new IntWritable(p_count));
		context.write(key, p_sum_count);
	}
} // end of combiner class 

