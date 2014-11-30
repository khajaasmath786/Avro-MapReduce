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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cts.avro.IntPair;
import com.cts.avro.student_marks;
public class AvroAverageReducer extends 
	Reducer<IntWritable, IntPair, AvroKey<Integer>, AvroValue<Float>> {
		Integer f_sum = 0;
		Integer f_count = 0;
		
		protected void reduce(IntWritable key, Iterable<IntPair> values, Context context) 
				throws IOException, InterruptedException {
			f_sum = 0;
			f_count = 0;
			for (IntPair value : values) {
				f_sum += value.getFirstInt();
				f_count += value.getSecondInt();
			}
			Float average = (float)f_sum/f_count;
			Integer s_id = new Integer(key.toString());
			context.write(new AvroKey<Integer>(s_id), new AvroValue<Float>(average));
		}
	} // end of reducer class 

