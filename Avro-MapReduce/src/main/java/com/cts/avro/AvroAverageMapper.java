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
public class AvroAverageMapper extends 
    Mapper<AvroKey<student_marks>, NullWritable, IntWritable, IntPair> {
	protected void map(AvroKey<student_marks> key, NullWritable value, Context context) 
			throws IOException, InterruptedException {
		IntWritable s_id = new IntWritable(key.datum().getStudentId());
		IntPair marks_one = new IntPair(key.datum().getMarks(), 1);
		context.write(s_id, marks_one);
	}
} 

