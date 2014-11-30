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
public class AvroAverageDriver extends Configured implements Tool{

	@Override
	public int run(String[] rawArgs) throws Exception {
		if (rawArgs.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getName());
	          //Input/student.avro Output
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = new Job(super.getConf());
		job.setJarByClass(AvroAverageDriver.class);
		job.setJobName("Avro Average");
		
		String[] args = new GenericOptionsParser(rawArgs).getRemainingArgs();
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);

		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(super.getConf()).delete(outPath, true);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(AvroAverageMapper.class);
		AvroJob.setInputKeySchema(job, student_marks.getClassSchema());
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPair.class);
		
		job.setCombinerClass(AvroAverageCombiner.class);
		
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(AvroAverageReducer.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.INT));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.FLOAT));
		
		  /* 
		   * Snappy conversion --- requires maven plugin 
		   * 
		   * <dependency>
      <groupId>org.xerial.snappy</groupId>
      <artifactId>snappy-java</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.commons</groupId>
      <artifactId>commons-compress</artifactId>
    </dependency>
    
		   * FileOutputFormat.setCompressOutput(job, true);
		    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		    
		    */

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new AvroAverageDriver(), args);
		System.exit(result);
	}
}