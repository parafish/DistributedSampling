package test.sampler;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Ignore;
import org.junit.Test;

import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;
import pre.reducer.WeightReducer;

import sample.pattern.mapper.AreaFreqSamplingMapper;
import sample.pattern.mapper.DiscriminitivitySamplingMapper;
import sample.pattern.mapper.FreqSamplingMapper;
import sample.record.mapper.RecordSamplingMapper;
import setting.NAMES;
import setting.PARAMETERS;

public class SamplerTest
{

	// test record sampling
	@Ignore
	@Test
	public void testRecord() throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input = new Path("/home/zheyi/sampling/temp/RECORD");
		Path output = new Path("/home/zheyi/sampling/tempdisc");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		// set up job
		Job job = new Job(conf, "record sampler");
		job.getConfiguration().set(NAMES.NSAMPLES.toString(), "30");		// must specify
		job.getConfiguration().set(NAMES.TOTALWEIGHT.toString(), "32");	// must specify

		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(RecordSamplingMapper.class);

		// specify the key-value input
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// run job
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
	}


	@Test
	public void testPattern() throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input = new Path("/home/zheyi/sampling/tempdisc");
		Path output = new Path("/home/zheyi/sampling/output");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		// set up job
		Job job = new Job(conf, "pattern sampler");
		job.getConfiguration().set(NAMES.ORI_FILE_1.toString(), "/home/zheyi/sampling/data/disc/positive.dat");
		job.getConfiguration().set(NAMES.ORI_FILE_2.toString(), "/home/zheyi/sampling/data/disc/negative.dat");

		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(DiscriminitivitySamplingMapper.class);

		// specify the key-value input
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// run job
		int exitCode = job.waitForCompletion(true) ? 0 : 1;

	}

}
