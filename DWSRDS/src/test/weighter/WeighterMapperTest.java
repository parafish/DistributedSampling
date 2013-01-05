package test.weighter;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Ignore;
import org.junit.Test;

import pre.mapper.pair.DiscriminitivityMapper;
import pre.mapper.pair.SquaredFreqMapper;
import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;
import pre.reducer.WeightReducer;

import setting.NAMES;
import setting.PARAMETERS;

public class WeighterMapperTest
{

	// for testing single file input
	@Ignore
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input = new Path("/home/zheyi/sampling/data/iris.dat");
		Path output = new Path("/home/zheyi/sampling/intermediate");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		// set up job
		Job job = new Job(conf, "calc total weight");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(FreqMapper.class);	
		job.setReducerClass(WeightReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// run job
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
	}


	
	@Test
	public void testDiscriminitivity() throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input1 = new Path("/home/zheyi/sampling/data/disc/positive.dat");
		Path input2 = new Path("/home/zheyi/sampling/data/disc/negative.dat");
		Path output = new Path("/home/zheyi/sampling/temp");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		// set up job
		Job job = new Job(conf, "calc total weight");
		job.getConfiguration().set(NAMES.ORI_FILE_2.toString(), input2.toString());
		
		
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, input1);
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(DiscriminitivityMapper.class);
		job.setReducerClass(WeightReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// run job
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
	}

}
