package driver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pre.mapper.single.FreqMapper;
import pre.reducer.WeightReducer;

import sample.pattern.mapper.FreqSamplingMapper;
import sample.record.mapper.RecordSamplingMapper;
import setting.NAMES;
import setting.PARAMETERS;

public class ChainDriver extends Configured implements Tool
{
	private final static Path temppath = PARAMETERS.localTempPath;

	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <#samples> \n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		String nSamples = args[2];

		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);
		fs.delete(temppath, true);		// clean up temp
		if (fs.exists(output))
		{
			System.out.println("Output directory exists: " + output.toString());
			System.out.print("delete it? (y / n):");
			DataInputStream in = new DataInputStream(System.in);
			char b = in.readChar();
			if (b == 'y')
				fs.delete(output, true);
			else return -1;
		}
		JobControl jc = new JobControl("hello world");
		
		
		// --------------------------- chain it! ---------------------------------
		Job job = new Job(conf, "chain it!");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// mapper
		job.setMapperClass(ChainMapper.class);
		ChainMapper.addMapper(job, FreqMapper.class, 
						LongWritable.class, Text.class, Text.class, Text.class, job.getConfiguration());
		
		// reducer
		job.setReducerClass(ChainReducer.class);
		
		job.getConfiguration().set(NAMES.TOTALWEIGHT.toString(), PARAMETERS.localInputPath.toString());
		ChainReducer.setReducer(job, WeightReducer.class, 
						Text.class, Text.class, Text.class, Text.class, job.getConfiguration());
		
		job.getConfiguration().set(NAMES.NSAMPLES.toString(), nSamples);
		ChainReducer.addMapper(job, RecordSamplingMapper.class, 
						Text.class, Text.class, Text.class, Text.class, job.getConfiguration());
		
		job.getConfiguration().set(NAMES.ORI_FILE_1.toString(), input.toString());
		ChainReducer.addMapper(job, FreqSamplingMapper.class, 
						Text.class, Text.class, Text.class, Text.class, job.getConfiguration());
		
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		
		return exitCode;
	}


	// for testing
	public static void main(String[] args) throws Exception
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input = PARAMETERS.localInputPath;
		Path output = PARAMETERS.localOutputPath;
		String nSamples = "50";

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		int exitCode = ToolRunner.run(conf, new ChainDriver(), 
						new String[] {input.toString(), output.toString(), nSamples});

		System.exit(exitCode);
	}

}
