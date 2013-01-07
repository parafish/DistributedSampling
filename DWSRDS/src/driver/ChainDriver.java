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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pre.mapper.PreMapper;
import pre.mapper.pair.DiscriminitivityMapper;
import pre.mapper.pair.SquaredFreqMapper;
import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;

import sample.pattern.mapper.AbstractPatternMapper;
import sample.pattern.mapper.AreaFreqSamplingMapper;
import sample.pattern.mapper.DiscriminitivitySamplingMapper;
import sample.pattern.mapper.FreqSamplingMapper;
import sample.pattern.mapper.SquaredFreqSamplingMapper;
import sample.record.mapper.RecordSamplingMapper;
import sample.record.reducer.RecordSamplingReducer;
import setting.NAMES;
import setting.PARAMETERS;

public class ChainDriver extends Configured implements Tool
{
	public int run(String[] args) throws Exception
	{
		if (args.length < 4 || args.length > 5)
		{
			System.err.printf(
							"Usage: %s [generic options] <input> [<input2>] <output> <#samples> <distribution>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Path input = null;
		Path input2 = null;
		Path output = null;
		String nSamples = null;
		int dist = 0;

		if (args.length == 4) // algo 1, 2, 4
		{
			input = new Path(args[0]);
			output = new Path(args[1]);
			nSamples = args[2];
			dist = Integer.valueOf(args[3]);
		}
		else		// algo 3
		{
			input = new Path(args[0]);
			input2 = new Path(args[1]);
			output = new Path(args[2]);
			nSamples = args[3];
			dist = Integer.valueOf(args[4]);
		}

		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);

		fs.delete(output, true);
		
		// --------------------------- chain it!
		// ---------------------------------
		
		Job job = new Job(conf);// , "distributed sampling");
		
		job.setJarByClass(getClass());
		
		job.setNumReduceTasks(0);			// must be 1!

		job.getConfiguration().set(NAMES.NSAMPLES.toString(), nSamples);
		job.getConfiguration().set(NAMES.ORI_FILE_1.toString(), input.toString());
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		// chain mapper
		job.setMapperClass(ChainMapper.class);

		// weight mapper
		PreMapper weightMapper = null;
		switch (dist)
		{
		case 1:
			weightMapper = new FreqMapper();
			break;
		case 2:
			weightMapper = new AreaFreqMapper();
			break;
		case 3:
			weightMapper = new DiscriminitivityMapper();
			job.getConfiguration().set(NAMES.ORI_FILE_2.toString(), input2.toString());
			break;
		case 4:
			weightMapper = new SquaredFreqMapper();
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}
		ChainMapper.addMapper(job, weightMapper.getClass(), LongWritable.class, Text.class,
						Text.class, Text.class, job.getConfiguration());

		// no weight reducer
		// sample record mapper
		//ChainMapper.addMapper(job, RecordSamplingMapper.class, Text.class, Text.class, Text.class,
		//				Text.class, job.getConfiguration());

		// reducer
		job.setReducerClass(Reducer.class);
		//job.setReducerClass(ChainReducer.class);

		// only one reducer - sample record reducer
		ChainReducer.setReducer(job, RecordSamplingReducer.class, Text.class, Text.class,
						Text.class, Text.class, job.getConfiguration());

		// pattern sampling mapper
		AbstractPatternMapper patternMapper = null;
		switch (dist)
		{
		case 1:
			patternMapper = new FreqSamplingMapper();
			break;
		case 2:
			patternMapper = new AreaFreqSamplingMapper();
			break;
		case 3:
			patternMapper = new DiscriminitivitySamplingMapper();
			break;
		case 4:
			patternMapper = new SquaredFreqSamplingMapper();
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}

		ChainReducer.addMapper(job, patternMapper.getClass(), Text.class, Text.class, Text.class,
						Text.class, job.getConfiguration());

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		return exitCode;
	}


	// for testing
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ChainDriver(), args);

		System.exit(exitCode);
	}

}
