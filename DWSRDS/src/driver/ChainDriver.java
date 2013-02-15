package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pre.mapper.PreMapper;
import pre.mapper.pair.DiscriminitivityMapper;
import pre.mapper.pair.SquaredFreqMapper;
import pre.mapper.pair.crossproduct.CartesianProduct.CartesianInputFormat;
import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;
import sample.pattern.mapper.AbstractPatternMapper;
import sample.pattern.mapper.AreaFreqSamplingMapper;
import sample.pattern.mapper.DiscriminitivitySamplingMapper;
import sample.pattern.mapper.FreqSamplingMapper;
import sample.pattern.mapper.SquaredFreqSamplingMapper;
import sample.record.mapper.RecordSamplingMapper;
import sample.record.reducer.RecordSamplingReducer;
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
		else
		// algo 3
		{
			input = new Path(args[0]);
			input2 = new Path(args[1]);
			output = new Path(args[2]);
			nSamples = args[3];
			dist = Integer.valueOf(args[4]);
		}

		Configuration conf = getConf();

		// delete the output
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(output, true);

		// ----------------- chain it! ----------------------------

		Job job = new Job(conf);// , "distributed sampling");

		job.setJarByClass(getClass());

		job.getConfiguration().set(PARAMETERS.MAX_LENGTH, "1000");
		job.getConfiguration().set(PARAMETERS.N_SAMPLES, nSamples);
		job.getConfiguration().set(PARAMETERS.LEFT_PATH, input.toString());

		FileOutputFormat.setOutputPath(job, output);

		// prepare mappers
		// weight mapper
		PreMapper weightMapper = null;
		switch (dist)
		{
		case 1:
			weightMapper = new FreqMapper();
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, input);
			break;
		case 2:
			weightMapper = new AreaFreqMapper();
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, input);
			break;
		case 3:
			weightMapper = new DiscriminitivityMapper();
			job.getConfiguration().set(PARAMETERS.RIGHT_PATH, input2.toString());
			job.setInputFormatClass(CartesianInputFormat.class);
			CartesianInputFormat.setLeftInputInfo(job, TextInputFormat.class, input.toString());
			CartesianInputFormat.setRightInputInfo(job, TextInputFormat.class, input2.toString());	
			break;
		case 4:
			weightMapper = new SquaredFreqMapper();
			job.setInputFormatClass(CartesianInputFormat.class);
			CartesianInputFormat.setLeftInputInfo(job, TextInputFormat.class, input.toString());
			CartesianInputFormat.setRightInputInfo(job, TextInputFormat.class, input.toString());	
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}
		
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


		// set MAP+ REDUCE MAP*
		// chain mapper
		job.setMapperClass(ChainMapper.class);
		
		// map to index-weight
		ChainMapper.addMapper(job, weightMapper.getClass(), Object.class, Text.class,
						NullWritable.class, Text.class, job.getConfiguration());
		
		// map to sampled index-weight'
		ChainMapper.addMapper(job, RecordSamplingMapper.class, NullWritable.class, Text.class, NullWritable.class,
						Text.class, job.getConfiguration());
		
		// reducer
		job.setNumReduceTasks(1);
		job.setReducerClass(ChainReducer.class);

		// reduce samples from mappers to one sample (size=N_SAMPLES)
		ChainReducer.setReducer(job, RecordSamplingReducer.class, NullWritable.class, Text.class,
						NullWritable.class, Text.class, job.getConfiguration());

		// sample patterns from specific index
		ChainReducer.addMapper(job, patternMapper.getClass(), NullWritable.class, Text.class, NullWritable.class,
						Text.class, job.getConfiguration());

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		return exitCode;
	}

	// for real running !
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ChainDriver(), args);

		System.exit(exitCode);
	}

}
