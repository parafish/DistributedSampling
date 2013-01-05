package driver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.thoughtworks.paranamer.DefaultParanamer;

import preprocess.WeightReducer;
import preprocess.pairweighter.DiscriminitivityPairMapper;
import preprocess.pairweighter.SquaredFreqPairMapper;
import preprocess.singleweighter.AreaFreqSingleMapper;
import preprocess.singleweighter.FreqSingleMapper;

import sample.pattern.AreaFreqPatternSamplingMapper;
import sample.pattern.DiscriminitivityPatternSamplingMapper;
import sample.pattern.FreqPatternSamplingMapper;
import sample.pattern.SquaredFreqPatternSamplingMapper;
import sample.record.RecordSamplingMapper;
import setting.NAMES;
import setting.PARAMETERS;

public class LocalSamplingDriver extends Configured implements Tool
{
	private final static Path temppath = PARAMETERS.localTempPath;

	public int run(String[] args) throws Exception
	{
		if (args.length < 4 || args.length > 5)
		{
			System.err.printf("Usage: %s [generic options] <input> [<input2>] <output> <#samples> <distribution>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		

		Path input = null;
		Path input2 = null;
		Path output = null;
		String nSamples = null;
		int dist = 0;
		
		if (args.length == 4)		// algo 1, 2, 4
		{
			input = new Path(args[0]);
			output = new Path(args[1]);
			nSamples = args[2];
			dist = Integer.valueOf(args[3]);
		}
		else						// algo 3
		{
			input = new Path(args[0]);
			input2 = new Path(args[1]);
			output = new Path(args[2]);
			nSamples = args[3];
			dist = Integer.valueOf(args[4]);			
		}

		Configuration conf = getConf();

		FileSystem fs = FileSystem.get(conf);
		fs.delete(temppath, true);		// clean up temp
		
		// ---------------------------------phase 1 ----------------------------
		Job jobPhase1 = new Job(conf, "weighter");
		jobPhase1.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(jobPhase1, input);
		FileOutputFormat.setOutputPath(jobPhase1, temppath);	// output to temp direc

		switch (dist)
		{
		case 1:
			jobPhase1.setMapperClass(FreqSingleMapper.class);
			break;
		case 2:
			jobPhase1.setMapperClass(AreaFreqSingleMapper.class);
			break;
		case 3:
			jobPhase1.getConfiguration().set(NAMES.ORI_FILE_2.toString(), input2.toString());
			jobPhase1.setMapperClass(DiscriminitivityPairMapper.class);
			break;
		case 4:
			jobPhase1.setMapperClass(SquaredFreqPairMapper.class);
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}
		
		jobPhase1.setReducerClass(WeightReducer.class);

		jobPhase1.setOutputKeyClass(Text.class);
		jobPhase1.setOutputValueClass(Text.class);

		// run job
		int jobResult1= jobPhase1.waitForCompletion(true) ? 0 : 1;
		
		if (jobResult1 != 0)
			return -1;
		
		// -------------------------------- phase 2 --------------------------------
		// do some file splitting here
		FileStatus[] status = fs.listStatus(new Path(temppath.toString() + "/" + NAMES.TOTALWEIGHT.toString()));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
		String totalweight = reader.readLine().split("\t")[0];
		reader.close();

		// configure job 2 (sample records
		Job jobPhase2 = new Job(getConf(), "sampler");
		jobPhase2.setJarByClass(getClass());
		
		jobPhase2.getConfiguration().set(NAMES.TOTALWEIGHT.toString(), totalweight);
		jobPhase2.getConfiguration().set(NAMES.NSAMPLES.toString(), nSamples);

		FileInputFormat.addInputPath(jobPhase2, new Path(temppath.toString() + "/" + NAMES.RECORD.toString()));
		FileOutputFormat.setOutputPath(jobPhase2, output);

		// specify the key-value input
		jobPhase2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		jobPhase2.setMapperClass(ChainMapper.class);
		
		ChainMapper.addMapper(jobPhase2, RecordSamplingMapper.class, 
						Text.class, Text.class, Text.class, Text.class, jobPhase2.getConfiguration());
		
		jobPhase2.getConfiguration().set(NAMES.ORI_FILE_1.toString(), input.toString());
		switch (dist)
		{
		case 1:
			ChainMapper.addMapper(jobPhase2, FreqPatternSamplingMapper.class, 
							Text.class, Text.class, Text.class, NullWritable.class, jobPhase2.getConfiguration());
			break;
		case 2:
			ChainMapper.addMapper(jobPhase2, AreaFreqPatternSamplingMapper.class, 
							Text.class, Text.class, Text.class, NullWritable.class, jobPhase2.getConfiguration());
			break;
		case 3:
			jobPhase2.getConfiguration().set(NAMES.ORI_FILE_2.toString(), input2.toString());
			ChainMapper.addMapper(jobPhase2, DiscriminitivityPatternSamplingMapper.class, 
							Text.class, Text.class, Text.class, NullWritable.class, jobPhase2.getConfiguration());
			break;
		case 4:
			ChainMapper.addMapper(jobPhase2, SquaredFreqPatternSamplingMapper.class, 
							Text.class, Text.class, Text.class, NullWritable.class, jobPhase2.getConfiguration());
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}
		
		int jobResult2 = jobPhase2.waitForCompletion(true) ? 0 : 1;
		
		return jobResult2;
	}

	// for testing
	public static void main(String[] args) throws Exception
	{
		Configuration conf = PARAMETERS.getLocalConf();

		Path input = PARAMETERS.localInputPath;
		Path input2 = PARAMETERS.locaInputPath2;
		Path output = PARAMETERS.localOutputPath;
		String nSamples = "200";
		String dist = "3";

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		int exitCode = ToolRunner.run(conf, new LocalSamplingDriver(), 
						new String[] {input.toString(), input2.toString(), output.toString(), nSamples, dist});

		System.exit(exitCode);
	}

}
