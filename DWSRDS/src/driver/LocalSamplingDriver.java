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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import preprocess.WeightReducer;
import preprocess.singleweighter.FreqSingleMapper;

import sample.pattern.FreqPatternSamplingMapper;
import sample.record.RecordSamplingMapper;
import setting.NAMES;
import setting.PARAMETERS;

public class LocalSamplingDriver extends Configured implements Tool
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
		
		// ---------------------------------phase 1 ----------------------------
		Job jobPhase1 = new Job(conf, "weighter");
		jobPhase1.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(jobPhase1, input);
		FileOutputFormat.setOutputPath(jobPhase1, temppath);	// output to temp direc

		jobPhase1.setMapperClass(FreqSingleMapper.class);	
		jobPhase1.setReducerClass(WeightReducer.class);

		jobPhase1.setOutputKeyClass(Text.class);
		jobPhase1.setOutputValueClass(Text.class);

		// run job
		int jobResult1= jobPhase1.waitForCompletion(true) ? 0 : 1;
		
		if (jobResult1 != 0)
			return -1;
		
		// -------------------------------- phase 2 --------------------------------
		// do some file splitting here
		FileStatus[] status = fs.listStatus(
						new Path(temppath.toString() + "/" + NAMES.TOTALWEIGHT.toString()));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
		String totalweight = reader.readLine().split("\t")[0];
		reader.close();

		// configure job 2 (sample records
		Job jobPhase2 = new Job(getConf(), "record sampler");
		jobPhase2.setJarByClass(getClass());
		
		jobPhase2.getConfiguration().set(NAMES.TOTALWEIGHT.toString(), totalweight);
		jobPhase2.getConfiguration().set(NAMES.NSAMPLES.toString(), nSamples);

		FileInputFormat.addInputPath(jobPhase2, 
						new Path(temppath.toString() + "/" + NAMES.RECORD.toString()));
		FileOutputFormat.setOutputPath(jobPhase2, 
						new Path(temppath.toString() + "/" + NAMES.SAMPLED_INDEX.toString()));

		jobPhase2.setMapperClass(RecordSamplingMapper.class);

		// specify the key-value input
		jobPhase2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		jobPhase2.setOutputKeyClass(Text.class);
		jobPhase2.setOutputValueClass(IntWritable.class);
		
		int jobResult2 = jobPhase2.waitForCompletion(true) ? 0 : 1;
		
		if (jobResult2 != 0)
			return -1;
		
		// -------------------------------- phase 3 --------------------------------
		Job jobPhase3 = new Job(getConf(), "pattern sampler");
		jobPhase3.setJarByClass(getClass());
		
		jobPhase3.getConfiguration().set(NAMES.ORI_FILE_1.toString(), input.toString());

		FileInputFormat.addInputPath(jobPhase3, new Path(temppath.toString() + "/" + NAMES.SAMPLED_INDEX.toString()));
		FileOutputFormat.setOutputPath(jobPhase3, output);

		jobPhase3.setMapperClass(FreqPatternSamplingMapper.class);

		// specify the key-value input
		jobPhase3.setInputFormatClass(KeyValueTextInputFormat.class);
		
		jobPhase3.setOutputKeyClass(Text.class);
		jobPhase3.setOutputValueClass(NullWritable.class);

		// run job
		int exitCode = jobPhase3.waitForCompletion(true) ? 0 : 1;
		
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

		int exitCode = ToolRunner.run(conf, new LocalSamplingDriver(), 
						new String[] {input.toString(), output.toString(), nSamples});

		System.exit(exitCode);
	}

}
