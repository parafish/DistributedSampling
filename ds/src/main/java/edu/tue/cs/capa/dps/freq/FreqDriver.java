package edu.tue.cs.capa.dps.freq;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.tue.cs.capa.dps.util.Config;
import edu.tue.cs.capa.dps.util.Helper.DecreasingDoubleWritableComparator;



public class FreqDriver extends Configured implements Tool
{
	private FreqDriver()
	{

	}


	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length < 3)
		{
			System.out.println("freq <input> <output> <samples>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		int nSamples = Integer.parseInt(args[2]);

		JobConf jobConf = new JobConf(getConf(), getClass());
		jobConf.set(Config.N_SAMPLES, String.valueOf(nSamples));

		
		FileInputFormat.addInputPath(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setMapperClass(FreqMapper.class);
		jobConf.setMapOutputKeyClass(DoubleWritable.class);
		jobConf.setMapOutputValueClass(Text.class);

		jobConf.setOutputKeyComparatorClass(DecreasingDoubleWritableComparator.class);

		jobConf.setNumReduceTasks(1);
		jobConf.setReducerClass(FreqReducer.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(DoubleWritable.class);
		jobConf.setOutputValueClass(Text.class);

		// print and run
		if (jobConf.getJobName() == "")
			jobConf.setJobName("FrequentPatternSampling");
		System.out.println("DistributedPatternSampling (" + jobConf.getJobName() + ")");
		
		System.out.println("\tInput paths: ");
		Path[] inputs = FileInputFormat.getInputPaths(jobConf);
		for (int ctr = 0; ctr < inputs.length; ctr++)
			System.out.println("\t\t\t" + inputs[ctr].toString());
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(jobConf));
		
		System.out.println("\tSample:\t" + jobConf.getInt(Config.N_SAMPLES, 0));
		System.out.println("\tConfigurations: ");
		System.out.println("\t\t\tDelimiter: \'" 
						+ jobConf.get(Config.ITEM_DELIMITER, Config.DEFAULT_ITEM_DELIMITER) + "\'");
		System.out.println("\t\t\tMaiximum record length: "
						+ jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH));
		System.out.println("\t\t\tMinimum Pattern length: "
						+ jobConf.getInt(Config.MIN_PATTERN_LENGTH, Config.DEFAULT_MIN_PATTERN_LENGTH));

		JobClient.runJob(jobConf);
		return 0;
	}


	public static void main(String[] args)
	{
		int exitCode;
		try
		{
			exitCode = ToolRunner.run(new FreqDriver(), args);
		}
		catch (Exception e)
		{
			exitCode = -1;
			e.printStackTrace();
		}
		System.exit(exitCode);

	}
}
