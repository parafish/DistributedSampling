package edu.tue.cs.capa.dps.disc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import edu.tue.cs.capa.dps.util.DpsExceptions.NonFixedLineLengthException;
import edu.tue.cs.capa.dps.util.Helper.DecreasingDoubleWritableComparator;



public class DiscDriver extends Configured implements Tool
{
	public DiscDriver()
	{
	}


	@Override
	public int run(String[] args) throws IOException
	{
		if (args.length < 4)
		{
			System.out.println("cmd <inPosDir> <inNegDir> <output> <samples>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path leftInputPath = new Path(args[0]);
		Path rightInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		int nSamples = Integer.parseInt(args[3]);

		JobConf jobConf = new JobConf(getConf(), getClass());
		jobConf.set(Config.RIGHT_PATH, rightInputPath.toString());
		jobConf.set(Config.N_SAMPLES, String.valueOf(nSamples));

		// detect the line length of the right dataset
		// fetch the line length
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] stats = fs.listStatus(rightInputPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().startsWith("part-");
			}
		});
		
		FSDataInputStream fsInputStream = fs.open(stats[0].getPath());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsInputStream));
		int lineLength = reader.readLine().length();
		if (lineLength !=  reader.readLine().length())	// check if the line lengths are the same
			throw new NonFixedLineLengthException("Line length is not fixed");
		reader.close();
		jobConf.setInt(Config.RIGHT_LINE_LENGTH, lineLength + 1);
		System.out.println("Detected right line length (without \\n): " + lineLength);

		FileInputFormat.addInputPath(jobConf, leftInputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setMapperClass(DiscMapper.class);

		jobConf.setOutputKeyComparatorClass(DecreasingDoubleWritableComparator.class);

		jobConf.setReducerClass(DiscReducer.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(DoubleWritable.class);
		jobConf.setOutputValueClass(Text.class);

		// print and run
		if (jobConf.getJobName() == "")
			jobConf.setJobName("DiscriminativityPatternSampling");
		System.out.println("DistributedPatternSampling (" + jobConf.getJobName() + ")");
		System.out.println("\tInput paths: ");
		for (Path input : FileInputFormat.getInputPaths(jobConf))
			System.out.println("\t\t\t" + input.toString());		
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(jobConf));
		System.out.println("\tSample:\t" + jobConf.getInt(Config.N_SAMPLES, 0));
		System.out.println("\tConfigurations: ");
		System.out.println("\t\t\tDelimiter: \'" + jobConf.get(Config.ITEM_DELIMITER, Config.DEFAULT_ITEM_DELIMITER) + "\'");
		System.out.println("\t\t\tRight dataset path: " + jobConf.get(Config.RIGHT_PATH));
		System.out.println("\t\t\tLength of lines in the right dataset (with \\n): " + jobConf.getInt(Config.RIGHT_LINE_LENGTH, 0));;
		System.out.println("\t\t\tMaiximum record length: "
						+ jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH));
		System.out.println("\t\t\tMinimum Pattern length: "
						+ jobConf.getInt(Config.MIN_PATTERN_LENGTH, Config.DEFAULT_MIN_PATTERN_LENGTH));
		JobClient.runJob(jobConf);

		return 0;
	}


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args)
	{
		int exitCode;
		try
		{
			exitCode = ToolRunner.run(new DiscDriver(), args);
		}
		catch (Exception e)
		{
			exitCode = -1;
			e.printStackTrace();
		}
		
		System.exit(exitCode);
	}
}