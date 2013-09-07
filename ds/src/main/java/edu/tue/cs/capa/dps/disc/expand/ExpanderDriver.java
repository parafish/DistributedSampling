package edu.tue.cs.capa.dps.disc.expand;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.tue.cs.capa.dps.disc.DiscDriver;
import edu.tue.cs.capa.dps.util.Config;

public class ExpanderDriver extends Configured implements Tool
{
	Path tempDir = new Path("temp");


	public ExpanderDriver()
	{
		// for call in other classes
	}


	@Override
	public int run(String[] args) throws IOException
	{
		if (args.length < 2)
		{
			System.out.println("expand <input> <output>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Path lineLengthPath = new Path(tempDir.toString() + "/longest-line-length");

		// find the longest line length
		JobConf longestLineLengthConf = new JobConf(getConf(), getClass());

		FileInputFormat.addInputPath(longestLineLengthConf, inputPath);
		longestLineLengthConf.setInputFormat(TextInputFormat.class);

		longestLineLengthConf.setMapperClass(LongestLineLengthMapper.class);
		longestLineLengthConf.setCombinerClass(LongestLineLengthReducer.class);
		longestLineLengthConf.setReducerClass(LongestLineLengthReducer.class);

		FileOutputFormat.setOutputPath(longestLineLengthConf, lineLengthPath);
		longestLineLengthConf.setOutputFormat(TextOutputFormat.class);
		longestLineLengthConf.setOutputKeyClass(NullWritable.class);
		longestLineLengthConf.setOutputValueClass(IntWritable.class);

		// run job and print out the statistics
		if (longestLineLengthConf.getJobName() == "")
			longestLineLengthConf.setJobName("LongestLineFinder");
		System.out.println("DistributedPatternSampling (" + longestLineLengthConf.getJobName()
						+ ")");
		System.out.println("\tInput paths: ");
		for (Path input : FileInputFormat.getInputPaths(longestLineLengthConf))
			System.out.println("\t\t\t" + input.toString());
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(longestLineLengthConf));

		JobClient.runJob(longestLineLengthConf);

		// fetch the line length
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] stats = fs.listStatus(lineLengthPath, new PathFilter()
		{
			@Override
			public boolean accept(Path path)
			{
				return path.getName().startsWith("part");
			}
		});
		FSDataInputStream fsInputStream = fs.open(stats[0].getPath());
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsInputStream));
		int lineLength = Integer.parseInt((reader.readLine().trim()));
		reader.close();
		// delete temp files
		fs.delete(lineLengthPath, true);
		System.out.println("\tLine length (without '\\n'): " + lineLength);

		/*
		 * expand each line with a given line length
		 */

		// expand each line
		JobConf expanderConf = new JobConf(getConf(), getClass());
		expanderConf.setInt(Config.LONGEST_LINE_LENGTH, lineLength);

		FileInputFormat.addInputPath(expanderConf, inputPath);
		expanderConf.setInputFormat(TextInputFormat.class);

		expanderConf.setMapperClass(ExpanderMapper.class);
		expanderConf.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(expanderConf, outputPath);
		expanderConf.setOutputFormat(TextOutputFormat.class);
		expanderConf.setOutputKeyClass(NullWritable.class);
		expanderConf.setOutputValueClass(Text.class);

		// print out and run
		if (expanderConf.getJobName() == "") expanderConf.setJobName("LineExpander");
		System.out.println("DistributedPatternSampling (" + expanderConf.getJobName() + ")");

		System.out.println("\tInput paths: ");
		for (Path input : FileInputFormat.getInputPaths(expanderConf))
			System.out.println("\t\t\t" + input.toString());
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(expanderConf));

		JobClient.runJob(expanderConf);

		return 0;
	}


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
