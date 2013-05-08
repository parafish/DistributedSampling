package expand;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.Config;


public class ExpanderDriver extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(ExpanderDriver.class);


	private ExpanderDriver()
	{

	}


	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			System.out.println("expand <input> <output>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String input = args[0];
		Path inputPath = new Path(input);
		String output = args[1];

		Path lineLengthPath = new Path(output + "/longest-line-length");

		// find the longest line length
		JobConf longestLineLengthConf = new JobConf(getConf(), getClass());

		FileInputFormat.addInputPath(longestLineLengthConf, new Path(input));
		longestLineLengthConf.setInputFormat(TextInputFormat.class);

		longestLineLengthConf.setMapperClass(LongestLineLengthMapper.class);
		longestLineLengthConf.setCombinerClass(LongestLineLengthReducer.class);
		longestLineLengthConf.setReducerClass(LongestLineLengthReducer.class);

		FileOutputFormat.setOutputPath(longestLineLengthConf, lineLengthPath);
		longestLineLengthConf.setOutputFormat(TextOutputFormat.class);
		longestLineLengthConf.setOutputKeyClass(NullWritable.class);
		longestLineLengthConf.setOutputValueClass(IntWritable.class);

		// run job and print out the statistics
		System.out.println("DistributedPatternSampling (" + longestLineLengthConf.getJobName() + ")");
		System.out.println("\tInput paths: ");
		Path inputs[] = FileInputFormat.getInputPaths(longestLineLengthConf);
		for (int ctr = 0; ctr < inputs.length; ctr++)
			System.out.println("\t\t\t" + inputs[ctr].toString());
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(longestLineLengthConf));
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(longestLineLengthConf);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / (float) 1000.0 + " seconds.");

		// fetch the line length
		FileSystem fs = FileSystem.get(getConf());
		FSDataInputStream fsInputStream = fs.open(new Path(lineLengthPath.toString() + "/part-00000"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsInputStream));
		int lineLength = Integer.parseInt((reader.readLine().trim()));
		reader.close();
		LOG.info("Line length (without '\\r'): " + lineLength);

		// expand each line
		JobConf expanderConf = new JobConf(getConf(), getClass());
		expanderConf.setInt(Config.LONGEST_LINE_LENGTH, lineLength);

		FileInputFormat.addInputPath(expanderConf, new Path(input));
		expanderConf.setInputFormat(TextInputFormat.class);

		expanderConf.setMapperClass(ExpanderMapper.class);
		expanderConf.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(expanderConf, new Path(output + "/" + inputPath.getName() + "-expanded-"
						+ (lineLength + 1)));
		expanderConf.setOutputFormat(TextOutputFormat.class);
		expanderConf.setOutputKeyClass(NullWritable.class);
		expanderConf.setOutputValueClass(Text.class);

		// print out and run
		System.out.println("DistributedPatternSampling (" + expanderConf.getJobName() + ")");
		System.out.println("\tInput paths: ");
		inputs = FileInputFormat.getInputPaths(expanderConf);
		for (int ctr = 0; ctr < inputs.length; ctr++)
			System.out.println("\t\t\t" + inputs[ctr].toString());
		System.out.println("\tOutput path: ");
		System.out.println("\t\t\t" + FileOutputFormat.getOutputPath(expanderConf));
		System.out.println("\tLongest line length (without \\n)): " + lineLength);
		
		
		startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(expanderConf);
		end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / (float) 1000.0 + " seconds.");

		return 0;
	}


	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ExpanderDriver(), args);
		System.exit(exitCode);

	}
}
