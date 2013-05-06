package disc;

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

import util.Config;
import util.Helper.DecreasingDoubleWritableComparator;


public class DiscDriver extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(DiscDriver.class);

	private boolean ow = true;


	private DiscDriver()
	{
	}


	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length < 4)
		{
			System.out.println("disc <inPosDir> <inNegDir> <output> <samples>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path leftInput = new Path(args[0]);
		Path rightInput = new Path(args[1]);
		Path output = new Path(args[2]);
		int nSamples = Integer.parseInt(args[3]);

		JobConf jobConf = new JobConf(getConf(), getClass());
		jobConf.set(Config.RIGHT_PATH, rightInput.toString());
		jobConf.set(Config.N_SAMPLES, String.valueOf(nSamples));

		if (ow) // delete the output
		{
			FileSystem fs = FileSystem.get(jobConf);
			fs.delete(output, true);
		}

		FileInputFormat.addInputPath(jobConf, leftInput);
		FileOutputFormat.setOutputPath(jobConf, output);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setMapperClass(DiscMapper.class);

		jobConf.setOutputKeyComparatorClass(DecreasingDoubleWritableComparator.class);
		
		jobConf.setReducerClass(DiscReducer.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(DoubleWritable.class);
		jobConf.setOutputValueClass(Text.class);

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
