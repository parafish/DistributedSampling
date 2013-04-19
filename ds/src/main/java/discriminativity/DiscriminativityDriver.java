package discriminativity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.Config;


public class DiscriminativityDriver extends Configured implements Tool
{
	private static final Log LOG = LogFactory.getLog(DiscriminativityDriver.class);

	private Path leftInput = null; // required
	private Path rightInput = null;
	private Path output = null; // required
	private int nSamples = 0; // required
	private boolean ow = true;


	private DiscriminativityDriver()
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

		leftInput = new Path(args[0]);
		rightInput = new Path(args[1]);
		output = new Path(args[2]);
		nSamples = Integer.parseInt(args[3]);

		JobConf jobConf = new JobConf(getConf(), getClass());
		jobConf.set(Config.LEFT_PATH, leftInput.toString());
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
		jobConf.setMapperClass(DiscriminativityRecordSampleMapper.class);
		
		jobConf.setNumReduceTasks(0);
		jobConf.setReducerClass(IdentityReducer.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		
		JobClient.runJob(jobConf);
		return 0;
	}


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new DiscriminativityDriver(), args);
		System.exit(exitCode);

	}
}
