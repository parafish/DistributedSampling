package driver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pre.AbstractPreMapper;
import pre.AreaFreqMapper;
import pre.CartesianProduct.CartesianInputFormat;
import pre.DiscriminativityMapper;
import pre.FreqMapper;
import pre.SquaredFreqMapper;
import sample.pattern.AbstractPatternMapper;
import sample.pattern.AreaFreqPatternMapper;
import sample.pattern.DiscriminativityPatternMapper;
import sample.pattern.FreqPatternMapper;
import sample.pattern.SquaredFreqPatternMapper;
import sample.record.RecordSamplingMapper;
import sample.record.RecordSamplingReducer;
import util.PARAMETERS;


public class ChainDriver extends Configured implements Tool
{
	private Path		leftInput	= null;				// required
	private Path		rightInput	= null;
	private Path		output		= null;				// required
	private int			nSamples	= 0;					// required
	private int			dist;								// required
	private boolean		ow			= false;				// optional
	private boolean[]	phase		= { true, true, true }; // optional


	private int parse(String[] arguments) throws ParseException
	{
		Options options = new Options();
		options.addOption("p", "phase", true, "which phase to run; debugging use only");
		options.addOption("w", "overwrite", false, "if overwrites the output directory");

		CommandLineParser parser = new GnuParser();
		CommandLine cmd = parser.parse(options, arguments);

		// -------------------------------------------------------------------
		// parse options
		if (cmd.hasOption('p'))
		{
			String p = cmd.getOptionValue('p');

			for (int i = 0; i < 3; i++)
			{
				if (!p.contains(String.valueOf(i+1))) phase[i] = false;
			}
		}

		if (cmd.hasOption('w')) ow = true;

		// -------------------------------------------------------------------
		// parse arguments
		String[] args = cmd.getArgs();
		if (args.length < 4 || args.length > 5)
		{
			{
				System.err.printf(
								"Usage: %s [generic options] <input> [<input2>] <output> <#samples> <distribution>\n",
								getClass().getSimpleName());
				ToolRunner.printGenericCommandUsage(System.err);
				return -1;
			}
		}

		// input path, settings
		if (args.length == 4) // algo 1, 2, 4
		{
			leftInput = new Path(args[0]);
			output = new Path(args[1]);
			nSamples = Integer.parseInt(args[2]);
			dist = Integer.valueOf(args[3]);
		}
		else
		// algo 3
		{
			leftInput = new Path(args[0]);
			rightInput = new Path(args[1]);
			output = new Path(args[2]);
			nSamples = Integer.parseInt(args[3]);
			dist = Integer.valueOf(args[4]);
		}

		return 0;
	}


	@Override
	public int run(String[] args) throws Exception
	{
		// ----------------- parse the args! ----------------------------
		if (parse(args) == -1) System.exit(1);

		// ----------------- chain it! ----------------------------
		JobConf jobConf = new JobConf(getConf(), getClass());
		jobConf.setJarByClass(getClass());

		jobConf.set(PARAMETERS.N_SAMPLES, String.valueOf(nSamples));
		jobConf.set(PARAMETERS.LEFT_PATH, leftInput.toString());

		if (ow) // delete the output
		{
			FileSystem fs = FileSystem.get(jobConf);
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(jobConf, output);

		// prepare mappers
		// weight mapper
		AbstractPreMapper weightMapper = null;
		AbstractPatternMapper patternMapper = null;
		switch (dist)
		{
		case 1:
			weightMapper = new FreqMapper();
			patternMapper = new FreqPatternMapper();
			jobConf.setInputFormat(TextInputFormat.class);
			FileInputFormat.addInputPath(jobConf, leftInput);
			break;
		case 2:
			weightMapper = new AreaFreqMapper();

			patternMapper = new AreaFreqPatternMapper();
			jobConf.setInputFormat(TextInputFormat.class);
			FileInputFormat.addInputPath(jobConf, leftInput);
			break;
		case 3:
			weightMapper = new DiscriminativityMapper();
			patternMapper = new DiscriminativityPatternMapper();
			jobConf.set(PARAMETERS.RIGHT_PATH, rightInput.toString());
			jobConf.setInputFormat(CartesianInputFormat.class);
			CartesianInputFormat.setLeftInputInfo(jobConf, TextInputFormat.class, leftInput.toString());
			CartesianInputFormat.setRightInputInfo(jobConf, TextInputFormat.class, rightInput.toString());
			break;
		case 4:
			weightMapper = new SquaredFreqMapper();
			patternMapper = new SquaredFreqPatternMapper();
			jobConf.setInputFormat(CartesianInputFormat.class);
			CartesianInputFormat.setLeftInputInfo(jobConf, TextInputFormat.class, leftInput.toString());
			CartesianInputFormat.setRightInputInfo(jobConf, TextInputFormat.class, rightInput.toString());
			break;
		default:
			System.err.println("distribution not supported");
			System.exit(1);
		}

		// -----------------------------------------------------------------
		// set MAP+ REDUCE MAP*
		// chain mapper
		jobConf.setMapperClass(ChainMapper.class);

		// map to index-weight
		if (phase[0])
			ChainMapper.addMapper(jobConf, weightMapper.getClass(), Writable.class, Text.class, Writable.class,
							Text.class, false, new JobConf(false));

		// map to sampled index-weight
		if (phase[1])
			ChainMapper.addMapper(jobConf, RecordSamplingMapper.class, Writable.class, Text.class, NullWritable.class,
							Text.class, false, new JobConf(false));

		// reducer
		jobConf.setNumReduceTasks(phase[1] ? 1 : 0);
		jobConf.setReducerClass(ChainReducer.class);

		// reduce samples from mappers to one sample (size=N_SAMPLES)
		if (phase[1])
			ChainReducer.setReducer(jobConf, RecordSamplingReducer.class, NullWritable.class, Text.class,
							NullWritable.class, Text.class, false, new JobConf(false));

		// sample patterns from specific index
		if (phase[2])
			ChainReducer.addMapper(jobConf, patternMapper.getClass(), NullWritable.class, Text.class,
							NullWritable.class, Text.class, false, new JobConf(false));

		JobClient.runJob(jobConf);
		return 0;
	}


	// for real running !
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ChainDriver(), args);
		System.exit(exitCode);
	}

}