package freq;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import util.Config;
import util.DpsExceptions.MissingParameterException;
import util.Helper;


public class FreqReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, NullWritable, Text>
{
	private static final Log LOG = LogFactory.getLog(FreqReducer.class);

	private FileSystem fs;
	private int nSamples;
	private int collectedSample = 0;

	private int minPatternLength;
	private double maxKey = 0.0d;


	@Override
	public void configure(JobConf jobConf)
	{
		// get the number of samples
		nSamples = Integer.parseInt(jobConf.get(Config.N_SAMPLES));
		if (nSamples == 0)
			throw new MissingParameterException("The sample size is not set");

		minPatternLength = jobConf.getInt(Config.MIN_PATTERN_LENGTH, Config.DEFAULT_MIN_PATTERN_LENGTH);
		
		try
		{
			fs = FileSystem.get(jobConf);
		}
		catch (IOException e)
		{
			LOG.error("IO Exception when reading input file");
			throw new RuntimeException(e);
		}
	}


	@Override
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output,
					Reporter reporter) throws IOException
	{
		if (maxKey == 0.0d)
		{
			maxKey = key.get();
			LOG.info("The maximum key: " + maxKey);
		}

		while (collectedSample < nSamples && values.hasNext())
		{
			collectedSample++;

			Text value = values.next();

			String[] pathposition = value.toString().split(Config.SepFilePosition);
			Path inputfilepath = new Path(pathposition[0]);
			long offset = Long.parseLong(pathposition[1]);
			String[] record = Helper.readRecord(fs, inputfilepath, offset).split(Config.SepItemsRegex);

			List<String> pattern = Helper.sampleUniformly(Arrays.asList(record), minPatternLength);

			if (pattern.size() == 0)
				return;

			output.collect(NullWritable.get(), new Text(Helper.composePattern(pattern)));
		}
	}

}