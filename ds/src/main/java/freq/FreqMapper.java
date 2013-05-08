package freq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Config;
import util.DpsCounters;
import util.DpsExceptions.MissingParameterException;
import util.sampler.AResSampler;
import util.sampler.Sampler;


/**
 * Maps the offset/text pairs to offset/weight pairs, with the help of its
 * superclass.
 * <p>
 * The weight are calculated as follows: <blockquote> w = 2^|R| </blockquote>
 * where |R| is the length of the record
 * 
 * @author zheyi
 * 
 */
public class FreqMapper extends MapReduceBase implements Mapper<Writable, Text, DoubleWritable, Text>
{
	private static final Log LOG = LogFactory.getLog(FreqMapper.class);

	private String filePath;
	private List<Sampler<String>> instances;
	private OutputCollector<DoubleWritable, Text> output;
	private Reporter reporter;
	
	private int maxRecordLength;


	@Override
	public void configure(JobConf jobConf)
	{
		filePath = jobConf.get("map.input.file");

		// get the number of samples
		int nSamples = jobConf.getInt(Config.N_SAMPLES, 0);
		if (nSamples == 0)
			throw new MissingParameterException("The sample size is not set");
		
		maxRecordLength = jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH);
		LOG.info("Max record length: " + maxRecordLength);
		
		instances = new ArrayList<Sampler<String>>(nSamples);
		for (int i = 0; i < nSamples; i++)
			instances.add(new AResSampler<String>());
	}


	@Override
	public void map(Writable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter)
					throws IOException
	{
		String[] items = value.toString().trim().split(Config.SepItemsRegex);
		int exp = items.length > maxRecordLength ? maxRecordLength : items.length;
		double weight = Math.pow(2, exp);
		String fileIndex = filePath + Config.SepFilePosition + key.toString();
		
		for(Sampler<String> sampler : instances)
			sampler.sample(fileIndex, weight);
		
		this.output = output;
		this.reporter = reporter;
	}

	
	/**
	 * Emits the sampled key/value pairs.
	 */
	@Override
	public void close() throws IOException
	{
		for (Sampler<String> sampler : instances)
		{
			this.output.collect(new DoubleWritable(sampler.getKey()), new Text(sampler.getItem()));
			this.reporter.incrCounter(DpsCounters.OVERFLOWED_TIMES, sampler.getOverflowed());
		}
	}

	

}
