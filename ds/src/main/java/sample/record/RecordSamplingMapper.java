package sample.record;

import static util.Parameters.DEBUG_MODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Parameters;
import util.ReservoirOneSampler;


/**
 * Samples from a stream using reservoir sampling.
 * <p>
 * If <em>k</em> samples are needed, there will be <em>k</em> instances of the
 * reservoir sampling algorithm, each of which maintains a reservoir of size 1.
 * 
 * @author zheyi
 * 
 */
public class RecordSamplingMapper extends MapReduceBase implements Mapper<Writable, Text, NullWritable, Text>
{
	private final static Logger					LOGGER		= Logger.getLogger(RecordSamplingMapper.class.getName());
	// instances of A-RES
	private List<ReservoirOneSampler>			instances	= null;
	// output collector
	private OutputCollector<NullWritable, Text>	output		= null;


	/**
	 * Initialized <code>N_SAMPLES</code> instances of the reservoir sampling
	 * algorithm
	 */
	@Override
	public void configure(JobConf jobConf)
	{
		// get the number of samples
		int nSamples = Integer.parseInt(jobConf.get(Parameters.N_SAMPLES));
		instances = new ArrayList<ReservoirOneSampler>(nSamples);

		for (int i = 0; i < nSamples; i++)
			instances.add(new ReservoirOneSampler());
	}


	/**
	 * Decides if an incoming key/value pair should be sampled. The
	 * <code>value</code> is the the weight, in integer.
	 */
	@Override
	public void map(Writable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
	{
		for (ReservoirOneSampler sampler : instances)
			sampler.sample(value.toString(), key.toString());

		this.output = output;
	}


	/**
	 * Emits the sampled key/value pairs.
	 */
	@Override
	public void close() throws IOException
	{
		for (ReservoirOneSampler sampler : instances)
		{
			StringBuilder output = new StringBuilder();
			output.append(sampler.getItem().toString()).append(Parameters.SepIndexWeight).append(sampler.getKey());

			this.output.collect(NullWritable.get(), new Text(output.toString()));
		}
		if (DEBUG_MODE)
			if (ReservoirOneSampler.getPrecision() >= ReservoirOneSampler.maximumPrecision)
				LOGGER.severe("Precision reaches " + ReservoirOneSampler.getPrecision() + " while the maximum is "
								+ ReservoirOneSampler.maximumPrecision);
			else
				LOGGER.info("Reservior Precisoin: " + ReservoirOneSampler.getPrecision());
	}

}
