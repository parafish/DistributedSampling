package sample.record;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mrunit.types.Pair;
import org.apfloat.Apfloat;

import util.Parameters;

/**
 * Selects k pairs from the incoming stream, with top k keys.
 * @author zheyi
 *
 */
public class RecordSamplingReducer extends MapReduceBase implements Reducer<NullWritable, Text, NullWritable, Text>
{
	private int										nSamples	= 0;
	private OutputCollector<NullWritable, Text>		output;

	// <key, index>
	private PriorityQueue<Pair<Apfloat, String>>	sample;


	@Override
	public void configure(JobConf jobConf)
	{
		// get the number of samples
		nSamples = Integer.parseInt(jobConf.get(Parameters.N_SAMPLES));

		sample = new PriorityQueue<Pair<Apfloat, String>>(nSamples, new Comparator<Pair<Apfloat, String>>()
		{
			public int compare(Pair<Apfloat, String> o1, Pair<Apfloat, String> o2)
			{
				return o1.getFirst().compareTo(o2.getFirst());
			}
		});
	}


	@Override
	public void close() throws IOException
	{
		for (Pair<Apfloat, String> pair : sample)
			this.output.collect(NullWritable.get(), new Text(pair.getSecond()));
	}


	@Override
	public void reduce(NullWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output,
					Reporter reporter) throws IOException
	{
		while (values.hasNext())
		{
			Text value = values.next();

			String[] indexweight = value.toString().split(Parameters.SepIndexWeight);
			String index = indexweight[0];
			Apfloat weight = new Apfloat(indexweight[1]);

			sample.add(new Pair<Apfloat, String>(weight, index));

			if (sample.size() > nSamples) sample.poll();
		}
		this.output = output;
	}
}
