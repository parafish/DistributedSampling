package sample.record.reducer;

import java.io.IOException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import setting.PARAMETERS;

public class RecordSamplingReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;
	private Random random = null;

	// <key, index>
	SortedMap<Double, String> sample;


	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		random = new Random();
		sample = new TreeMap<Double, String>();
	}


	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException
	{
		for (Text value : values)
		{
			String[] indexweight = value.toString().split(PARAMETERS.SepIndexWeight);
			String index = indexweight[0];
			Double weight = Double.parseDouble(indexweight[1]);

			// TODO: do sth on weight
			// TODO: change it to a 'big' version
			if (sample.size() < nSamples)
				sample.put(weight, index);
			else
				if (weight > sample.firstKey())
				{
					sample.remove(sample.firstKey());
					sample.put(weight, index);
				}
		}
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (String index : sample.values())
			context.write(NullWritable.get(), new Text(index));
	}
}
