package sample.record.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.types.Pair;

import setting.NAMES;
import setting.PARAMETERS;

public class RecordSamplingReducer extends Reducer<Text, Text, Text, Text>
{
	private static final Text OnlyKey = new Text("1");
	private int nSamples = 0;
	private Random random = null;

	// <key, index>
	List<Pair<Double, String>> sample;


	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(NAMES.NSAMPLES.toString()));
		random = new Random();
		sample = new ArrayList<Pair<Double, String>>(nSamples);

		for (int i = 0; i < nSamples; i++)
			sample.add(new Pair<Double, String>((double) 0, ""));
	}


	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
					InterruptedException
	{
		for (Text value : values)
		{
			String[] indexweight = value.toString().split(PARAMETERS.SeparatorIndexWeight);
			String index = indexweight[0];
			String weight = indexweight[1];

			// TODO: do sth on weight
			
			// TODO: change it to a 'big' version
			for (int i = 0; i < sample.size(); i++)
			{
				Double rankKey = Math.pow(random.nextDouble(), 1.0 / Double.parseDouble(weight));

				if (rankKey > sample.get(i).getFirst())
				{
					sample.set(i, new Pair<Double, String>(rankKey, index));
				}
			}
		}
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Pair<Double, String> instance : sample)
		{
			context.write(OnlyKey, new Text(instance.getSecond()));
		}
	}
}
