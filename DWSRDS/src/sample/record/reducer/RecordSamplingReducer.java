package sample.record.reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;

import setting.PARAMETERS;

public class RecordSamplingReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;
	
	// <key, index>
	PriorityQueue<Pair<Double, String>> sample;


	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		
		sample  = new PriorityQueue<Pair<Double,String>>(nSamples, new Comparator<Pair<Double, String>>()
		{
			@Override
			public int compare(Pair<Double, String> o1, Pair<Double, String> o2)
			{
				return Double.compare(o1.getFirst(), o2.getFirst());
			}});
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

			// TODO: change it to a 'big' version
			
			sample.add(new Pair<Double, String>(weight, index));
			
			if (sample.size() > nSamples)
				sample.poll();
		}
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Pair<Double, String> pair : sample)
			context.write(NullWritable.get(), new Text(pair.getSecond()));
	}
}
