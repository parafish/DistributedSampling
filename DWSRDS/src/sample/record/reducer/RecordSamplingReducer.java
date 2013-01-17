package sample.record.reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.apfloat.Apfloat;

import setting.PARAMETERS;

public class RecordSamplingReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;
	
	// <key, index>
	PriorityQueue<Pair<Apfloat, String>> sample;


	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		
		sample  = new PriorityQueue<Pair<Apfloat,String>>(nSamples, new Comparator<Pair<Apfloat, String>>()
		{
			@Override
			public int compare(Pair<Apfloat, String> o1, Pair<Apfloat, String> o2)
			{
				return o1.getFirst().compareTo(o2.getFirst());
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
			
			Apfloat weight = new Apfloat(indexweight[1]);
			
			sample.add(new Pair<Apfloat, String>(weight, index));
			
			if (sample.size() > nSamples)
				sample.poll();
		}
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Pair<Apfloat, String> pair : sample)
			context.write(NullWritable.get(), new Text(pair.getSecond()));
	}
}
