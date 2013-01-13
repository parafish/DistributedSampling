package sample.record.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.types.Pair;

import setting.PARAMETERS;

public class RecordSamplingMapper extends Mapper<NullWritable, Text, NullWritable, Text>
{
	private Random random = null;
	private int nSamples = 0;

	// <key, index>
	List<Pair<Double, String>> sample;
	
	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		random = new Random();
		sample = new ArrayList<Pair<Double, String>>(nSamples);
		
		for (int i =0; i<nSamples; i++)
			sample.add(new Pair<Double, String>((double) 0, ""));
	}

	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String [] indexweight = value.toString().split(PARAMETERS.SepIndexWeight);
		String index = indexweight[0];
		String weight = indexweight[1];
		
		// TODO: do sth on weight
		// TODO: change it to a 'big' version
		// scan n reservoirs
		for (int i =0; i<sample.size(); i++)
		{
			Double rankKey = Math.pow(random.nextDouble(), 1.0 / Double.parseDouble(weight));
			if (rankKey >  sample.get(i).getFirst())
			{
				sample.set(i, new Pair<Double, String>(rankKey, index));
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (Pair<Double, String> instance : sample)
		{
			context.write(NullWritable.get(), 
				new Text(instance.getSecond() + PARAMETERS.SepIndexWeight + instance.getFirst()));	
		}
	}
}
