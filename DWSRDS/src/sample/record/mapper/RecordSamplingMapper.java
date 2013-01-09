package sample.record.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.types.Pair;

import setting.NAMES;
import setting.PARAMETERS;

public class RecordSamplingMapper extends Mapper<Text, Text, Text, Text>
{
	private static final Text OnlyKey = new Text("1");
	private Random random = null;
	private int nSamples = 0;

	// <key, index>
	List<Pair<Double, String>> sample;
	
	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(NAMES.NSAMPLES.toString()));
		random = new Random();
		sample = new ArrayList<Pair<Double, String>>(nSamples);
		
		for (int i =0; i<nSamples; i++)
			sample.add(new Pair<Double, String>((double) 0, ""));
	}

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
		String [] indexweight = value.toString().split(PARAMETERS.SeparatorIndexWeight);
		String index = indexweight[0];
		String weight = indexweight[1];
		
		// TODO: do sth on weight
		// TODO: change it to a 'big' version
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
			context.write(OnlyKey, 
				new Text(instance.getSecond() + PARAMETERS.SeparatorIndexWeight + instance.getFirst()));	
		}
	}
}
