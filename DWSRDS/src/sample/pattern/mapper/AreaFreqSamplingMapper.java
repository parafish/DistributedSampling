package sample.pattern.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.PARAMETERS;

public class AreaFreqSamplingMapper extends AbstractPatternMapper
{
	private Random random = new Random();

	private <T> List<T> sampleWeighted(List<T> items)
	{
		int k = 0;
		double key = 0.0;
		// sample k~id(1..n)
		for (int i=1; i<= items.size(); i++)
		{
			double currKey = Math.pow(random.nextDouble(), 1.0 / i);
			if (currKey > key)
			{
				key = currKey;
				k = i;
			}
		}
		
		// sample F~(|F|=k)		
		// XXX: keep ascending order
		Collections.shuffle(items);		
		return items.subList(0, k);
	}
	
	
	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		Path inputfilepath = new Path(context.getConfiguration().get(PARAMETERS.LEFT_PATH));
		long offset = Long.parseLong(value.toString());
		
		String[] record = readRecord(fs, inputfilepath, offset).split(PARAMETERS.SepItems);

		List<String> pattern = sampleWeighted(Arrays.asList(record));
		
		if (pattern.size() == 0)
			return;

		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));

		context.write(NullWritable.get(), new Text(builder.toString()));

	}
	
	
	
	
	
	

}