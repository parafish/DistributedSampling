package sample.pattern.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.NAMES;
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
		Collections.shuffle(items);		
		return items.subList(0, k);
	}
	
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		// TODO: how to keep the original file
		Path inputfilepath = new Path(context.getConfiguration().get(NAMES.ORI_FILE_1.toString()));	
		String[] record = readRecord(fs, inputfilepath, value.toString()).split(PARAMETERS.SeparatorItem);

		List<String> pattern = sampleWeighted(Arrays.asList(record));
		
		if (pattern.size() == 0)
			return;

		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));

		context.write(new Text(builder.toString()), NullWritable.get());

	}
	
	
	
	
	
	

}