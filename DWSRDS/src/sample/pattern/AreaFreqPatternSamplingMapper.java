package sample.pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.NAMES;

public class AreaFreqPatternSamplingMapper extends AbstractPatternSamplingMapper
{

	private <T> List<T> sampleWeighted(List<T> items)
	{
		ArrayList<T> pattern = new ArrayList<T>();
		// sample k~id(1..n)
		// FIXME
		// sample F~(|F|=k)
		
		// ensure that at least one item in this pattern
		return pattern;
	}
	
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		// TODO: how to keep the original file
		Path inputfilepath = new Path(context.getConfiguration().get(NAMES.ORI_FILE_1.toString()));	
		String[] record = readRecord(fs, inputfilepath, key.toString()).split(" ");

		List<String> pattern = sampleUniformly(Arrays.asList(record));
		
		if (pattern.size() == 0)
			return;

		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));

		context.write(new Text(builder.toString()), NullWritable.get());

	}
	
	
	
	
	
	

}
