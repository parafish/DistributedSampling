package sample.pattern.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.PARAMETERS;

public class FreqSamplingMapper extends AbstractPatternMapper
{
	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path inputfilepath = new Path(context.getConfiguration()
						.get(PARAMETERS.LEFT_PATH));		
		
		long offset = Long.parseLong(value.toString());
		String[] record = readRecord(fs, inputfilepath, offset).split(PARAMETERS.SepItems);

		List<String> pattern = sampleUniformly(Arrays.asList(record));
		
		if (pattern.size() == 0)
			return;

		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));

		context.write(NullWritable.get(), new Text(builder.toString()));
	}
}
