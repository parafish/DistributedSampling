package sample.pattern.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
		Path inputfilepath = new Path(context.getConfiguration()
						.get(PARAMETERS.LEFT_PATH));		
		
		long offset = Long.parseLong(value.toString());
		String[] record = readRecord(fs, inputfilepath, offset).split(PARAMETERS.SepItems);

		List<String> pattern = sampleUniformly(Arrays.asList(record));
		
		if (pattern.size() == 0)
			return;

		context.write(NullWritable.get(), new Text(composePattern(pattern)));
	}
}
