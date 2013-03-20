package sample.pattern;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Parameters;


public class FreqPatternMapper extends AbstractPatternMapper
{
	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		Path inputfilepath = new Path(leftPath);

		long offset = Long.parseLong(value.toString());
		String[] record = readRecord(fs, inputfilepath, offset).split(Parameters.SepItems);

		List<String> pattern = sampleUniformly(Arrays.asList(record));

		if (pattern.size() == 0) return;

		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
	}
}