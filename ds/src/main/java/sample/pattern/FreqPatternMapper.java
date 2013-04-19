package sample.pattern;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Config;


public class FreqPatternMapper extends AbstractPatternMapper
{
	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		String [] pathposition = value.toString().split(Config.SepFilePosition);
//		System.out.println("pathposition: " + value.toString());
		Path inputfilepath = new Path(pathposition[0]);

		long offset = Long.parseLong(pathposition[1]);
		String[] record = readRecord(fs, inputfilepath, offset).split(Config.SepItemsRegex);

		List<String> pattern = sampleUniformly(Arrays.asList(record));

		if (pattern.size() == 0) return; 

		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
	}
}