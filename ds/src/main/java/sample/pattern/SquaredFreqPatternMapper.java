package sample.pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Config;

public class SquaredFreqPatternMapper extends AbstractPatternMapper
{
	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		Path inputfilepath = new Path(leftPath);

		String index1 = value.toString().split(Config.SepIndexes)[0];
		String index2 = value.toString().split(Config.SepIndexes)[1];

		long offset1 = Long.parseLong(index1);
		long offset2 = Long.parseLong(index2);

		String[] record1 = readRecord(fs, inputfilepath, offset1).split(Config.SepItems);
		String[] record2 = readRecord(fs, inputfilepath, offset2).split(Config.SepItems);

		List<String> intersect = new ArrayList<String>(Arrays.asList(record1));
		intersect.retainAll(new ArrayList<String>(Arrays.asList(record2)));

		List<String> pattern = sampleUniformly(intersect);

		if (pattern.size() == 0) return;

		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
		
	}
}
