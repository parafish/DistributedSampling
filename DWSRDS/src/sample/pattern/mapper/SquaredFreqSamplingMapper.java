package sample.pattern.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.PARAMETERS;

public class SquaredFreqSamplingMapper extends AbstractPatternMapper
{
	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		Path inputfilepath = new Path(context.getConfiguration().get(PARAMETERS.LEFT_PATH));

		String index1 = value.toString().split(PARAMETERS.SepIndexes)[0];
		String index2 = value.toString().split(PARAMETERS.SepIndexes)[1];

		long offset1 = Long.parseLong(index1);
		long offset2 = Long.parseLong(index2);

		String[] record1 = readRecord(fs, inputfilepath, offset1).split(PARAMETERS.SepItems);
		String[] record2 = readRecord(fs, inputfilepath, offset2).split(PARAMETERS.SepItems);

		List<String> intersect = new ArrayList<String>(Arrays.asList(record1));
		intersect.retainAll(new ArrayList<String>(Arrays.asList(record2)));

		List<String> pattern = sampleUniformly(intersect);

		if (pattern.size() == 0) return;

		context.write(NullWritable.get(), new Text(composePattern(pattern)));

	}
}
