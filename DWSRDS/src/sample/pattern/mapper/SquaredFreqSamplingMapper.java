package sample.pattern.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.NAMES;
import setting.PARAMETERS;

public class SquaredFreqSamplingMapper extends AbstractPatternMapper
{
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path inputfilepath = new Path(context.getConfiguration()
						.get(NAMES.ORI_FILE_1.toString()));		
		// TODO: how to keep the original file
		
		String index1 = value.toString().split(PARAMETERS.SeparatorIndex)[0];
		String index2 = value.toString().split(PARAMETERS.SeparatorIndex)[1];
		
		String [] record1 = readRecord(fs, inputfilepath, index1).split(PARAMETERS.SeparatorItem);
		String [] record2 = readRecord(fs, inputfilepath, index2).split(PARAMETERS.SeparatorItem);

		List<String> intersect = new ArrayList<String>(Arrays.asList(record1));
		intersect.retainAll(new ArrayList<String>(Arrays.asList(record2)));
		
		List<String> pattern = sampleUniformly(intersect);
		
		if (pattern.size() == 0)
			return;

		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));

		context.write(new Text(builder.toString()), NullWritable.get());

	}
}
