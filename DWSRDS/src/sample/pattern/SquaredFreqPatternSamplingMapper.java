package sample.pattern;

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

public class SquaredFreqPatternSamplingMapper extends AbstractPatternSamplingMapper
{
	@Override
	public void map(Text key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path inputfilepath = new Path(context.getConfiguration()
						.get(NAMES.ORI_FILE_1.toString()));		// TODO: how to keep the original file
		
		String index1 = key.toString().split(" ")[0];
		String index2 = key.toString().split(" ")[1];
		
		String [] record1 = readRecord(fs, inputfilepath, index1).split(" ");
		String [] record2 = readRecord(fs, inputfilepath, index2).split(" ");

		List<String> intersect = Arrays.asList(record1);
		intersect.retainAll(Arrays.asList(record2));
		
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
