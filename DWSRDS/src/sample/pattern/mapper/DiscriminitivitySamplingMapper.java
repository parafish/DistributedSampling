package sample.pattern.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import setting.PARAMETERS;

public class DiscriminitivitySamplingMapper extends AbstractPatternMapper
{

	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		// XXX: move FileSystem construction to 'setup'
		// but it limits the index expressiveness
		Path input1 = new Path(context.getConfiguration().get(PARAMETERS.LEFT_PATH));		
		Path input2 = new Path(context.getConfiguration().get(PARAMETERS.RIGHT_PATH));
		
		String index1 = value.toString().split(PARAMETERS.SepIndexes)[0];
		String index2 = value.toString().split(PARAMETERS.SepIndexes)[1];
		
		long offset1 = Long.parseLong(index1);
		long offset2 = Long.parseLong(index2);
		
		final String [] positiveRecord = readRecord(fs, input1, offset1).split(PARAMETERS.SepItems);
		final String [] negativeRecord = readRecord(fs, input2, offset2).split(PARAMETERS.SepItems);
	
		List<String> negList = new ArrayList<String>(Arrays.asList(negativeRecord));
		
		// sample from complement
		List<String> complement = new ArrayList<String>(Arrays.asList(positiveRecord));  
		complement.removeAll(negList);
		
		List<String> pattern1 = sampleUniformly(complement);
		while (pattern1.size() == 0)					// ensure not empy
			pattern1 = sampleUniformly(complement);
		
		// sample from intersection
		List<String> intersect = new ArrayList<String>(Arrays.asList(positiveRecord));
		intersect.retainAll(negList);
		
		List<String> pattern2 = sampleUniformly(intersect);
		
		// the result pattern is the union of pattern1 and pattern2
		pattern1.addAll(pattern2);
		Set<String> pattern = new TreeSet<String>(pattern1);
		
		if (pattern.size() == 0)
			return;
	
		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));
	
		context.write(NullWritable.get(), new Text(builder.toString()));
	
	}

}
