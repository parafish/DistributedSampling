package sample.pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Parameters;

public class DiscriminativityPatternMapper extends AbstractPatternMapper
{

	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{

		Path input1 = new Path(leftPath);		
		Path input2 = new Path(rightPath);
		
		String[] indices = value.toString().split(Parameters.SepIndexes);
		String index1 = indices[0];
		String index2 = indices[1];
		
		long offset1 = Long.parseLong(index1);
		long offset2 = Long.parseLong(index2);
		
		final String [] positiveRecord = readRecord(fs, input1, offset1).split(Parameters.SepItems);
		final String [] negativeRecord = readRecord(fs, input2, offset2).split(Parameters.SepItems);
	
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
	
		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
		
	}

}
