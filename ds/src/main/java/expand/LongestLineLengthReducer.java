package expand;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LongestLineLengthReducer extends MapReduceBase implements Reducer<NullWritable, IntWritable, NullWritable, IntWritable>
{

	@Override
	public void reduce(NullWritable key, Iterator<IntWritable> values,
					OutputCollector<NullWritable, IntWritable> output, Reporter reporter) throws IOException
	{
		int max = 0;
		while(values.hasNext())
		{
			int curr = values.next().get();
			if (curr > max)
				max = curr;
		}
		output.collect(NullWritable.get(), new IntWritable(max));
	}

}
