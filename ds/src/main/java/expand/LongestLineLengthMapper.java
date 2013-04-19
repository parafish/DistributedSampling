package expand;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LongestLineLengthMapper extends MapReduceBase implements Mapper<Writable, Text, NullWritable, IntWritable>
{

	@Override
	public void map(Writable key, Text value, OutputCollector<NullWritable, IntWritable> output, Reporter reporter)
					throws IOException
	{
		int length = value.toString().trim().length();
		output.collect(NullWritable.get(), new IntWritable(length));
	}

}
