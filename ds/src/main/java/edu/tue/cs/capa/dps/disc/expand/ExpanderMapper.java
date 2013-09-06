package edu.tue.cs.capa.dps.disc.expand;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.tue.cs.capa.dps.util.Config;
import edu.tue.cs.capa.dps.util.DpsExceptions.MissingParameterException;


public class ExpanderMapper extends MapReduceBase implements Mapper<Writable, Text, NullWritable, Text>
{
	private int lineLength = 0;
	
	@Override
	public void configure(JobConf jobConf)
	{
		lineLength=  jobConf.getInt(Config.LONGEST_LINE_LENGTH, 0);
		if (lineLength == 0)
			throw new MissingParameterException("Cannot get the longest line length.");
	}

	@Override
	public void map(Writable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		String line = value.toString().trim();
		String expandedLine = StringUtils.rightPad(line, lineLength);
		output.collect(NullWritable.get(), new Text(expandedLine));
		
//		if (line.length() < lineLength)
//		{
//			StringBuilder builder = new StringBuilder(line);
//			for (int i = 0; i < lineLength - line.length(); i++) 
//				builder.append(" ");
//			output.collect(NullWritable.get(), new Text(builder.toString()));
//		}
//		else
//		{
//			output.collect(NullWritable.get(), new Text(line));
//		}
	}

}
