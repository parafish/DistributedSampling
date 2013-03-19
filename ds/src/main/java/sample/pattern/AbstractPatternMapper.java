package sample.pattern;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

import util.PARAMETERS;
import util.RNG;


public abstract class AbstractPatternMapper extends MapReduceBase implements Mapper<NullWritable, Text, NullWritable, Text>
{
	// Filesystem, to get the file
	protected FileSystem fs = null;
	// random number generator
	protected RNG  rng = new RNG();
	// left path
	protected String leftPath = null;
	// right path
	protected String rightPath = null;

	@Override
	public void configure(JobConf jobConf)
	{
		leftPath = jobConf.get(PARAMETERS.LEFT_PATH);
		rightPath = jobConf.get(PARAMETERS.RIGHT_PATH);
		
		try
		{
			fs = FileSystem.get(jobConf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}


	// fs - path - offset, return a record (line)
	protected String readRecord(FileSystem fs, Path input, long offset) throws IOException
	{
		// find the file
		FSDataInputStream in = fs.open(input);
		in.seek(offset);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String record = reader.readLine();
		in.close();
		reader.close();

		return record;
	}


	// uniformly sample from a record, pattern >= 1
	protected <T> List<T> sampleUniformly(List<T> items)
	{
		ArrayList<T> pattern = new ArrayList<T>();
		if (items.size() < 2)
		{
			pattern.addAll(items);
			return pattern;
		}
		
		while (pattern.size() < 2)
		{
			pattern.clear();
			for (T t : items)
				if (rng.nextBoolean()) pattern.add(t);
		}
		return pattern;
	}


	protected String composePattern(Collection<String> pattern)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));
		return builder.toString();
	}

}