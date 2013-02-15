package sample.pattern.mapper;

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
import org.apache.hadoop.mapreduce.Mapper;

import rng.RNG;

public class AbstractPatternMapper extends Mapper<NullWritable, Text, NullWritable, Text>
{
	// Filesystem, to get the file
	protected FileSystem fs = null;

	// random number generator
	RNG rng = new RNG();


	@Override
	protected void setup(Context context) throws IOException
	{
		fs = FileSystem.get(context.getConfiguration());
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
		while (pattern.size() <= 1)
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