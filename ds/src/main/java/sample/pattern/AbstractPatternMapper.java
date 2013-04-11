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

import util.Parameters;
import util.RNG;


/**
 * Reads a record from the input file(s), where the record is located at
 * <code>value</code>; then samples a pattern from the record.
 * <p>
 * The distribution is implemented in a subclass.
 * 
 * @author zheyi
 * 
 */
public abstract class AbstractPatternMapper extends MapReduceBase implements
				Mapper<NullWritable, Text, NullWritable, Text>
{
	// Filesystem, to get the file
	protected FileSystem	fs			= null;
	// random number generator
	protected RNG			rng			= new RNG();
	// left path
	protected String		leftPath	= null;
	// right path
	protected String		rightPath	= null;
	// minimum pattern length
	protected int			minLength	= 0;


	@Override
	public void configure(JobConf jobConf)
	{
		leftPath = jobConf.get(Parameters.LEFT_PATH);
		rightPath = jobConf.get(Parameters.RIGHT_PATH);
		minLength = jobConf.getInt(Parameters.MIN_PATTERN_LENGTH, 0);
		try
		{
			fs = FileSystem.get(jobConf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}


	/**
	 * Reads a record from the given filesystem/inputPath/offset.
	 * 
	 * @param fs
	 *            The filesystem
	 * @param input
	 *            The inputPath
	 * @param offset
	 *            The offset of the first character of this record.
	 * @return The desired record
	 * @throws IOException
	 */
	protected String readRecord(FileSystem fs, Path input, long offset) throws IOException
	{
		FSDataInputStream in = fs.open(input); // find the file
		in.seek(offset);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String record = reader.readLine().trim();
		in.close();
		reader.close();

		return record;
	}


	/**
	 * Uniformly samples a pattern from a record. The pattern size should be
	 * greater than 1, if the record has more than 1 items.
	 * 
	 * @param items
	 *            the record, represented in a list of items
	 * @return the sampled pattern
	 */
	protected <T extends Comparable<T>> List<T> sampleUniformly(List<T> items)
	{
		ArrayList<T> pattern = new ArrayList<T>();
		if (items.size() < minLength) return pattern;

		// Collections.shuffle(items);
		// pattern.addAll(items.size() - minLength, items);
		// Collections.sort(pattern);

		if (items.size() == minLength)
		{
			pattern.addAll(items);
			return pattern;
		}

		do
		{
			pattern.clear();
			for (T t : items)
				if (rng.nextBoolean()) pattern.add(t);
		} while (pattern.size() < minLength);

		return pattern;
	}


	/**
	 * Composes a set of items to a string, separated by space. Used for output.
	 * 
	 * @param pattern a set of items
	 * @return a string containing the items
	 */
	protected String composePattern(Collection<String> pattern)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));
		return builder.toString();
	}

}