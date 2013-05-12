package edu.tue.cs.capa.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;


public class Helper
{
	/**
	 * Reads a record from the given filesystem/inputPath/offset.
	 * 
	 * @param fs The filesystem
	 * @param input The inputPath
	 * @param offset The offset of the first character of this record.
	 * @return The desired record
	 * @throws IOException
	 */
	public static String readRecord(FileSystem fs, Path input, long offset) throws IOException
	{
		FSDataInputStream in = fs.open(input); // find the file
		in.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String record = reader.readLine().trim();
		reader.close();

		return record;
	}
	
	public static Set<String> readRecordAsSet(FileSystem fs, Path path, long offset) throws IOException
	{
		String [] line = readRecord(fs, path, offset).split(Config.SepItemsRegex);
		return Sets.newHashSet(line);
		//		return new HashSet<String>(Arrays.asList(line));
	}


	/**
	 * Uniformly samples a pattern from a record. The pattern size should be
	 * greater than 1, if the record has more than 1 items.
	 * 
	 * @param items the record, represented in a list of items
	 * @return the sampled pattern
	 */
	public static <T extends Comparable<T>> List<T> sampleUniformly(Collection<T> items, int minLength)
	{
		Random rng = new Random();
		List<T> pattern = new ArrayList<T>();
		if (items.size() < minLength)
			return pattern;

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
				if (rng.nextBoolean())
					pattern.add(t);
		} while (pattern.size() < minLength);

		return pattern;
	}


	/**
	 * Composes a set of items to a string, separated by space. Used for output.
	 * 
	 * @param pattern a set of items
	 * @return a string containing the items
	 */
	public static String composePattern(Collection<String> pattern)
	{
		StringBuilder builder = new StringBuilder();
		for (String s : pattern)
			builder.append(s).append(" ");
		builder.deleteCharAt(builder.lastIndexOf(" "));
		return builder.toString();
	}


	/** A Comparator optimized for DoubleWritable. */
	public static class DecreasingDoubleWritableComparator extends WritableComparator
	{
		static
		{ // register this comparator
			WritableComparator.define(DoubleWritable.class, new DecreasingDoubleWritableComparator());
		}


		public DecreasingDoubleWritableComparator()
		{
			super(DoubleWritable.class);
		}


		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			double thisValue = readDouble(b1, s1);
			double thatValue = readDouble(b2, s2);
			return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
		}
	}

}
