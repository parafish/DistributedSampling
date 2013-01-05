package sample.pattern;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AbstractPatternSamplingMapper extends Mapper<Text, Text, Text, NullWritable>
{
	// fs - path - offset, return a record (line)
	protected String readRecord(FileSystem fs, Path input, String offset) throws IOException
	{
		long index = Long.parseLong(offset);
		
		// find the file
		FSDataInputStream in = fs.open(input);
		in.seek(index);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String record = reader.readLine();
		in.close();
		reader.close();
		
		return record;
	}
	
	protected <T> List<T> sampleUniformly(List<T> items)
	{
		ArrayList<T> pattern = new ArrayList<T>();
		
		Random random = new Random();
		for (T t : items)
		{
			if (random.nextBoolean())
				pattern.add(t);
		}
		
		// ensure that at least one item in this pattern	// TODO: should we do this? maybe no
		return pattern;
	}
}