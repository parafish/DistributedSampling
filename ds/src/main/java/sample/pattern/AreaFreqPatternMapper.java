package sample.pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.util.ArithmeticUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Parameters;
import util.ReservoirOneSampler;


public class AreaFreqPatternMapper extends AbstractPatternMapper
{
	// FIXME: wrong!
	private <T extends Comparable<T>> List<T> sampleWeighted(List<T> items)
	{
		if (items.size() < minLength) return new ArrayList<T>();

		int k;
		// sample k~s(1..n), reservoir size=1
		ReservoirOneSampler weightSamper = new ReservoirOneSampler();
		do
		{
			for (int i = (minLength==0?1:minLength); i <= items.size(); i++)
			{
				long weight = ArithmeticUtils.binomialCoefficient(items.size(), i);
				weightSamper.sample(String.valueOf(weight), i);
				
//				double currKey = Math.pow(rng.nextDouble(), 1.0d / weight);
//				if (currKey > key)
//				{
//					key = currKey;
//					k = i;
//				}
			}
			k = (Integer)weightSamper.getItem();
		} while (k < minLength);

		// sample F~(|F|=k), reservoir size = k

		Collections.shuffle(items);
		List<T> res = new ArrayList<T>(items.subList(0, k));
		// int count = 0;
		// for (T item : items)
		// {
		// count++;
		// if (count <= k)
		// res.add(item);
		// else
		// {
		// int r = rng.nextInt(count);
		// if (r < k) res.set(r, item);
		// }
		// }

		Collections.sort(res);
		return res;
	}


	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		Path inputfilepath = new Path(leftPath);
		long offset = Long.parseLong(value.toString());

		String[] record = readRecord(fs, inputfilepath, offset).split(Parameters.SepItems);

		List<String> pattern = sampleWeighted(Arrays.asList(record));

		if (pattern.size() == 0) return;

		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
	}

}