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
import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;
import org.apfloat.Apint;

import util.Config;
import util.sampler.ReservoirOneSampler;


public class AreaFreqPatternMapper extends AbstractPatternMapper
{
	private String binominalCoefficient(int n, int k)
	{
		return null;
	}


	// FIXME: wrong!
	<T extends Comparable<T>> List<T> sampleWeighted(List<T> items)
	{
		if (items.size() < minLength)
			return new ArrayList<T>();

		int n = items.size();
		// sample k~s(1..n), reservoir size=1
		ReservoirOneSampler weightSamper = new ReservoirOneSampler();

		for (int i = (minLength == 0 ? 1 : minLength); i <= n; i++)
		{
			double logBiCo = ArithmeticUtils.binomialCoefficientLog(n, i);
			Apfloat floatweight = ApfloatMath.exp(new Apfloat(String.valueOf(logBiCo)));
			Apint weight = floatweight.floor();
			if (floatweight.ceil().subtract(floatweight).compareTo(floatweight.subtract(floatweight.floor())) < 0)
				weight = floatweight.ceil();
			weightSamper.sample(weight.toString(true), i);
		}
		int k = (Integer) weightSamper.getItem();

		// sample F~(|F|=k), reservoir size = k

		Collections.shuffle(items);
		
		List<T> res = items.subList(0, k);

		Collections.sort(res);
		return res;
//		return new ArrayList<T>(Arrays.asList((T) String.valueOf(res.size())));
	}


	@Override
	public void map(NullWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
					throws IOException
	{
		Path inputfilepath = new Path(leftPath);
		long offset = Long.parseLong(value.toString());

		String[] record = readRecord(fs, inputfilepath, offset).split(Config.SepItems);

		List<String> pattern = sampleWeighted(Arrays.asList(record));

		if (pattern.size() == 0)
			return;

		output.collect(NullWritable.get(), new Text(composePattern(pattern)));
	}

}