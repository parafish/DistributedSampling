package edu.tue.cs.capa.util.sampler;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;


public class DryRunSampler<T> implements Sampler<T>
{
	private double key;
	private T item;

	private BitsStreamGenerator random = new MersenneTwister();
	private double lastRandom;
	private long overflowed;


	public boolean sample(T obj, double w)
	{
		if (w <= 0)
			return false;
		if (lastRandom == 0.0d)
			lastRandom = random.nextDouble();

		double exp = 1.0d / w;
		double candidateKey = Math.pow(lastRandom, exp);

		if (candidateKey >= key) // if the reservoir is not full, or the candidate key is larger
		{
			if (candidateKey == 1.0d)
			{
				System.out.println("key=" + key + "candidatekey=" + candidateKey + "recordlength="
								+ (int) (Math.log(w) / Math.log(2)) + "\trandom=" + lastRandom);
				overflowed ++ ;
			}
			key = candidateKey;
			item = obj;
			return true;
		}
		lastRandom = 0.0d;

		return false;
	}


	public boolean dryRun(double w)
	{
		if (key == 0.0d)
			return true;

		if (w <= 0)
			return false;

		lastRandom = random.nextDouble();

		double exp = 1.0d / w;
		double candidateKey = Math.pow(lastRandom, exp);

		if (candidateKey >= key)
			return true;

		return false;
	}


	@Override
	public double getKey()
	{
		return key;
	}


	@Override
	public T getItem()
	{
		return item;
	}


	@Override
	public long getOverflowed()
	{
		return overflowed;
	}
}
