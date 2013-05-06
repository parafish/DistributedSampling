package util.sampler;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;


public class DryRunSampler<T> implements Sampler<T>
{
	private double key;
	private T item;

	private BitsStreamGenerator random = new MersenneTwister();
	private double lastRandom;


	public boolean sample(T obj, double w)
	{
		if (w <= 0)
			return false;
		if (lastRandom == 0.0d)
			lastRandom = random.nextDouble();

		double exp = 1.0d / w;
		double candidateKey = Math.pow(lastRandom, exp);

		if (key == 0.0d || candidateKey > key) // if the reservoir is not full, or the candidate key is larger
		{
			if (candidateKey == 1.0d)
				System.out.println("Key reached 1.0, with record length = " + Math.log(w) / Math.log(2) + "random = "
								+ lastRandom);
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

		if (candidateKey > key)
			return true;

		return false;
	}


	public double getKey()
	{
		return key;
	}


	public T getItem()
	{
		return item;
	}


	public static void main(String[] args)
	{
		DryRunSampler sampler = new DryRunSampler();
		long weight = (long) Math.pow(2, 40);
		System.out.println("weight:  " + weight);

		int times = 340000;
		long start = System.currentTimeMillis();
		sampler.sample(" ", weight);
		for (int i = 0; i < times; i++)
		{
			sampler.dryRun(weight);
		}
		long end = System.currentTimeMillis();

		System.out.println("run dry run for " + times + " times, time: " + (end - start) + " seconds");
	}
}
