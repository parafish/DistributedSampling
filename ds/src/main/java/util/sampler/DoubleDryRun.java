package util.sampler;

import java.util.Random;


public class DoubleDryRun
{
	private double key;
	private Object item;

	private Random random = new Random();
	double lastRandom;


	public boolean sample(long w, Object obj)
	{
		if (w <= 0)
			return false;
		if (lastRandom == 0.0d)
			lastRandom = random.nextDouble();

		double exp = 1.0d / w;
		double candidateKey = Math.pow(lastRandom, exp);

		lastRandom = 0.0d;

		if (key == 0.0d || candidateKey > key) // if the reservoir is not full, or the candidate key is larger
		{
			key = candidateKey;
			item = obj;
			return true;
		}

		return false;
	}


	public boolean sampleDryRun(long w)
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


	public Object getItem()
	{
		return item;
	}


	public static void main(String[] args)
	{
		DoubleDryRun sampler = new DoubleDryRun();
		long weight = (long)Math.pow(2, 40);
		System.out.println("weight:  " + weight);

		int times = 340000;
		long start = System.currentTimeMillis();
		sampler.sample(weight, " ");
		for (int i = 0; i < times; i++)
		{
			sampler.sampleDryRun(weight);
		}
		long end = System.currentTimeMillis();

		System.out.println("run dry run for " + times + " times, time: " + (end - start) + " seconds");
	}

}
