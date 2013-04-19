package util.sampler;

import java.math.BigInteger;
import java.util.Random;

import org.apfloat.Apfloat;
import org.apfloat.FixedPrecisionApfloatHelper;

import util.RNG;


public class DryRunSampler
{
	private Apfloat key = null;
	private Object item = null;

	private final RNG random = new RNG();
	Apfloat lastRandom = null;

	private static int precision = 20;
	private static FixedPrecisionApfloatHelper helper = new FixedPrecisionApfloatHelper(precision);


	public boolean sample(BigInteger w, Object obj)
	{
		Apfloat floatWeight = new Apfloat(w);
		if (floatWeight.compareTo(Apfloat.ZERO) <= 0)
			return false;

		if (lastRandom == null)
			lastRandom = random.nextApfloat(precision);

		Apfloat exp = helper.divide(Apfloat.ONE, floatWeight);
		Apfloat candidateKey = helper.pow(lastRandom, exp);
		lastRandom = null;

		if (key == null || candidateKey.compareTo(key) > 0) // if the reservoir is not full, or the candidate key is larger
		{
			key = candidateKey;
			item = obj;
			return true;
		}

		return false;
	}


	public boolean sampleDryRun(BigInteger w)
	{
		if (key == null)
			return true;
		
		Apfloat floatWeight = new Apfloat(w);
		if (floatWeight.compareTo(Apfloat.ZERO) <= 0)
			return false;

		lastRandom = random.nextApfloat(precision);
		Apfloat exp = helper.divide(Apfloat.ONE, floatWeight);
		Apfloat candidateKey = helper.pow(lastRandom, exp);

		if (candidateKey.compareTo(key) > 0)
			return true;

		return false;
	}
	

	public String getKey()
	{
		return key.toString(true);
	}


	public Object getItem()
	{
		return item;
	}
	
	public static void main(String [] args)
	{
		DryRunSampler sampler = new DryRunSampler();
		BigInteger weight = new BigInteger("2").pow(40);
		Random random = new Random();

		int times = 340000;
		long start = System.currentTimeMillis();
		sampler.sample(weight, " ");
		double min = 0.0d;
		for (int i =0; i<times; i++)
		{
			sampler.sampleDryRun(weight);
		}
		long end = System.currentTimeMillis();
		
		System.out.println("run dry run for " + times + " times, time: " + (end - start) / 1000 + " seconds");
	}

}
