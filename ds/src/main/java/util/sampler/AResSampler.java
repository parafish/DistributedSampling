package util.sampler;

import java.util.Random;


public class AResSampler<T> implements Sampler<T>
{
	private double key;
	private T item;

	private Random random;


	public AResSampler()
	{
		random = new Random();
	}
	

	@Override
	public boolean sample(T _item, double weight)
	{
		if (weight <= 0.0d)
			return false;

		double exp = 1.0d / weight;
		double candidateKey = Math.pow(random.nextDouble(), exp);

		if (candidateKey > key)
		{
			key = candidateKey;
			item = _item;
			return true;
		}

		return false;
	}

	
	@Override
	public T getItem()
	{
		return item;
	}


	@Override
	public double getKey()
	{
		return key;
	}

}
