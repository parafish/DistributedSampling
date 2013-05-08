package util.sampler;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;


public class AResSampler<T> implements Sampler<T>
{
	private double key;
	private T item;

	private BitsStreamGenerator random = new MersenneTwister();


	public AResSampler()
	{
	}


	@Override
	public boolean sample(T _item, double weight)
	{
		if (weight <= 0.0d)
			return false;

		double exp = 1.0d / weight;
		double r = random.nextDouble();
		double candidateKey = Math.pow(r, exp);

		if (candidateKey > key)
		{
			if (candidateKey == 1.0d)
				System.out.println("key=" + key + "candidatekey=" + candidateKey + "recordlength="
								+ (int) (Math.log(weight) / Math.log(2)) + "\trandom=" + r);
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
