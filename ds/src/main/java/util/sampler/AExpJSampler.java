package util.sampler;

import java.util.Random;


public class AExpJSampler<T> implements Sampler<T>
{
	private double key;
	private T item;

	private Random random = new Random();

	private boolean startjump = true;
	private double accumulation;
	private double Xw;


	@Override
	public boolean sample(T item, double weight)
	{
		if (key == 0.0d) // if the reservoir is not full
		{
			if (weight <= 0)
				return false;

			double exp = 1.0d / weight;
			key = Math.pow(random.nextDouble(), exp);
			this.item = item;
			return true;
		}
		else
		{
			if (startjump)
			{
				double r = random.nextDouble();
				Xw = Math.log(r) / Math.log(key);
				accumulation = 0L;
				startjump = false;
			}

			// if skipped
			accumulation += weight;
			if (accumulation >= Xw) // no skip
			{
				double tw = Math.pow(key, weight);
				double r2 = random.nextDouble() * (1.0d - tw) + tw;
				double exp = 1.0d / weight;
				key = Math.pow(r2, exp);
				this.item = item;
				startjump = true;
				return true;
			}

			return false; // skip
		}
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
