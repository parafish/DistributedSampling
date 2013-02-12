package rng;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;

/**
 * A wrapper class to enable different RNG integrated to this Seminar project
 * @author zheyi
 *
 */
public class RNG
{
	final BitsStreamGenerator rng;
	
	public RNG()
	{
		rng = new MersenneTwister(null);
	}
	
	public double nextDouble()
	{
		return rng.nextDouble();
	}
	
	public int nextInt(int max)
	{
		return rng.nextInt(max);
	}
	
	public boolean nextBoolean()
	{
		return rng.nextBoolean();
	}
}
