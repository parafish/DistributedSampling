package util;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.apfloat.Apfloat;
import org.apfloat.FixedPrecisionApfloatHelper;

/**
 * A wrapper class to enable different RNG integrated to this Seminar project
 * @author zheyi
 *
 */
public class RNG
{
	final private BitsStreamGenerator rng;
	final static private BitsStreamGenerator seed = new MersenneTwister();
	
	public RNG()
	{
		rng = new MersenneTwister(seed.nextLong());
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
	
	// XXX: no protection on 0 and 1
	public Apfloat nextApfloat(long precision)
	{
		return new Apfloat(String.valueOf(rng.nextDouble()), precision);
	}
	
	public Apfloat nextApfloat(Apfloat start, Apfloat end, FixedPrecisionApfloatHelper helper)
	{
		if (end.compareTo(start) < 0) 
			throw new RuntimeException("The range is negative");
		
		if (end.compareTo(start) == 0) 
			return start;
		
		Apfloat r = this.nextApfloat(helper.precision());
		Apfloat range = helper.subtract(end, start);
		Apfloat incr = helper.multiply(range, r);
		return helper.add(start, incr);
	}
	
	public static void main(String [] args)
	{
		byte [] buffer = new byte[100];
		BitsStreamGenerator generator = new MersenneTwister();
		generator.nextBytes(buffer);
		StringBuilder s = new StringBuilder();
		
		System.out.println(buffer);
	}
}
