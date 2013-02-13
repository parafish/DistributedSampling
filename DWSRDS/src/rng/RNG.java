package rng;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.apfloat.Apfloat;

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
	
	public Apfloat nextApfloat(long precision)
	{
		return null;
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
