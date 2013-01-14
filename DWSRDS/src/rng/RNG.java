package rng;

/**
 * A wrapper class to enable different RNG integrated to this Seminar project
 * @author zheyi
 *
 */
public class RNG
{
	RngStream rng = new RngStream();
	
	public RNG()
	{
		rng.increasedPrecis(true);
	}
	
	public double nextDouble()
	{
		return rng.randU01();
	}
}
