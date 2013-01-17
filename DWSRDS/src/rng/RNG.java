package rng;

/**
 * A wrapper class to enable different RNG integrated to this Seminar project
 * @author zheyi
 *
 */
public class RNG
{
	final RngStream rng ;
	
	public RNG()
	{
		rng = new RngStream();
		
		rng.increasedPrecis(true);
	}
	
	public double nextDouble()
	{
		return rng.randU01();
	}
}
