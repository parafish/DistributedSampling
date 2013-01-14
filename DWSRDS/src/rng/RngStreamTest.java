package rng;

public class RngStreamTest
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		RngStream rng1 = new RngStream("my stream");
		RngStream rng2 = new RngStream("another stream");
		
		for (int i =0; i< 5; i++)
		{
			System.out.println(rng1.randU01());
			System.out.println(rng2.randU01());
			
		}

	}

}
