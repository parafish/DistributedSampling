package discriminativity;

import org.apache.hadoop.util.ProgramDriver;

import discriminativity.DiscriminativityDriver;


public class SamplingDriver
{
	public static void main(String[] args)
	{
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try
		{
			pgd.addClass("disc", DiscriminativityDriver.class, "sample according to discriminativity");
			pgd.driver(args);
			exitCode = 0;
		}
		catch (Throwable e)
		{
			e.printStackTrace();
		}
	    System.exit(exitCode);
	}

}
