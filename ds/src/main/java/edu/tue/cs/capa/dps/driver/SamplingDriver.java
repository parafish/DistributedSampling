package edu.tue.cs.capa.dps.driver;

import org.apache.hadoop.util.ProgramDriver;

import edu.tue.cs.capa.dps.disc.DiscDriver;
import edu.tue.cs.capa.dps.expand.ExpanderDriver;
import edu.tue.cs.capa.dps.freq.FreqDriver;


public class SamplingDriver
{
	public static void main(String[] args)
	{
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try
		{
			pgd.addClass("expand", ExpanderDriver.class, "expand lines to a fixed length");
			pgd.addClass("disc", DiscDriver.class, "sample according to discriminativity");
			pgd.addClass("freq", FreqDriver.class, "sample according to frequency");
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
