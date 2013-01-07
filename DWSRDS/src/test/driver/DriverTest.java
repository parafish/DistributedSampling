package test.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import setting.PARAMETERS;
import driver.ChainDriver;

public class DriverTest
{

	@Test
	public void testLocal() throws Exception
	{

		Configuration conf = PARAMETERS.getLocalConf();

		Path input = PARAMETERS.localInputPath;
		Path input2 = PARAMETERS.localInputPath2;
		Path output = PARAMETERS.localOutputPath;
		
		String nSamples = "10";
		String distribution  = "3";

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		int exitCode = ToolRunner.run(conf, new ChainDriver(), new String[] { input.toString(),
			input2.toString(), 
						output.toString(), nSamples, distribution});

		System.exit(exitCode);
	}
}
