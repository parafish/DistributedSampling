package test.driver;

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

		Path input = new Path("/home/zheyi/sampling/data/webdocs.dat");
//		Path input = new Path("/home/zheyi/Downloads/test2.dat");
		Path input2 = new Path("/home/zheyi/sampling/data/chess.dat");
		Path output = PARAMETERS.localOutputPath;
		
		String nSamples = "50";
		String distribution  = "1";

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true);

		int exitCode = ToolRunner.run(conf, new ChainDriver(), new String[] { input.toString(),
//			input2.toString(),
						output.toString(), nSamples, distribution});

		System.exit(exitCode);
	}
}
