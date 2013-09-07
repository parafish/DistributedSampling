package edu.tue.cs.capa.dps.disc;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.tue.cs.capa.dps.disc.expand.ExpanderDriver;

public class CombinedDiscDriver extends Configured implements Tool
{

	Path tempDir = new Path("temp/disc-expand/");


	// input, output,
	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length < 4)
		{
			System.out.println("disc <inPosDir> <inNegDir> <output> <samples>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Path leftInputPath = new Path(args[0]);
		Path rightInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		int nSamples = Integer.parseInt(args[3]);

		// delete temp dir, and check output exists
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(tempDir, true);
		if (fs.exists(outputPath)) 
		{ throw new FileAlreadyExistsException("Output directory "
						+ outputPath + " already exists"); }

		String[] expanderArgs = new String[] { rightInputPath.toString(), tempDir.toString() };
		int exitCode = ToolRunner.run(getConf(), new ExpanderDriver(), expanderArgs);
		if (exitCode == 0)
		{
			String[] discArgs = new String[] { leftInputPath.toString(), tempDir.toString(),
							outputPath.toString(), String.valueOf(nSamples) };
			exitCode = ToolRunner.run(getConf(), new DiscDriver(), discArgs);

		}

		// clean up
		fs.delete(tempDir, true); // TODO: must clean !!!

		return exitCode;

	}


	/**
	 * @param args
	 * @throws Exception
	 */
	public static int main(String[] args)
	{
		int exitCode;
		try
		{
			exitCode = ToolRunner.run(new CombinedDiscDriver(), args);
		}
		catch (Exception e)
		{
			exitCode = -1;
			e.printStackTrace();
		}
		return exitCode;
	}

}
