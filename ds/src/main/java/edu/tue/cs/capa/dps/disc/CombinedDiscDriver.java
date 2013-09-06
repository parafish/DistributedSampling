package edu.tue.cs.capa.dps.disc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.tue.cs.capa.dps.disc.expand.ExpanderDriver;

public class CombinedDiscDriver extends Configured implements Tool {

	Path tempDir = new Path("temp/disc-expand/");

	// input, output,
	@Override
	public int run(String[] args) throws Exception 
	{
		if (args.length < 4) {
			System.out.println("d <inPosDir> <inNegDir> <output> <samples>");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(tempDir);

		Path leftInput = new Path(args[0]);
		Path rightInput = new Path(args[1]);
		Path output = new Path(args[2]);
		int nSamples = Integer.parseInt(args[3]);

		ExpanderDriver.main(new String[] { rightInput.toString(),
				tempDir.toString() });
		
		DiscDriver discDriver = new DiscDriver();
		discDriver.setConf(getConf());
		discDriver.run(new String[] { leftInput.toString(),
				tempDir.toString() + "/" + rightInput.getName() + "-expanded",
				output.toString(), String.valueOf(nSamples) });

		
		// clean up
		fs.delete(tempDir);
		
		return 0;

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static int main(String[] args) {
		int exitCode;
		try {
			exitCode = ToolRunner.run(new CombinedDiscDriver(), args);
		} catch (Exception e) {
			exitCode = -1;
			e.printStackTrace();
		}
		return exitCode;
	}

}
