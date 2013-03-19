package pre;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Test;

import pre.CartesianProduct.CartesianInputFormat;
import pre.CartesianProduct.CartesianRecordReader;


public class CartesianProductTest
{
	private static JobConf			defaultConf		= new JobConf();
	private static FileSystem		localFs			= null;
	private static Path				workDir			= new Path(new Path(System.getProperty("test.build.data", "."),
																	"src/test"), "resources");
	private static final Reporter	voidReporter	= Reporter.NULL;

	static
	{
		try
		{
			localFs = FileSystem.getLocal(defaultConf);
		}
		catch (IOException e)
		{
			throw new RuntimeException("init failure", e);
		}
	}


	@Test
	public void testCartesianProduct() throws IOException
	{
		JobConf job = new JobConf();
		Path file = new Path(workDir, "testRecords");

		// localFs.delete(workDir, true);
		FileInputFormat.setInputPaths(job, file.toString());

		CartesianInputFormat cif = new CartesianInputFormat();
		CartesianInputFormat.setLeftInputInfo(job, TextInputFormat.class, file.toString());
		CartesianInputFormat.setRightInputInfo(job, TextInputFormat.class, file.toString());

		InputSplit[] splits = cif.getSplits(job, 1);

		int count = 0;
		for (int j = 0; j < splits.length; j++)
		{
			RecordReader<Text, Text> reader = cif.getRecordReader(splits[j], job, voidReporter);
			Class readerClass = reader.getClass();
			assertEquals("reader class is CartesianRecordReader.", CartesianRecordReader.class, readerClass);
			Text key = reader.createKey();
			Class keyClass = key.getClass();
			assertEquals("Key class is Text.", Text.class, keyClass);
			Text value = reader.createValue();
			Class valueClass = value.getClass();
			assertEquals("Value class is Text.", Text.class, valueClass);
			count++;
			
			// see if equals as we want
			reader.next(key, value);
			assertEquals("left 1, right 1, key", new Text("0,0"), key);
			assertEquals("left 1, right 1, value", new Text("1 2 3 4,1 2 3 4"), value);
			
			reader.next(key, value);
			assertEquals("left 1, right 2, key", new Text("0,8"), key);
			assertEquals("left 1, right 2, value", new Text("1 2 3 4,2 3 4"), value);
			
			reader.next(key, value);
			assertEquals("left 2, right 1, key", new Text("8,0"), key);
			assertEquals("left 2, right 1, value", new Text("2 3 4,1 2 3 4"), value);

			reader.next(key, value);
			assertEquals("left 2, right 2, key", new Text("8,8"), key);
			assertEquals("left 2, right 2, value", new Text("2 3 4,2 3 4"), value);
		}
	}
}
