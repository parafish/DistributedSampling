package pre;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

public class PreMapperTest
{
	private long lineNumber = 1234567L;
	private Text record = new Text("2 3 4 5 6 7");
	private Text pairRecords = new Text("1 2 3 4,2 3 4 5");
	
	@Test
	public void testFreqWeightMapper()
	{
		new MapDriver<Writable, Text, Writable, Text>()
			.withMapper(new FreqMapper())
			.withInput(new LongWritable(lineNumber), record)
			.withOutput(new LongWritable(lineNumber), new Text("64"))
			.runTest();
	}
	
	@Test
	public void testAreaFreqWeightMapper()
	{
		new MapDriver<Writable, Text, Writable, Text>()
			.withMapper(new AreaFreqMapper())
			.withInput(new LongWritable(lineNumber), record)
			.withOutput(new LongWritable(lineNumber), new Text("192"))
			.runTest();
	}
	
	@Test
	public void testDiscriminitivityWeightMapper()
	{
		new MapDriver<Writable, Text, Writable, Text>()
			.withMapper(new DiscriminativityMapper())
			.withInput(new Text("0,0"), pairRecords)
			.withOutput(new Text("0,0"), new Text("8"))
			.runTest();
	}

	@Test
	public void testSquaredFreqWeightMapper()
	{		
		new MapDriver<Writable, Text, Writable, Text>()
			.withMapper(new SquaredFreqMapper())
			.withInput(new Text("0,0"), pairRecords)
			.withOutput(new Text("0,0"), new Text("8"))
			.runTest();
	}


}
