package pre.mapper.pair;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import pre.mapper.PreMapper;
import setting.PARAMETERS;

public abstract class AbstractPairMapper extends PreMapper
{
	private Text OnlyKey = new Text("1");	// dummy key. to avoid sorting
	
	private Path secondFilePath;
	private Map<String, Set<String>> leftRecords;		// <index, items>
	
	protected abstract <T> BigInteger calcWeight(Set<T> items1, Set<T> items2);
	protected abstract Path getSecondFilePath(Context context);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// FIXME: single input file required
		secondFilePath = getSecondFilePath(context); 
		leftRecords = new HashMap<String, Set<String>>();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String [] items = value.toString().split(PARAMETERS.SeparatorItem);
		leftRecords.put(key.toString(), new HashSet<String>(Arrays.asList(items)));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream fsDataInputStream = fs.open(secondFilePath);

		String line = null;
		// TODO: using FSDataInputStream.readLine() is deprecated.
		long pos = fsDataInputStream.getPos();
		Set<String> rightRecord = new HashSet<String>();
		while ((line = fsDataInputStream.readLine()) != null)		// how to keep line offset
		{
			rightRecord.addAll(Arrays.asList(line.split(PARAMETERS.SeparatorItem)));	
			
			for (Map.Entry<String, Set<String>> indexitemspair : leftRecords.entrySet())
			{
				BigInteger weight = calcWeight(indexitemspair.getValue(), rightRecord);
				
				if (weight.compareTo(BigInteger.ZERO) == 0)	// if weight == 0, skip this item
					continue;
				
				String emitKey = indexitemspair.getKey() + PARAMETERS.SeparatorIndex + String.valueOf(pos);
				
				context.write(OnlyKey, new Text(emitKey + PARAMETERS.SeparatorIndexWeight + weight.toString()));
			}
			
			context.progress();	// report progress
			
			pos = fsDataInputStream.getPos();
			rightRecord.clear();
		}
	}
}
