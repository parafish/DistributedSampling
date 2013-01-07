package pre.mapper.pair;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import pre.mapper.PreMapper;

import setting.PARAMETERS;

public abstract class AbstractPairMapper extends PreMapper
{
	private Text OnlyKey = new Text("1");	// dummy key. to avoid sorting
	
	private Path secondFilePath;
	private Map<String, String []> leftRecords;
	
	protected abstract <T> BigInteger calcWeight(T [] items1, T [] items2);
	protected abstract Path getSecondFilePath(Context context);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// FIXME: single input file required
		secondFilePath = getSecondFilePath(context); 
		leftRecords = new HashMap<String, String []>();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		leftRecords.put(key.toString(), value.toString().split(PARAMETERS.SeparatorItem));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream fsDataInputStream = fs.open(secondFilePath);

		String line = null;
		// TODO: using FSDataInputStream.readLine() is deprecated.
		long pos = fsDataInputStream.getPos();
		while ((line = fsDataInputStream.readLine()) != null)		// how to keep line offset
		{
			String [] rightRecord = line.split(PARAMETERS.SeparatorItem);	
			for (Map.Entry<String, String []> pair : leftRecords.entrySet())
			{
				BigInteger weight = calcWeight(pair.getValue(), rightRecord);
				String emitKey = pair.getKey() + PARAMETERS.SeparatorIndex + String.valueOf(pos);	 // TODO: change separator
				
				context.write(OnlyKey, new Text(emitKey + PARAMETERS.SeparatorIndexWeight + weight.toString()));
			}
			pos = fsDataInputStream.getPos();
		}
	}
}
