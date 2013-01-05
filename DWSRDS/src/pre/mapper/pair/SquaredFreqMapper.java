package pre.mapper.pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SquaredFreqMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(T[] items1, T[] items2)
	{
		List<T> leftRecord = new ArrayList<T>(Arrays.asList(items1));
		List<T> rightRecord = new ArrayList<T>(Arrays.asList(items2));
		
		leftRecord.retainAll(rightRecord);		// FIXME: change list to set!!!w
		
		return new BigInteger("2").pow(leftRecord.size());
	}

	@Override
	protected Path getSecondFilePath(Context context)
	{
		//context.getConfiguration().get("secondFilePath");
		return ((FileSplit)context.getInputSplit()).getPath();
	}

}
