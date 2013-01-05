package pre.mapper.pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import setting.NAMES;

public class DiscriminitivityMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(T[] positive, T[] negative)
	{
		List<T> negList = new ArrayList<T>(Arrays.asList(negative));
		
		List<T> complement = new ArrayList<T>(Arrays.asList(positive));
		complement.removeAll(negList);
		
		List<T> intersect = new ArrayList<T>(Arrays.asList(positive));
		intersect.retainAll(negList);
		
		BigInteger firstPart = new BigInteger("2").pow(complement.size()).subtract(new BigInteger("1"));
		BigInteger secondPart = new BigInteger("2").pow(intersect.size());
		return firstPart.multiply(secondPart);
	}

	@Override
	protected Path getSecondFilePath(Context context)
	{
		return new Path(context.getConfiguration().get(NAMES.ORI_FILE_2.toString()));
	}

}
