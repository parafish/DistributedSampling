package pre.mapper.pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import setting.NAMES;

public class DiscriminitivityMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(Set<T> positive, Set<T> negative)
	{
		Set<T> intersect = new HashSet<T> (positive);
		intersect.retainAll(negative);
		
		Set<T> complement = new HashSet<T>(positive);
		complement.removeAll(negative);
		
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
