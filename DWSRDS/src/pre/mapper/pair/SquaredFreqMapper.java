package pre.mapper.pair;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import setting.NAMES;

public class SquaredFreqMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(Set<T> items1, Set<T> items2)
	{
		Set<T> intersect = new HashSet<T>(items1);
		intersect.retainAll(items2);
		
		if (intersect.size() == 0)
			return BigInteger.ZERO;
		
		return new BigInteger("2").pow(intersect.size());
	}

	@Override
	protected Path getSecondFilePath(Context context)
	{
		//context.getConfiguration().get("secondFilePath");
		return new Path(context.getConfiguration().get(NAMES.ORI_FILE_1.toString()));
	}

}
