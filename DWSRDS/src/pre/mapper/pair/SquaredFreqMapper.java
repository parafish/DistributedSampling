package pre.mapper.pair;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SquaredFreqMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(T [] items1, T [] items2)
	{
		Set<T> intersect = new HashSet<T>(Arrays.asList(items1));
		intersect.retainAll(new HashSet<T>(Arrays.asList(items2)));
				
		return new BigInteger("2").pow(intersect.size());
	}

}
