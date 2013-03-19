package pre;

import java.math.BigInteger;

/**
 * Maps the off/set pairs to offset/weight pairs where weights are calculated as follows:
 * <blockquote>
 * w = |R| * 2^(|R|-1)
 * </blockquote> 
 * where |R| is the length of the record
 * @author zheyi
 *
 */
public class AreaFreqMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		int length = items.length;
		return new BigInteger("2").pow(length-1).multiply(new BigInteger(String.valueOf(length)));
	}

}
