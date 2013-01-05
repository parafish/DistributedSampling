package preprocess.singleweighter;

import java.math.BigInteger;

public class AreaFreqSingleMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		int length = items.length;
		return new BigInteger("2").pow(length-1).multiply(new BigInteger(String.valueOf(length)));
	}

}
