package preprocess.singleweighter;

import java.math.BigInteger;

public class FreqSingleMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		return new BigInteger("2").pow(items.length);
	}
}
