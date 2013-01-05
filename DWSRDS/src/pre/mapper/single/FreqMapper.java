package pre.mapper.single;

import java.math.BigInteger;

public class FreqMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		return new BigInteger("2").pow(items.length);
	}
}
