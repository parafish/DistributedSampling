package pre.mapper.single;

import java.math.BigInteger;

public class FreqMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		int  exp = items.length;// > maxlength ? maxlength : items.length;
		return new BigInteger("2").pow(exp);
	}
}
