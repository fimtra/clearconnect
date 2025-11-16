package com.fimtra.datafission.field;

import static com.fimtra.datafission.DataFissionProperties.Values.USE_CLASSIC_DOUBLE_VALUE_CODEC;

import com.fimtra.datafission.IValue;
import com.fimtra.util.Log;
import com.fimtra.util.StringAppender;

/**
 * Adapter to switch between classic double codec and optimised codec logic.
 *
 * @author Ramon Servadei
 */
final class DoubleValueCodeAdapter
{
    private static final IDoubleValueCodec delegate =
            USE_CLASSIC_DOUBLE_VALUE_CODEC ? new ClassicDoubleValueCodec() :
                    new OptimisedDoubleValueCodecAdapter();

    static
    {
        Log.log(DoubleValueCodeAdapter.class, "IDoubleValueCodec=", delegate.getClass()
                .getSimpleName());
    }

    static double fromCharArray(char[] chars, int start, int len) throws NumberFormatException
    {
        return delegate.fromCharArray(chars, start, len);
    }

    static String textValue(double value)
    {
        return delegate.textValue(value);
    }

    static StringAppender appendTo(StringAppender stringAppender, double value)
    {
        return delegate.appendTo(stringAppender, value);
    }
}

interface IDoubleValueCodec
{
    double fromCharArray(char[] chars, int start, int len) throws NumberFormatException;

    String textValue(double value);

    StringAppender appendTo(StringAppender stringAppender, double value);
}

final class ClassicDoubleValueCodec implements IDoubleValueCodec
{
    @Override
    public double fromCharArray(char[] chars, int start, int len) throws NumberFormatException
    {
        return Double.parseDouble(new String(chars, start, len));
    }

    @Override
    public String textValue(double value)
    {
        return Double.toString(value);
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender, double value)
    {
        return stringAppender.append(IValue.DOUBLE_CODE)
                .append(value);
    }
}

final class OptimisedDoubleValueCodecAdapter implements IDoubleValueCodec
{
    @Override
    public double fromCharArray(char[] chars, int start, int len) throws NumberFormatException
    {
        return DoubleValueCodec.fromCharArray(chars, start, len);
    }

    @Override
    public String textValue(double value)
    {
        final StringAppender stringAppender = new StringAppender(28);
        DoubleValueCodec.writeToCharArray(value, stringAppender);
        return stringAppender.toString();
    }

    @Override
    public StringAppender appendTo(StringAppender stringAppender, double value)
    {
        final StringAppender appender = stringAppender.append(IValue.DOUBLE_CODE);
        DoubleValueCodec.writeToCharArray(value, appender);
        return stringAppender;
    }
}
