/*
 * Copyright (c) 2014 Ramon Servadei, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
 */
package com.fimtra.platform.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fimtra.datafission.IValue;
import com.fimtra.util.is;

/**
 * An expression processor performs the job of processing an {@link IExpression} against a record.
 * The processor is used by calling {@link #beginScan()} then
 * {@link #processRecordField(String, IValue)} (for each field to check) and finally
 * {@link #evalate()} to get the result.
 * <p>
 * <b>NOT THREAD SAFE</b>
 * <p>
 * Equal by comparison of the internal {@link IExpression}
 * 
 * @author Ramon Servadei
 */
public final class ExpressionProcessor
{
    /**
     * Add any {@link DataSignatureExpression} instances found in the expression to the list.
     */
    private static List<DataSignatureExpression> getDataSignatureExpressions(List<DataSignatureExpression> signatures,
        IExpression expr)
    {
        if (expr instanceof DataSignatureExpression)
        {
            signatures.add((DataSignatureExpression) expr);
        }
        else
        {
            final IExpression[] operands = expr.getOperands();
            for (int i = 0; i < operands.length; i++)
            {
                getDataSignatureExpressions(signatures, operands[i]);
            }
        }
        return signatures;
    }

    final IExpression expression;
    final List<String> scannedFields;
    final Map<String, List<DataSignatureExpression>> signatureExpressionsPerField;

    public ExpressionProcessor(IExpression expression)
    {
        this.expression = expression;
        final List<DataSignatureExpression> dataSignatureExpressions = new ArrayList<DataSignatureExpression>();
        getDataSignatureExpressions(dataSignatureExpressions, expression);

        this.scannedFields = new ArrayList<String>(dataSignatureExpressions.size());
        this.signatureExpressionsPerField =
            new HashMap<String, List<DataSignatureExpression>>(this.scannedFields.size());

        List<DataSignatureExpression> expressions;
        DataSignatureExpression dataSignatureExpression;
        for (int i = 0; i < dataSignatureExpressions.size(); i++)
        {
            dataSignatureExpression = dataSignatureExpressions.get(i);
            this.scannedFields.add(dataSignatureExpression.getFieldName());
            expressions = this.signatureExpressionsPerField.get(dataSignatureExpression.getFieldName());
            if (expressions == null)
            {
                expressions = new ArrayList<DataSignatureExpression>(1);
                this.signatureExpressionsPerField.put(dataSignatureExpression.getFieldName(), expressions);
            }
            expressions.add(dataSignatureExpression);
        }
    }

    public void beginScan()
    {
        for (List<DataSignatureExpression> dataSignatureExpressions : this.signatureExpressionsPerField.values())
        {
            for (int i = 0; i < dataSignatureExpressions.size(); i++)
            {
                dataSignatureExpressions.get(i).reset();
            }
        }
    }

    public void processRecordField(String fieldName, IValue value)
    {
        final List<DataSignatureExpression> dataSignatureExpressions = this.signatureExpressionsPerField.get(fieldName);
        if (dataSignatureExpressions != null)
        {
            for (int i = 0; i < dataSignatureExpressions.size(); i++)
            {
                dataSignatureExpressions.get(i).prepareResult(value);
            }
        }
    }

    public boolean evalate()
    {
        return this.expression.evaluate();
    }

    /**
     * @return the list of fields that form the data signature scan for the radar
     */
    public List<String> getScannedFields()
    {
        return Collections.unmodifiableList(this.scannedFields);
    }

    public void destroy()
    {
        this.scannedFields.clear();
        this.signatureExpressionsPerField.clear();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + this.expression + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.expression == null) ? 0 : this.expression.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (is.same(this, obj))
        {
            return true;
        }
        if (is.differentClass(this, obj))
        {
            return false;
        }
        final ExpressionProcessor other = (ExpressionProcessor) obj;
        return is.eq(this.expression, other.expression);
    }

}
