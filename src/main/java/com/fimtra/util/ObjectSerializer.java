/*
 * Copyright (c) 2016 Ramon Servadei
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.util;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.BlobValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.FieldTemplate.FieldTypeEnum;

/**
 * Serializes objects using an {@link IRecord} as the transfer technology. The direct members of the
 * object (and its super-classes) are translated into fields in the record. Transient members are
 * ignored. Members that are primitive types are translated into an appropriate record field type
 * (see {@link IValue} ). Non-primitive types are transferred as a {@link BlobValue}. The
 * {@link ObjectSerializer} provides an efficient serialization mechanism for objects that have
 * primitive types as the main population of members. Additionally, for objects that change
 * frequently, using the {@link ObjectSerializer} will mean that only changing fields are sent (as
 * opposed to the full serialized version of the object using standard java serialization).
 * <p>
 * <b>Classes must be public, non-inner types and have public no-arg constructors.<b/>
 * <p>
 * Internally this keeps a {@link ClassTemplate} per object class that is read/written. The
 * {@link ClassTemplate} instances (one per class) held internally by each instance are never
 * removed (except when an {@link ObjectSerializer} instance is garbage collected).
 * <p>
 * Each instance of an {@link ObjectSerializer} keeps references to the objects that are
 * read/written. An object reference can be removed by calling {@link #recordDeleted(IRecord)} or
 * when the {@link ObjectSerializer} instance is garbage collected.
 *
 * @author Ramon Servadei
 */
public final class ObjectSerializer
{
    private static final String CLASS_NAME = "className";
    private static final String CLASS_TEMPLATE = "classTemplate";

    private static String getObjectNameFromRecord(IRecord record)
    {
        return record.getContextName() + "." + record.getName();
    }

    private final Map<String, Object> refs;
    private final Map<Class<?>, ClassTemplate> classTemplates;

    public ObjectSerializer()
    {
        super();
        this.refs = new ConcurrentHashMap<>();
        this.classTemplates = new ConcurrentHashMap<>();
    }

    /**
     * Write the object member variables into the fields of the record. The same record for the
     * object should be used to ensure that a proper delta is created for subsequent writes of the
     * same object.
     *
     * @param o      the object to write
     * @param record the record to store the member attributes of the object
     */
    public void writeObject(Object o, IRecord record) throws Exception
    {
        if (!record.getSubMapKeys().contains(CLASS_TEMPLATE))
        {
            record.getOrCreateSubMap(CLASS_TEMPLATE).put(CLASS_NAME,
                    TextValue.valueOf(o.getClass().getCanonicalName()));
        }
        getClassTemplate(o).writeToRecord(o, record);
    }

    /**
     * Read an object whose member attributes are stored in the fields of the record.
     *
     * @param record the record holding the state of the object
     * @return the resolved object from the record
     */
    @SuppressWarnings("unchecked")
    public <T> T readObject(IRecord record) throws Exception
    {
        final Object result = getObjectReference(record);
        getClassTemplate(result).readFromRecord(result, record);
        return (T) result;
    }

    public void recordDeleted(IRecord record)
    {
        this.refs.remove(getObjectNameFromRecord(record));
    }

    private ClassTemplate getClassTemplate(Object result)
    {
        ClassTemplate classTemplate = this.classTemplates.get(result.getClass());
        if (classTemplate == null)
        {
            classTemplate = new ClassTemplate(result.getClass());
            this.classTemplates.put(result.getClass(), classTemplate);
        }
        return classTemplate;
    }

    private Object getObjectReference(IRecord record) throws Exception
    {
        final String name = getObjectNameFromRecord(record);
        Object object = this.refs.get(name);
        if (object == null)
        {
            synchronized (this.refs)
            {
                object = this.refs.get(name);
                if (object == null)
                {
                    final IValue classNameFieldValue =
                            record.getOrCreateSubMap(CLASS_TEMPLATE).get(CLASS_NAME);
                    if (classNameFieldValue != null)
                    {
                        object = Class.forName(classNameFieldValue.textValue()).newInstance();
                        this.refs.put(name, object);
                    }
                    else
                    {
                        throw new NullPointerException("No class name defined: " + record);
                    }
                }
            }
        }
        return object;
    }
}

/**
 * Generates and holds an array of {@link FieldTemplate} objects that represent all the fields of a
 * class and its super-classes.
 * <p>
 * <b>Note: a class template for a nested class is not supported.</b>
 *
 * @author Ramon Servadei
 */
final class ClassTemplate
{
    private static List<FieldTemplate> getFieldTemplates(Field[] declaredFields, int level)
    {
        List<FieldTemplate> result = new ArrayList<>(declaredFields.length);
        Class<?> type;
        for (Field field : declaredFields)
        {
            type = field.getType();
            if (!Modifier.isTransient(field.getModifiers()) && !Modifier.isStatic(field.getModifiers())
                    && !Modifier.isFinal(field.getModifiers()))
            {
                field.setAccessible(true);
                result.add(
                        new FieldTemplate(field, FieldTypeEnum.from(type), (field.getName() + "." + level)));
            }
        }
        return result;
    }

    final FieldTemplate[] fields;

    ClassTemplate(Class<?> clazz)
    {
        List<FieldTemplate> templates = new ArrayList<>();
        Class<?> c = clazz;
        int i = 0;
        do
        {
            if (c.getEnclosingClass() != null)
            {
                throw new UnsupportedOperationException("Cannot handle inner classes: " + c);
            }
            templates.addAll(getFieldTemplates(c.getDeclaredFields(), i++));
        }
        while ((c = c.getSuperclass()) != null);

        this.fields = templates.toArray(new FieldTemplate[templates.size()]);
    }

    public void readFromRecord(Object o, IRecord record) throws Exception
    {
        FieldTemplate fieldTemplate;
        for (int i = 0; i < this.fields.length; i++)
        {
            fieldTemplate = this.fields[i];
            fieldTemplate.type.readFromRecord(o, record, fieldTemplate.field, fieldTemplate.recFieldName);
        }
    }

    public void writeToRecord(Object o, IRecord record) throws Exception
    {
        FieldTemplate fieldTemplate;
        for (int i = 0; i < this.fields.length; i++)
        {
            fieldTemplate = this.fields[i];
            fieldTemplate.type.writeToRecord(o, record, fieldTemplate.field, fieldTemplate.recFieldName);
        }
    }

}

/**
 * Holds a {@link Field}, the record name for the field and its type. This allows a field for an
 * object reference to be correctly written and read.
 *
 * @author Ramon Servadei
 */
final class FieldTemplate
{
    enum FieldTypeEnum
    {
        BOOLEAN(Boolean.TYPE), LONG(Long.TYPE), INT(Integer.TYPE), SHORT(Short.TYPE), BYTE(Byte.TYPE), CHAR(
            Character.TYPE), DOUBLE(Double.TYPE), FLOAT(Float.TYPE), OBJECT(Object.class), TEXT(String.class);

        final Class<?> type;

        FieldTypeEnum(Class<?> type)
        {
            this.type = type;
        }

        public static FieldTypeEnum from(Class<?> type)
        {
            final FieldTypeEnum[] enums = values();
            for (int i = 0; i < enums.length; i++)
            {
                if (enums[i].type == type)
                {
                    return enums[i];
                }
            }
            return OBJECT;
        }

        void readFromRecord(Object o, IRecord record, Field field, String recFieldName) throws Exception
        {
            try
            {
                final IValue iValue = record.get(recFieldName);
                if (iValue == null)
                {
                    return;
                }

                switch(this)
                {
                    case TEXT:
                        field.set(o, iValue.textValue());
                        break;
                    case BOOLEAN:
                        field.setBoolean(o, iValue.longValue() == 1L);
                        break;
                    case BYTE:
                        field.setByte(o, (byte) iValue.longValue());
                        break;
                    case SHORT:
                        field.setShort(o, (short) iValue.longValue());
                        break;
                    case CHAR:
                        field.setChar(o, (char) iValue.longValue());
                        break;
                    case INT:
                        field.setInt(o, (int) iValue.longValue());
                        break;
                    case LONG:
                        field.setLong(o, iValue.longValue());
                        break;
                    case DOUBLE:
                        field.setDouble(o, iValue.doubleValue());
                        break;
                    case FLOAT:
                        field.setFloat(o, (float) iValue.doubleValue());
                        break;
                    case OBJECT:
                        field.set(o, SerializationUtils.fromByteArray(iValue.byteValue()));
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(field + ", record field=" + recFieldName, e);
            }
        }

        void writeToRecord(Object o, IRecord record, Field field, String recFieldName) throws Exception
        {
            try
            {
                final Object fieldObject = field.get(o);
                if (fieldObject == null)
                {
                    return;
                }

                switch(this)
                {
                    case TEXT:
                        record.put(recFieldName, String.valueOf(fieldObject));
                        break;
                    case BOOLEAN:
                        record.put(recFieldName, field.getBoolean(o) ? 1L : 0L);
                        break;
                    case BYTE:
                        record.put(recFieldName, field.getByte(o));
                        break;
                    case SHORT:
                        record.put(recFieldName, field.getShort(o));
                        break;
                    case CHAR:
                        record.put(recFieldName, field.getChar(o));
                        break;
                    case INT:
                        record.put(recFieldName, field.getInt(o));
                        break;
                    case LONG:
                        record.put(recFieldName, field.getLong(o));
                        break;
                    case FLOAT:
                        record.put(recFieldName, field.getFloat(o));
                        break;
                    case DOUBLE:
                        record.put(recFieldName, field.getDouble(o));
                        break;
                    case OBJECT:
                        record.put(recFieldName, BlobValue.toBlob((Serializable) fieldObject));
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(field + ", record field=" + recFieldName, e);
            }
        }

    }

    final FieldTypeEnum type;
    final Field field;
    final String recFieldName;

    FieldTemplate(Field field, FieldTypeEnum type, String recFieldName)
    {
        this.field = field;
        this.type = type;
        this.recFieldName = recFieldName;
    }
}