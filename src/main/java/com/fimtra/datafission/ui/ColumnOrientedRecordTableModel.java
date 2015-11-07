/*
 * Copyright (c) 2013 Ramon Servadei 
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
package com.fimtra.datafission.ui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.ImmutableSnapshotRecord;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.Pair;

/**
 * A {@link TableModel} implementation that can be attached as an {@link IRecordListener} for any
 * number of records. Models fields as table rows so records are displayed in a column-oriented
 * manner.
 * 
 * @see ColumnOrientedRecordTable
 * @author Ramon Servadei
 */
public class ColumnOrientedRecordTableModel extends AbstractTableModel implements IRecordListener
{
    private static final long serialVersionUID = 1L;

    final ConcurrentMap<String, IRecordListener> recordRemovedListeners;
    final Map<Pair<String, String>, Integer> recordIndexByName;
    final List<IRecord> records;
    final List<String> fieldIndexes;
    final Set<String> fieldNames;
    ColumnOrientedRecordTable recordTable;

    public ColumnOrientedRecordTableModel()
    {
        this.recordIndexByName = new HashMap<Pair<String, String>, Integer>();
        this.records = new ArrayList<IRecord>();
        this.fieldIndexes = new ArrayList<String>();
        this.fieldNames = new HashSet<String>();
        this.recordRemovedListeners = new ConcurrentHashMap<String, IRecordListener>();
        checkAddFieldRow(RecordTableUtils.CONTEXT);
    }

    /**
     * Listen for removed records in the context and remove from the model if they exist. This can
     * be called for multiple contexts
     * 
     * @param context
     *            the context
     */
    public void addRecordRemovedListener(final IObserverContext context)
    {
        if (!this.recordRemovedListeners.containsKey(context.getName()))
        {
            final IRecordListener observer = new IRecordListener()
            {
                @Override
                public void onChange(IRecord imageCopy, IRecordChange atomicChange)
                {
                    final Set<String> removedRecords = atomicChange.getRemovedEntries().keySet();
                    for (String removedRecordName : removedRecords)
                    {
                        recordUnsubscribed(removedRecordName, context.getName());
                    }
                }
            };
            this.recordRemovedListeners.put(context.getName(), observer);
            context.addObserver(observer, ISystemRecordNames.CONTEXT_RECORDS);
        }
    }

    /**
     * Call to remove a 'record removed listener' previously added to the context via
     * {@link #addRecordRemovedListener(IObserverContext)}
     * 
     * @param context
     */
    public void removeRecordRemovedListener(final IObserverContext context)
    {
        if (!this.recordRemovedListeners.containsKey(context.getName()))
        {
            context.removeObserver(this.recordRemovedListeners.remove(context.getName()),
                ISystemRecordNames.CONTEXT_RECORDS);
        }
    }

    @Override
    public int getRowCount()
    {
        return this.fieldIndexes.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.records.size() + 1;
    }

    @Override
    public String getColumnName(int column)
    {
        switch(column)
        {
            case 0:
                return RecordTableUtils.FIELD;
        }
        if ((column - 1) < this.records.size())
        {
            return this.records.get(column - 1).getName();
        }
        return "";
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        final String fieldName = this.fieldIndexes.get(rowIndex);
        if (columnIndex == 0)
        {
            return fieldName;
        }

        if ((columnIndex - 1) < this.records.size())
        {
            final IRecord record = this.records.get(columnIndex - 1);
            if (RecordTableUtils.CONTEXT.equals(fieldName))
            {
                return TextValue.valueOf(record.getContextName());
            }
            final IValue iValue = record.get(fieldName);
            if (iValue == null)
            {
                if (record.getSubMapKeys().contains(fieldName))
                {
                    return RecordTableUtils.SUBMAP;
                }
                return RecordTableUtils.BLANK;
            }
            return iValue;
        }
        return RecordTableUtils.BLANK;
    }

    @Override
    public Class<?> getColumnClass(int columnIndex)
    {
        switch(columnIndex)
        {
            case 0:
                return String.class;
        }
        return IValue.class;
    }

    @Override
    public void onChange(final IRecord imageCopyValidInCallingThreadOnly, final IRecordChange atomicChange)
    {
        final IRecord imageCopy = ImmutableSnapshotRecord.create(imageCopyValidInCallingThreadOnly);
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                int columnIndex = getColumnIndexForRecord(imageCopy);
                int column_plus1 = columnIndex + 1;
                if (columnIndex == -1)
                {
                    // its new
                    columnIndex = ColumnOrientedRecordTableModel.this.records.size();
                    ColumnOrientedRecordTableModel.this.records.add(imageCopy);
                    ColumnOrientedRecordTableModel.this.recordIndexByName.put(
                        RowOrientedRecordTableModel.getRecordLookupKey(imageCopy), Integer.valueOf(columnIndex));
                    for (String key : imageCopy.keySet())
                    {
                        checkAddFieldRow(key);
                        cellUpdated(getRowIndexForFieldName(key), column_plus1);
                    }
                    for (String key : imageCopy.getSubMapKeys())
                    {
                        checkAddFieldRow(key);
                        cellUpdated(getRowIndexForFieldName(key), column_plus1);
                    }
                    fireTableStructureChanged();
                }
                else
                {
                    ColumnOrientedRecordTableModel.this.records.set(columnIndex, imageCopy);

                    for (String removedKey : atomicChange.getRemovedEntries().keySet())
                    {
                        boolean exists = false;
                        for (IRecord record : ColumnOrientedRecordTableModel.this.records)
                        {
                            if (record.keySet().contains(removedKey))
                            {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists)
                        {
                            deleteFieldRow(removedKey);
                        }
                    }

                    int rowIndex;
                    for (String changedKey : atomicChange.getPutEntries().keySet())
                    {
                        checkAddFieldRow(changedKey);
                        rowIndex = getRowIndexForFieldName(changedKey);
                        cellUpdated(rowIndex, column_plus1);
                        fireTableCellUpdated(rowIndex, column_plus1);
                    }

                    // todo what happens if a sub-map is removed?
                    // flash updates for submap keys
                    for (String changedKey : atomicChange.getSubMapKeys())
                    {
                        checkAddFieldRow(changedKey);
                        rowIndex = getRowIndexForFieldName(changedKey);
                        cellUpdated(rowIndex, column_plus1);
                        fireTableCellUpdated(rowIndex, column_plus1);
                    }
                }
            }
        });
    }

    public void recordUnsubscribed(final String recordName, final String contextName)
    {
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                final Integer index =
                    ColumnOrientedRecordTableModel.this.recordIndexByName.remove(RowOrientedRecordTableModel.getRecordLookupKey(
                        recordName, contextName));
                if (index != null)
                {
                    ColumnOrientedRecordTableModel.this.records.remove(index.intValue());
                    // rebuild indexes
                    ColumnOrientedRecordTableModel.this.recordIndexByName.clear();
                    for (int i = 0; i < ColumnOrientedRecordTableModel.this.records.size(); i++)
                    {
                        ColumnOrientedRecordTableModel.this.recordIndexByName.put(
                            RowOrientedRecordTableModel.getRecordLookupKey(ColumnOrientedRecordTableModel.this.records.get(i)),
                            Integer.valueOf(i));
                    }
                    fireTableStructureChanged();
                }
            }
        });
    }

    public void recordUnsubscribed(IRecord record)
    {
        recordUnsubscribed(record.getName(), record.getContextName());
    }

    void addCellUpdatedListener(ColumnOrientedRecordTable recordTable)
    {
        this.recordTable = recordTable;
    }

    void cellUpdated(int row, int column)
    {
        if (this.recordTable != null)
        {
            this.recordTable.cellUpdated(row, column);
        }
    }

    void checkAddFieldRow(String fieldName)
    {
        if (this.fieldNames.add(fieldName))
        {
            this.fieldIndexes.add(fieldName);
            int index = getRowIndexForFieldName(fieldName);
            fireTableRowsInserted(index, index);
        }
    }

    void deleteFieldRow(String fieldName)
    {
        if (this.fieldNames.remove(fieldName))
        {
            int index = getRowIndexForFieldName(fieldName);
            this.fieldIndexes.remove(index);
            fireTableRowsDeleted(index, index);
        }
    }

    int getColumnIndexForRecord(IRecord imageCopy)
    {
        Integer index = this.recordIndexByName.get(RowOrientedRecordTableModel.getRecordLookupKey(imageCopy));
        if (index == null)
        {
            return -1;
        }
        return index.intValue();
    }

    int getRowIndexForFieldName(String fieldName)
    {
        return this.fieldIndexes.indexOf(fieldName);
    }

    public IRecord getRecord(int selectedColumn)
    {
        return this.records.get(selectedColumn - 1);
    }

}
