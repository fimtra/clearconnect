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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import com.fimtra.datafission.IObserverContext;
import com.fimtra.datafission.IObserverContext.ISystemRecordNames;
import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IRecordListener;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.datafission.ui.RecordTableUtils.ICellUpdateHandler;
import com.fimtra.util.Pair;

/**
 * A {@link TableModel} implementation that can be attached as an {@link IRecordListener} for any
 * number of records. Models fields as table rows so records are displayed in a column-oriented
 * manner.
 * 
 * @see ColumnOrientedRecordTable
 * @author Ramon Servadei
 */
public final class ColumnOrientedRecordTableModel extends AbstractTableModel implements IRecordListener,
    RecordTableUtils.ICoalescedUpdatesHandler
{
    private static final long serialVersionUID = 1L;

    final ConcurrentMap<String, IRecordListener> recordRemovedListeners;
    final Map<Pair<String, String>, Integer> recordIndexByName;
    final List<IRecord> records;
    final List<String> fieldIndexes;
    final Map<String, AtomicInteger> fieldIndexLookupMap;
    ICellUpdateHandler cellUpdateHandler = new ICellUpdateHandler()
    {
        @Override
        public void cellUpdated(int row, int column)
        {
            // noop
        }
    };

    // these members handle batching up of updates and removes
    final AtomicBoolean batchUpdateScheduled;

    /**
     * Record updates keyed by {recordname,context}
     * 
     * @see {@link RowOrientedRecordTableModel#getRecordLookupKey(String, String)}
     */
    final Map<Pair<String, String>, IRecord> pendingBatchUpdates;
    /**
     * Coalesced atomic updates keyed by {recordname,context}
     * 
     * @see {@link RowOrientedRecordTableModel#getRecordLookupKey(String, String)}
     */
    final Map<Pair<String, String>, IRecordChange> pendingBatchAtomicChanges;

    public ColumnOrientedRecordTableModel()
    {
        this.recordIndexByName = new HashMap<>();
        this.records = new ArrayList<>();
        this.fieldIndexes = new ArrayList<>();
        this.fieldIndexLookupMap = new HashMap<>();
        this.recordRemovedListeners = new ConcurrentHashMap<>();

        this.batchUpdateScheduled = new AtomicBoolean();
        this.pendingBatchUpdates = new HashMap<>();
        this.pendingBatchAtomicChanges = new HashMap<>();

        final ArrayList<Integer> inserts = new ArrayList<>();
        checkAddFieldRow(RecordTableUtils.CONTEXT, inserts);
        if (inserts.size() > 0)
        {
            fireTableRowsInserted(inserts.get(0).intValue(), inserts.get(inserts.size() - 1).intValue());
        }

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
        RecordTableUtils.coalesceAndSchedule(this, imageCopyValidInCallingThreadOnly, atomicChange,
            this.batchUpdateScheduled, this.pendingBatchUpdates, this.pendingBatchAtomicChanges);
    }

    @Override
    public void handleCoalescedUpdates(Map<Pair<String, String>, IRecord> recordImages,
        Map<Pair<String, String>, IRecordChange> recordAtomicChanges)
    {
        final Set<String> fieldsToDelete = new HashSet<>();
        final List<Integer> inserts = new ArrayList<>();

        Map.Entry<Pair<String, String>, IRecord> entry = null;
        Pair<String, String> nameAndContext = null;
        IRecord imageCopy = null;
        int rowIndex = 0;
        IRecordChange atomicChange;
        int columnIndex;
        // we do +1 because the table always shows the field names at index 0
        int column_plus1;
        Integer index;
        for (Map.Entry<Pair<String, String>, IRecord> pairIRecordEntry : recordImages.entrySet())
        {
            entry = pairIRecordEntry;
            nameAndContext = entry.getKey();
            imageCopy = entry.getValue();

            index = this.recordIndexByName.get(nameAndContext);
            if (index == null)
            {
                // its a new record (and a new column for the record must be added)
                columnIndex = ColumnOrientedRecordTableModel.this.records.size();
                column_plus1 = columnIndex + 1;
                ColumnOrientedRecordTableModel.this.records.add(imageCopy);
                ColumnOrientedRecordTableModel.this.recordIndexByName.put(
                        RowOrientedRecordTableModel.getRecordLookupKey(imageCopy),
                        Integer.valueOf(columnIndex));
                fireTableStructureChanged();

                // as its a new record, we need to add the fields (some might be new)
                for (String key : imageCopy.keySet())
                {
                    rowIndex = checkAddFieldRow(key, inserts);
                    cellUpdated(rowIndex, column_plus1);
                }
                for (String key : imageCopy.getSubMapKeys())
                {
                    rowIndex = checkAddFieldRow(key, inserts);
                    cellUpdated(rowIndex, column_plus1);
                }
                if (inserts.size() > 0)
                {
                    fireTableRowsInserted(inserts.get(0).intValue(),
                            inserts.get(inserts.size() - 1).intValue());
                    inserts.clear();
                }
            }
            else
            {
                columnIndex = index.intValue();
                column_plus1 = columnIndex + 1;

                // an update to an existing record
                atomicChange = recordAtomicChanges.get(nameAndContext);
                ColumnOrientedRecordTableModel.this.records.set(columnIndex, imageCopy);

                // first handle removing any fields (rows)
                removeRows(fieldsToDelete, atomicChange.getRemovedEntries().keySet());

                // handle updates now
                // add any new rows first...
                for (String changedKey : atomicChange.getPutEntries().keySet())
                {
                    checkAddFieldRow(changedKey, inserts);
                }
                for (String changedKey : atomicChange.getSubMapKeys())
                {
                    checkAddFieldRow(changedKey, inserts);
                }
                if (inserts.size() > 0)
                {
                    fireTableRowsInserted(inserts.get(0).intValue(),
                            inserts.get(inserts.size() - 1).intValue());
                    inserts.clear();
                }

                // now flash changes
                for (String changedKey : atomicChange.getPutEntries().keySet())
                {
                    rowIndex = checkAddFieldRow(changedKey, inserts);
                    cellUpdated(rowIndex, column_plus1);
                    fireTableCellUpdated(rowIndex, column_plus1);
                }
                for (String changedKey : atomicChange.getSubMapKeys())
                {
                    rowIndex = checkAddFieldRow(changedKey, inserts);
                    cellUpdated(rowIndex, column_plus1);
                    fireTableCellUpdated(rowIndex, column_plus1);
                }
            }
        }
    }

    void removeRows(final Set<String> fieldsToDelete, final Set<String> deletedFields)
    {
        int rowIndex;
        boolean exists;
        for (String removedKey : deletedFields)
        {
            exists = false;
            // check if the field that has been removed exists in any other records
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
                fieldsToDelete.add(removedKey);
            }
        }
        if (fieldsToDelete.size() > 0)
        {
            // process deletes in a batch
            rowIndex =
                RecordTableUtils.deleteIndexedFields(fieldsToDelete, this.fieldIndexes, this.fieldIndexLookupMap);
            // we can handle 1 row delete, anymore and we call structure changed
            if (rowIndex > -1)
            {
                fireTableRowsDeleted(rowIndex, rowIndex);
            }
            else
            {
                fireTableDataChanged();
            }
        }
    }

    public void recordUnsubscribed(final String recordName, final String contextName)
    {
        SwingUtilities.invokeLater(() -> {
            final Integer index =
                ColumnOrientedRecordTableModel.this.recordIndexByName.remove(RowOrientedRecordTableModel.getRecordLookupKey(
                    recordName, contextName));
            if (index != null)
            {
                final IRecord removed = ColumnOrientedRecordTableModel.this.records.remove(index.intValue());
                removeRows(new HashSet<>(), removed.keySet());

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
        });
    }

    public void recordUnsubscribed(IRecord record)
    {
        recordUnsubscribed(record.getName(), record.getContextName());
    }

    void setCellUpdatedHandler(ICellUpdateHandler recordTable)
    {
        this.cellUpdateHandler = recordTable;
    }

    void cellUpdated(int row, int column)
    {
        this.cellUpdateHandler.cellUpdated(row, column);
    }

    int checkAddFieldRow(String fieldName, List<Integer> inserts)
    {
        AtomicInteger index = this.fieldIndexLookupMap.get(fieldName);
        if (index == null)
        {
            this.fieldIndexes.add(fieldName);
            index = new AtomicInteger(this.fieldIndexes.size() - 1);
            this.fieldIndexLookupMap.put(fieldName, index);
            inserts.add(Integer.valueOf(index.intValue()));
        }
        return index.intValue();
    }

    int getRowIndexForFieldName(String fieldName)
    {
        final AtomicInteger index = this.fieldIndexLookupMap.get(fieldName);
        if (index == null)
        {
            throw new NullPointerException("No index for field " + fieldName);
        }
        return index.intValue();
    }

    public IRecord getRecord(int selectedColumn)
    {
        return this.records.get(selectedColumn - 1);
    }

}
