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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
import com.fimtra.datafission.ui.RecordTableUtils.ICellUpdateHandler;
import com.fimtra.datafission.ui.RecordTableUtils.ICoalescedUpdatesHandler;
import com.fimtra.util.Log;
import com.fimtra.util.Pair;

/**
 * A {@link TableModel} implementation that can be attached as an {@link IRecordListener} for any
 * number of records. Models fields as table columns so records are displayed in a row-oriented
 * manner.
 * 
 * @see RowOrientedRecordTable
 * @author Ramon Servadei
 */
public final class RowOrientedRecordTableModel extends AbstractTableModel implements IRecordListener,
    ICoalescedUpdatesHandler
{
    private static final long serialVersionUID = 1L;

    static Pair<String, String> getRecordLookupKey(IRecord record)
    {
        return getRecordLookupKey(record.getName(), record.getContextName());
    }

    static Pair<String, String> getRecordLookupKey(String recordName, String contextName)
    {
        return new Pair<String, String>(contextName, recordName);
    }

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
    final AtomicBoolean batchRemoveScheduled;

    /**
     * Record updates keyed by {recordname,context}
     * 
     * @see {@link #getRecordLookupKey(String, String)}
     */
    final Map<Pair<String, String>, IRecord> pendingBatchUpdates;
    /**
     * Coalesced atomic updates keyed by {recordname,context}
     * 
     * @see {@link #getRecordLookupKey(String, String)}
     */
    final Map<Pair<String, String>, IRecordChange> pendingBatchAtomicChanges;
    /** list of records to remove identified by name and keyed per context */
    final Map<String, List<String>> pendingBatchRemoves;

    public RowOrientedRecordTableModel()
    {
        this.recordIndexByName = new HashMap<Pair<String, String>, Integer>();
        this.records = new ArrayList<IRecord>();
        this.fieldIndexes = new ArrayList<String>();
        this.fieldIndexLookupMap = new HashMap<String, AtomicInteger>();
        this.recordRemovedListeners = new ConcurrentHashMap<String, IRecordListener>();

        this.batchUpdateScheduled = new AtomicBoolean();
        this.batchRemoveScheduled = new AtomicBoolean();
        this.pendingBatchUpdates = new HashMap<Pair<String, String>, IRecord>();
        this.pendingBatchAtomicChanges = new HashMap<Pair<String, String>, IRecordChange>();
        this.pendingBatchRemoves = new HashMap<String, List<String>>();

        // add these by hand
        this.fieldIndexes.add(RecordTableUtils.NAME);
        this.fieldIndexLookupMap.put(RecordTableUtils.NAME, new AtomicInteger(this.fieldIndexes.size() - 1));
        this.fieldIndexes.add(RecordTableUtils.CONTEXT);
        this.fieldIndexLookupMap.put(RecordTableUtils.CONTEXT, new AtomicInteger(this.fieldIndexes.size() - 1));
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
                    if (removedRecords.size() > 0)
                    {
                        recordUnsubscribedBatch(new HashSet<String>(removedRecords), context.getName());
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
        return this.records.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.fieldIndexes.size();
    }

    @Override
    public String getColumnName(int column)
    {
        return this.fieldIndexes.get(column);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        if (columnIndex >= this.fieldIndexes.size())
        {
            return RecordTableUtils.BLANK;
        }

        final IRecord record = this.records.get(rowIndex);
        switch(columnIndex)
        {
            case 0:
                return record.getName();
            case 1:
                return record.getContextName();
        }
        String fieldName = this.fieldIndexes.get(columnIndex);
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

    @Override
    public Class<?> getColumnClass(int columnIndex)
    {
        switch(columnIndex)
        {
            case 0:
                return String.class;
            case 1:
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
        final Set<String> fieldsToDelete = new HashSet<String>();
        final AtomicBoolean stuctureChanged = new AtomicBoolean();

        int startInsert = -1;
        int endInsert = -1;
        Map.Entry<Pair<String, String>, IRecord> entry = null;
        Pair<String, String> nameAndContext = null;
        IRecord imageCopy = null;
        Integer index;
        int rowIndex = 0;
        int colIndex = 0;
        IRecordChange atomicChange;

        for (Iterator<Map.Entry<Pair<String, String>, IRecord>> it = recordImages.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            nameAndContext = entry.getKey();
            imageCopy = entry.getValue();

            index = this.recordIndexByName.get(nameAndContext);
            if (index == null)
            {
                // its new
                rowIndex = this.records.size();
                this.records.add(imageCopy);
                this.recordIndexByName.put(getRecordLookupKey(imageCopy), Integer.valueOf(rowIndex));
                for (String key : imageCopy.keySet())
                {
                    colIndex = checkAddColumn(key, stuctureChanged);
                    cellUpdated(rowIndex, colIndex);
                }
                for (String key : imageCopy.getSubMapKeys())
                {
                    colIndex = checkAddColumn(key, stuctureChanged);
                    cellUpdated(rowIndex, colIndex);
                }
                if (startInsert == -1)
                {
                    startInsert = rowIndex;
                    endInsert = rowIndex;
                }
                else
                {
                    endInsert = rowIndex;
                }
            }
            else
            {
                // an update
                rowIndex = index.intValue();
                this.records.set(rowIndex, imageCopy);
                atomicChange = recordAtomicChanges.get(nameAndContext);

                for (String removedKey : atomicChange.getRemovedEntries().keySet())
                {
                    boolean exists = false;
                    for (IRecord record : this.records)
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
                    RecordTableUtils.deleteIndexedFields(fieldsToDelete, this.fieldIndexes, this.fieldIndexLookupMap);
                    // NOTE: when a column is deleted, we need to process as a structure change
                    stuctureChanged.set(true);
                }

                for (String changedKey : atomicChange.getPutEntries().keySet())
                {
                    colIndex = checkAddColumn(changedKey, stuctureChanged);
                    cellUpdated(rowIndex, colIndex);
                    fireTableCellUpdated(rowIndex, colIndex);
                }

                // todo what happens if a sub-map is removed?
                // flash updates for submap keys
                for (String changedKey : atomicChange.getSubMapKeys())
                {
                    colIndex = checkAddColumn(changedKey, stuctureChanged);
                    cellUpdated(rowIndex, colIndex);
                    fireTableCellUpdated(rowIndex, colIndex);
                }
            }
        }

        if (stuctureChanged.get())
        {
            fireTableStructureChanged();
        }
        else
        {
            if (startInsert > -1)
            {
                try
                {
                    fireTableRowsInserted(startInsert, endInsert);
                }
                catch (IndexOutOfBoundsException e)
                {
                    Log.log(this, "Error with startInsert=" + startInsert + ", endInsert=" + endInsert, e);
                }
            }
        }
    }

    void recordUnsubscribedBatch(final Collection<String> recordNames, final String contextName)
    {
        synchronized (this.pendingBatchRemoves)
        {
            List<String> list = this.pendingBatchRemoves.get(contextName);
            if (list == null)
            {
                list = new ArrayList<String>(recordNames.size());
                this.pendingBatchRemoves.put(contextName, list);
            }
            list.addAll(recordNames);

            if (!this.batchRemoveScheduled.getAndSet(true))
            {
                RecordTableUtils.cellUpdater.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        scheduleHandlePendingRemoves();
                    }
                }, 250, TimeUnit.MILLISECONDS);
            }
        }
    }

    void scheduleHandlePendingRemoves()
    {
        final Map<String, List<String>> local = new HashMap<String, List<String>>();

        synchronized (this.pendingBatchRemoves)
        {
            local.putAll(this.pendingBatchRemoves);

            this.pendingBatchRemoves.clear();
            this.batchRemoveScheduled.set(false);
        }

        final Set<Pair<String, String>> nameAndContextToRemove = new HashSet<Pair<String, String>>();

        Map.Entry<String, List<String>> entry = null;
        String contextName = null;
        List<String> listOfRecordNames = null;
        int i;
        for (Iterator<Map.Entry<String, List<String>>> it = local.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            contextName = entry.getKey();
            listOfRecordNames = entry.getValue();
            for (i = 0; i < listOfRecordNames.size(); i++)
            {
                nameAndContextToRemove.add(getRecordLookupKey(listOfRecordNames.get(i), contextName));
            }
        }

        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                handlePendingRemoves(nameAndContextToRemove);
            }
        });
    }

    void handlePendingRemoves(final Set<Pair<String, String>> nameAndContextToRemove)
    {
        List<IRecord> copy = new ArrayList<IRecord>(this.records);
        this.records.clear();

        int singleIndex = -1;
        if (nameAndContextToRemove.size() == 1)
        {
            final Integer index = this.recordIndexByName.get(nameAndContextToRemove.iterator().next());
            if (index != null)
            {
                singleIndex = index.intValue();
            }
        }

        // first rebuild records without the deleted records
        final int size = copy.size();
        IRecord record;
        int i;
        for (i = 0; i < size; i++)
        {
            record = copy.get(i);
            if (!nameAndContextToRemove.remove(getRecordLookupKey(record)))
            {
                this.records.add(record);
            }
        }

        // rebuild indexes
        this.recordIndexByName.clear();
        for (i = 0; i < this.records.size(); i++)
        {
            this.recordIndexByName.put(getRecordLookupKey(this.records.get(i)), Integer.valueOf(i));
        }

        if (singleIndex > -1)
        {
            fireTableRowsDeleted(singleIndex, singleIndex);
        }
        else
        {
            fireTableDataChanged();
        }
    }

    public void recordUnsubscribed(IRecord record)
    {
        final Set<String> recordNames = new HashSet<String>();
        recordNames.add(record.getName());
        recordUnsubscribedBatch(recordNames, record.getContextName());
    }

    void setCellUpdatedHandler(ICellUpdateHandler recordTable)
    {
        this.cellUpdateHandler = recordTable;
    }

    void cellUpdated(int row, int column)
    {
        this.cellUpdateHandler.cellUpdated(row, column);
    }

    int checkAddColumn(String columnName, AtomicBoolean stuctureChanged)
    {
        AtomicInteger index = RowOrientedRecordTableModel.this.fieldIndexLookupMap.get(columnName);
        if (index == null)
        {
            this.fieldIndexes.add(columnName);

            index = new AtomicInteger(this.fieldIndexes.size() - 1);
            this.fieldIndexLookupMap.put(columnName, index);

            // remove the NAME and CONTEXT
            this.fieldIndexes.remove(0);
            this.fieldIndexes.remove(0);

            Collections.sort(this.fieldIndexes);

            // re-add NAME and CONTEXT
            this.fieldIndexes.add(0, RecordTableUtils.CONTEXT);
            this.fieldIndexes.add(0, RecordTableUtils.NAME);

            for (int i = 0; i < this.fieldIndexes.size(); i++)
            {
                this.fieldIndexLookupMap.get(this.fieldIndexes.get(i)).set(i);
            }
            stuctureChanged.set(true);
        }
        return index.intValue();
    }

    public IRecord getRecord(int selectedRow)
    {
        return this.records.get(selectedRow);
    }
}