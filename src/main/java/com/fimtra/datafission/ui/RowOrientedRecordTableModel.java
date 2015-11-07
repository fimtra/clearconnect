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

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.RowSorter.SortKey;
import javax.swing.SortOrder;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
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
import com.fimtra.util.ThreadUtils;

/**
 * A {@link TableModel} implementation that can be attached as an {@link IRecordListener} for any
 * number of records. Models fields as table columns so records are displayed in a row-oriented
 * manner.
 * 
 * @see RowOrientedRecordTable
 * @author Ramon Servadei
 */
public class RowOrientedRecordTableModel extends AbstractTableModel implements IRecordListener
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
    final Set<String> fieldNames;
    RowOrientedRecordTable recordTable;

    public RowOrientedRecordTableModel()
    {
        this.recordIndexByName = new HashMap<Pair<String, String>, Integer>();
        this.records = new ArrayList<IRecord>();
        this.fieldIndexes = new ArrayList<String>();
        this.fieldNames = new HashSet<String>();
        this.recordRemovedListeners = new ConcurrentHashMap<String, IRecordListener>();
        checkAddColumn(RecordTableUtils.NAME);
        checkAddColumn(RecordTableUtils.CONTEXT);
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
        final IRecord imageCopy = ImmutableSnapshotRecord.create(imageCopyValidInCallingThreadOnly);
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                int rowIndex = getRowIndexForRecord(imageCopy);
                if (rowIndex == -1)
                {
                    // its new
                    rowIndex = RowOrientedRecordTableModel.this.records.size();
                    RowOrientedRecordTableModel.this.records.add(imageCopy);
                    RowOrientedRecordTableModel.this.recordIndexByName.put(getRecordLookupKey(imageCopy),
                        Integer.valueOf(rowIndex));
                    for (String key : imageCopy.keySet())
                    {
                        checkAddColumn(key);
                        cellUpdated(rowIndex, getColumnIndexForColumnName(key));
                    }
                    for (String key : imageCopy.getSubMapKeys())
                    {
                        checkAddColumn(key);
                        cellUpdated(rowIndex, getColumnIndexForColumnName(key));
                    }
                    fireTableRowsInserted(rowIndex, rowIndex);
                }
                else
                {
                    RowOrientedRecordTableModel.this.records.set(rowIndex, imageCopy);

                    for (String removedKey : atomicChange.getRemovedEntries().keySet())
                    {
                        boolean exists = false;
                        for (IRecord record : RowOrientedRecordTableModel.this.records)
                        {
                            if (record.keySet().contains(removedKey))
                            {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists)
                        {
                            deleteColumn(removedKey);
                        }
                    }

                    int columnIndex;
                    for (String changedKey : atomicChange.getPutEntries().keySet())
                    {
                        checkAddColumn(changedKey);
                        columnIndex = getColumnIndexForColumnName(changedKey);
                        cellUpdated(rowIndex, columnIndex);
                        fireTableCellUpdated(rowIndex, columnIndex);
                    }

                    // todo what happens if a sub-map is removed?
                    // flash updates for submap keys
                    for (String changedKey : atomicChange.getSubMapKeys())
                    {
                        checkAddColumn(changedKey);
                        columnIndex = getColumnIndexForColumnName(changedKey);
                        cellUpdated(rowIndex, columnIndex);
                        fireTableCellUpdated(rowIndex, columnIndex);
                    }
                }
                // this may not be the most efficient way but the auto-sorting on update
                // does not appear to work
                // todo huge perf hit as the table row count increases
                // RowOrientedRecordTableModel.this.recordTable.getRowSorter().allRowsChanged();
            }
        });
    }

    public void recordUnsubscribed(final String recordName, final String contextName)
    {
        // todo this will hurt for lots of records deleted
        SwingUtilities.invokeLater(new Runnable()
        {
            @Override
            public void run()
            {
                final Integer index =
                    RowOrientedRecordTableModel.this.recordIndexByName.remove(getRecordLookupKey(recordName,
                        contextName));
                if (index != null)
                {
                    RowOrientedRecordTableModel.this.records.remove(index.intValue());
                    // rebuild indexes
                    RowOrientedRecordTableModel.this.recordIndexByName.clear();
                    for (int i = 0; i < RowOrientedRecordTableModel.this.records.size(); i++)
                    {
                        RowOrientedRecordTableModel.this.recordIndexByName.put(
                            getRecordLookupKey(RowOrientedRecordTableModel.this.records.get(i)), Integer.valueOf(i));
                    }
                    fireTableRowsDeleted(index.intValue(), index.intValue());
                }
            }
        });
    }

    public void recordUnsubscribed(IRecord record)
    {
        recordUnsubscribed(record.getName(), record.getContextName());
    }

    void addCellUpdatedListener(RowOrientedRecordTable recordTable)
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

    void checkAddColumn(String columnName)
    {
        if (this.fieldNames.add(columnName))
        {
            this.fieldIndexes.add(columnName);
            fireTableStructureChanged();
        }
    }

    void deleteColumn(String columnName)
    {
        if (this.fieldNames.remove(columnName))
        {
            this.fieldIndexes.remove(columnName);
            fireTableStructureChanged();
        }
    }

    int getRowIndexForRecord(IRecord imageCopy)
    {
        Integer index = this.recordIndexByName.get(getRecordLookupKey(imageCopy));
        if (index == null)
        {
            return -1;
        }
        return index.intValue();
    }

    int getColumnIndexForColumnName(String columnName)
    {
        return RowOrientedRecordTableModel.this.fieldIndexes.indexOf(columnName);
    }

    public IRecord getRecord(int selectedRow)
    {
        return this.records.get(selectedRow);
    }

}

/**
 * Encapsulates all common util components for record tables.
 * 
 * @author Ramon Servadei
 */
abstract class RecordTableUtils
{
    static final TextValue SUBMAP = TextValue.valueOf("SUB-MAP...");
    static final IValue BLANK = TextValue.valueOf("");
    static final ScheduledExecutorService cellUpdater = ThreadUtils.newScheduledExecutorService(
        "RecordTable-cellupdater", 1);
    static final Color UPDATE_COLOUR = new Color(255, 0, 0);
    static final long TTL = 250;
    static final String NAME = "Name";
    static final String CONTEXT = "Context";
    static final String FIELD = "Field";

    /**
     * A standard renderer for {@link IValue} objects
     * 
     * @author Ramon Servadei
     */
    static final class DefaultIValueCellRenderer extends DefaultTableCellRenderer
    {
        private static final long serialVersionUID = 1L;

        DefaultIValueCellRenderer()
        {
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
            boolean hasFocus, int row, int column)
        {
            final Component tableCellRendererComponent =
                super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
            setValue(((IValue) value).textValue());
            return tableCellRendererComponent;
        }
    }

    static class CellUpdate
    {
        final int row, col, hashCode;
        final long start;

        @Override
        public String toString()
        {
            return "Coord [row=" + this.row + ", col=" + this.col + "]";
        }

        CellUpdate(int row, int col)
        {
            super();
            this.row = row;
            this.col = col;
            this.start = System.currentTimeMillis();

            final int prime = 31;
            int hashCode = 1;
            hashCode = prime * hashCode + this.col;
            hashCode = prime * hashCode + this.row;
            this.hashCode = hashCode;
        }

        boolean isActive()
        {
            return System.currentTimeMillis() - this.start < RecordTableUtils.TTL;
        }

        @Override
        public int hashCode()
        {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CellUpdate other = (CellUpdate) obj;
            if (this.col != other.col)
                return false;
            if (this.row != other.row)
                return false;
            return true;
        }

    }

    private RecordTableUtils()
    {
    }

    /**
     * Shows a pop-up view of a sub-map. This does not update in real-time
     */
    static void showSnapshotSubMapData(MouseEvent evt, IRecord record, String subMapKey)
    {
        final JPopupMenu submapPopup = new JPopupMenu();
        Map<String, IValue> subMapData = record.getOrCreateSubMap(subMapKey);

        final DefaultTableModel model = new DefaultTableModel()
        {
            private static final long serialVersionUID = 1L;

            @Override
            public Class<?> getColumnClass(int columnIndex)
            {
                switch(columnIndex)
                {
                    case 0:
                        return String.class;
                }
                return super.getColumnClass(columnIndex);
            }
        };
        model.addColumn("Field");
        model.addColumn("Value");

        // populate the model
        Map.Entry<String, IValue> entry = null;
        String key = null;
        IValue value = null;
        for (Iterator<Map.Entry<String, IValue>> it = subMapData.entrySet().iterator(); it.hasNext();)
        {
            entry = it.next();
            key = entry.getKey();
            value = entry.getValue();
            model.addRow(new Object[] { key, value });
        }

        JTable table = new JTable(model);
        table.setDefaultRenderer(IValue.class, new RecordTableUtils.DefaultIValueCellRenderer());
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        TableRowSorterForStringWithNumbers sorter = new TableRowSorterForStringWithNumbers(model);
        table.setRowSorter(sorter);
        sorter.modelStructureChanged();

        // force sorting when opened
        List<SortKey> sortKeys = new ArrayList<SortKey>(1);
        sortKeys.add(new SortKey(0, SortOrder.ASCENDING));
        sorter.setSortKeys(sortKeys);
        sorter.sort();

        submapPopup.add(new JLabel("Submap: " + subMapKey));
        JButton closeButton = new JButton("Close");
        closeButton.addActionListener(new ActionListener()
        {
            @Override
            public void actionPerformed(ActionEvent e)
            {
                submapPopup.setVisible(false);
                submapPopup.removeAll();
            }
        });
        submapPopup.add(closeButton);
        submapPopup.add(new JScrollPane(table));
        submapPopup.setPreferredSize(new Dimension(450, 300));
        submapPopup.setLocation(evt.getLocationOnScreen());
        submapPopup.setVisible(true);
    }
}