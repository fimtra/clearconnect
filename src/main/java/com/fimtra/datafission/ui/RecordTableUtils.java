/*
 * Copyright (c) 2015 Ramon Servadei 
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.RowSorter.SortKey;
import javax.swing.SortOrder;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IRecordChange;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.AtomicChange;
import com.fimtra.datafission.core.ImmutableSnapshotRecord;
import com.fimtra.datafission.field.TextValue;
import com.fimtra.util.Pair;
import com.fimtra.util.ThreadUtils;

/**
 * Encapsulates all common util components for record tables.
 * 
 * @author Ramon Servadei
 */
abstract class RecordTableUtils
{
    private static final int RECORD_UPDATE_PERIOD_MILLIS = 250;
    // double the update period to help ensure we don't update-delete-update
    static final int RECORD_DELETE_PERIOD_MILLIS = RECORD_UPDATE_PERIOD_MILLIS * 2;
    
    static final TextValue SUBMAP = TextValue.valueOf("SUB-MAP...");
    static final IValue BLANK = TextValue.valueOf("");
    static final ScheduledExecutorService cellUpdater = ThreadUtils.newScheduledExecutorService(
        "RecordTable-cellupdater", 1);
    static final Color UPDATE_COLOUR = new Color(255, 0, 0);
    static final long TTL = 250;
    static final String NAME = "Name";
    static final String CONTEXT = "Context";
    static final String FIELD = "Field";

    static interface ICellUpdateHandler
    {
        void cellUpdated(int row, int column);
    }

    static interface ICoalescedUpdatesHandler
    {
        void handleCoalescedUpdates(Map<Pair<String, String>, IRecord> recordImages,
            Map<Pair<String, String>, IRecordChange> recordAtomicChanges);
    }

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

    static void coalesceAndSchedule(final ICoalescedUpdatesHandler handler,
        final IRecord imageCopyValidInCallingThreadOnly, final IRecordChange atomicChange,
        final AtomicBoolean batchUpdateScheduled, final Map<Pair<String, String>, IRecord> pendingBatchUpdates,
        final Map<Pair<String, String>, IRecordChange> pendingBatchAtomicChanges)
    {
        final IRecord imageCopy = ImmutableSnapshotRecord.create(imageCopyValidInCallingThreadOnly);
        final Pair<String, String> key = RowOrientedRecordTableModel.getRecordLookupKey(imageCopy);

        synchronized (pendingBatchUpdates)
        {
            pendingBatchUpdates.put(key, imageCopy);
            IRecordChange change = pendingBatchAtomicChanges.get(key);
            if (change == null)
            {
                pendingBatchAtomicChanges.put(
                    key,
                    new AtomicChange(atomicChange.getName(), atomicChange.getPutEntries(),
                        atomicChange.getOverwrittenEntries(), atomicChange.getRemovedEntries()));
            }
            else
            {
                List<IRecordChange> subsequentChanges = new ArrayList<>(1);
                subsequentChanges.add(atomicChange);
                change.coalesce(subsequentChanges);
            }

            if (!batchUpdateScheduled.getAndSet(true))
            {
                cellUpdater.schedule(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        final Map<Pair<String, String>, IRecord> recordImages =
                            new HashMap<>();
                        final Map<Pair<String, String>, IRecordChange> recordAtomicChanges =
                            new HashMap<>();
                        synchronized (pendingBatchUpdates)
                        {
                            recordImages.putAll(pendingBatchUpdates);
                            recordAtomicChanges.putAll(pendingBatchAtomicChanges);

                            pendingBatchUpdates.clear();
                            pendingBatchAtomicChanges.clear();
                            batchUpdateScheduled.set(false);
                        }

                        SwingUtilities.invokeLater(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                handler.handleCoalescedUpdates(recordImages, recordAtomicChanges);
                            }
                        });
                    }
                }, RECORD_UPDATE_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
            }
        }
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
        List<SortKey> sortKeys = new ArrayList<>(1);
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

    /**
     * Given a list of Strings and a map holding the index of each string, this method removes the
     * strings and re-builds the index.
     * 
     * @param fieldsToDelete
     *            the fields to delete
     * @param fields
     *            the current fields to process
     * @param fieldIndexLookupMap
     *            the current index of each field in the fields list
     * @return the index of the removed field IF there is only ONE field to delete, otherwise -1.
     */
    static int deleteIndexedFields(Set<String> fieldsToDelete, List<String> fields,
        Map<String, AtomicInteger> fieldIndexLookupMap)
    {
        List<String> copy = new ArrayList<>(fields);
        fields.clear();

        int singleIndex = -1;
        if (fieldsToDelete.size() == 1)
        {
            final AtomicInteger index = fieldIndexLookupMap.get(fieldsToDelete.iterator().next());
            if (index != null)
            {
                singleIndex = index.intValue();
            }
        }

        // first rebuild fieldIndex without the deleted fields
        final int size = copy.size();
        String fieldName;
        int i;
        for (i = 0; i < size; i++)
        {
            fieldName = copy.get(i);
            if (!fieldsToDelete.remove(fieldName))
            {
                fields.add(fieldName);
            }
        }

        // rebuild indexes
        fieldIndexLookupMap.clear();
        for (i = 0; i < fields.size(); i++)
        {
            fieldIndexLookupMap.put(fields.get(i), new AtomicInteger(i));
        }

        return singleIndex;
    }
}