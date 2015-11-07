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

import java.awt.Component;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.TableColumnModelEvent;
import javax.swing.event.TableColumnModelListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.swing.table.TableRowSorter;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.core.Context;
import com.fimtra.util.Log;
import com.fimtra.util.StringWithNumbersComparator;
import com.fimtra.util.ThreadUtils;

/**
 * A basic JTable that provides UI updates when record fields change. The record is displayed in a
 * column-oriented manner with fields displayed as rows. This does not auto-resize columns nor does
 * it provide sorting on columns.
 * 
 * @author Ramon Servadei
 */
public class ColumnOrientedRecordTable extends JTable
{
    public static void main(String[] args)
    {
        ColumnOrientedRecordTableModel model = new ColumnOrientedRecordTableModel();
        ColumnOrientedRecordTable table = new ColumnOrientedRecordTable(model);

        final Context c = new Context("RecordPivotTable");
        final IRecord record1 = c.createRecord("record1");
        record1.put("field1", 1);
        record1.put("field2", "field2 value");
        c.publishAtomicChange(record1);

        final IRecord record2 = c.createRecord("record2");
        record2.put("field1", "rec2");
        c.publishAtomicChange(record2);

        final IRecord record3 = c.createRecord("record3");
        record3.put("field1", "rec3");
        c.publishAtomicChange(record3);

        final IRecord record4 = c.createRecord("record4");
        record4.put("field1", "rec4");
        c.publishAtomicChange(record4);

        final IRecord record5 = c.createRecord("record5");
        record5.put("field1", "rec5");
        c.publishAtomicChange(record5);

        c.addObserver(model, record1.getName(), record2.getName(), record3.getName(), record4.getName(),
            record5.getName());

        JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.getContentPane().add(new JScrollPane(table));
        frame.pack();
        frame.setVisible(true);

        final ScheduledExecutorService service = ThreadUtils.newScheduledExecutorService("sdf", 1);
        service.scheduleWithFixedDelay(new Runnable()
        {

            @Override
            public void run()
            {
                record1.put("time", new Date().toString());
                record2.put("time", new Date().toString());
                record3.put("time", new Date().toString());
                c.publishAtomicChange(record1);
                c.publishAtomicChange(record2);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static final long serialVersionUID = 1L;

    final Map<RecordTableUtils.CellUpdate, RecordTableUtils.CellUpdate> updates;

    public ColumnOrientedRecordTable(ColumnOrientedRecordTableModel model)
    {
        super(model);
        this.updates = new HashMap<RecordTableUtils.CellUpdate, RecordTableUtils.CellUpdate>();
        setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        setRowSorter(new TableRowSorterForStringWithNumbers(getModel()));
        setDefaultRenderer(IValue.class, new RecordTableUtils.DefaultIValueCellRenderer());
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        // add the mouse double-click listener to display sub-map data
        addMouseListener(new MouseAdapter()
        {
            @Override
            public void mouseClicked(MouseEvent evt)
            {
                if (evt.getClickCount() > 1)
                {
                    int row = rowAtPoint(evt.getPoint());
                    int col = columnAtPoint(evt.getPoint());
                    if (row >= 0 && col >= 0)
                    {
                        if (RecordTableUtils.SUBMAP.equals(getValueAt(row, col)))
                        {
                            RecordTableUtils.showSnapshotSubMapData(evt, getModel().records.get(col - 1),
                                getModel().fieldIndexes.get(convertRowIndexToModel(row)));
                        }
                    }
                }
            }
        });
    }

    public void fromStateString(String stateString)
    {
        try
        {
            final Map<String, Integer> widths = new HashMap<String, Integer>();
            getTableHeader().getColumnModel().addColumnModelListener(new TableColumnModelListener()
            {

                @Override
                public void columnSelectionChanged(ListSelectionEvent e)
                {
                }

                @Override
                public void columnRemoved(TableColumnModelEvent e)
                {
                }

                @Override
                public void columnMoved(TableColumnModelEvent e)
                {
                }

                @Override
                public void columnMarginChanged(ChangeEvent e)
                {
                }

                @Override
                public void columnAdded(TableColumnModelEvent e)
                {
                    final TableColumn column = getTableHeader().getColumnModel().getColumn(e.getToIndex());
                    final Integer preferredWidth = widths.get(column.getHeaderValue());
                    if (preferredWidth != null)
                    {
                        column.setPreferredWidth(preferredWidth.intValue());
                    }
                }
            });

            final String[] tokens = stateString.split(",");
            for (int i = 0; i < tokens.length; i++)
            {
                widths.put(tokens[i++], Integer.valueOf(tokens[i]));
            }
        }
        catch (Exception e)
        {
            Log.log(this, "Could not resolve columns from '", stateString, "'");
        }
    }

    public String toStateString()
    {
        StringBuilder sb = new StringBuilder();
        final int columnCount = getColumnModel().getColumnCount();
        TableColumn column;
        for (int i = 0; i < columnCount; i++)
        {
            column = getColumnModel().getColumn(i);
            if (i > 0)
            {
                sb.append(",");
            }
            sb.append(column.getIdentifier()).append(",").append(column.getWidth());
        }
        return sb.toString();
    }

    /**
     * Get the {@link IRecord} for the selected column
     * 
     * @return the selected column's record or <code>null</code> if no column is selected
     */
    public IRecord getSelectedRecord()
    {
        final int selectedColumn = getSelectedColumn();
        if (selectedColumn == -1)
        {
            return null;
        }
        return getModel().getRecord(convertColumnIndexToModel(selectedColumn));
    }

    @Override
    public ColumnOrientedRecordTableModel getModel()
    {
        return (ColumnOrientedRecordTableModel) super.getModel();
    }

    @Override
    public void setModel(TableModel dataModel)
    {
        if (dataModel instanceof ColumnOrientedRecordTableModel)
        {
            ((ColumnOrientedRecordTableModel) dataModel).addCellUpdatedListener(this);
            super.setModel(dataModel);
        }
        else
        {
            throw new IllegalArgumentException("Only supports table models of type: "
                + RowOrientedRecordTableModel.class);
        }
    }

    @Override
    public Component prepareRenderer(TableCellRenderer renderer, final int row, final int column)
    {
        final Component prepareRenderer = super.prepareRenderer(renderer, row, column);
        final int convertedRowIndex = convertRowIndexToModel(row);
        final int convertedColumnIndex = convertColumnIndexToModel(column);
        final RecordTableUtils.CellUpdate key =
            new RecordTableUtils.CellUpdate(convertedRowIndex, convertedColumnIndex);
        final RecordTableUtils.CellUpdate cellUpdate = this.updates.get(key);
        if (cellUpdate != null && cellUpdate.isActive())
        {
            prepareRenderer.setBackground(RecordTableUtils.UPDATE_COLOUR);
            RecordTableUtils.cellUpdater.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    SwingUtilities.invokeLater(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            // trigger another render update to clear the update cell background
                            if (convertedRowIndex < getRowCount())
                            {
                                ((AbstractTableModel) ColumnOrientedRecordTable.this.getModel()).fireTableCellUpdated(
                                    convertedRowIndex, convertedColumnIndex);
                            }
                        }
                    });
                }
            }, 500, TimeUnit.MILLISECONDS);
        }
        else
        {
            if (cellUpdate != null)
            {
                this.updates.remove(key);
            }
            if (getSelectedRow() == row)
            {
                prepareRenderer.setBackground(getSelectionBackground());
            }
            else
            {
                prepareRenderer.setBackground(null);
            }
        }
        
        final Object valueAt = getValueAt(row, column);
        if (valueAt instanceof IValue)
        {
            ((JComponent) prepareRenderer).setToolTipText(((IValue) valueAt).textValue() + " ("
                + ((IValue) valueAt).getType() + ")");
        }
        else
        {
            ((JComponent) prepareRenderer).setToolTipText(valueAt.toString());
        }
        
        return prepareRenderer;
    }

    void cellUpdated(int row, int column)
    {
        final RecordTableUtils.CellUpdate coord = new RecordTableUtils.CellUpdate(row, column);
        this.updates.put(coord, coord);
    }
}

/**
 * Sorts strings with numbers correctly
 * 
 * @author Ramon Servadei
 */
final class TableRowSorterForStringWithNumbers extends TableRowSorter<TableModel>
{
    final StringWithNumbersComparator comparator = new StringWithNumbersComparator();

    TableRowSorterForStringWithNumbers(TableModel model)
    {
        super(model);
    }

    @Override
    public void modelStructureChanged()
    {
        super.modelStructureChanged();
        for (int i = 0; i < getModelWrapper().getColumnCount(); i++)
        {
            if (String.class.equals(getModel().getColumnClass(i)))
            {
                setComparator(i, this.comparator);
            }
        }
    }
}