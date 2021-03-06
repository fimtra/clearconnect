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

import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.TableColumnModelEvent;
import javax.swing.event.TableColumnModelListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;

import com.fimtra.datafission.IRecord;
import com.fimtra.datafission.IValue;
import com.fimtra.datafission.ui.RecordTableUtils.ICellUpdateHandler;
import com.fimtra.util.Log;

/**
 * A basic JTable that provides UI updates when record fields change. Records are displayed in a
 * row-oriented manner; each record is displayed on one row, fields displayed as columns of the row.
 * This does not auto-resize columns.
 * 
 * @author Ramon Servadei
 */
public class RowOrientedRecordTable extends JTable implements ICellUpdateHandler
{
    private static final long serialVersionUID = 1L;

    final Map<RecordTableUtils.CellUpdate, RecordTableUtils.CellUpdate> updates;

    public RowOrientedRecordTable(RowOrientedRecordTableModel model)
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
                            RecordTableUtils.showSnapshotSubMapData(evt,
                                getModel().records.get(convertRowIndexToModel(row)),
                                getModel().fieldIndexes.get(convertColumnIndexToModel(col)));
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
            final Map<String, Integer> widths = new HashMap<>();
            final String[] tokens = stateString.split(",");
            String columnName;
            for (int i = 0; i < tokens.length; i++)
            {
                columnName = tokens[i++];
                getModel().addColumnFromResolvedState(columnName);
                widths.put(columnName, Integer.valueOf(tokens[i]));
            }

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

            getModel().fireTableStructureChanged();
        }
        catch (Exception e)
        {
            Log.log(this, "Could not resolve columns from '", stateString, "'");
        }
    }

    public String toStateString()
    {
        StringBuilder sb = new StringBuilder();
        final TableColumnModel headerColumnModel = getTableHeader().getColumnModel();
        final int columnCount = headerColumnModel.getColumnCount();
        TableColumn column;
        for (int i = 0; i < columnCount; i++)
        {
            column = headerColumnModel.getColumn(i);
            if (i > 0)
            {
                sb.append(",");
            }
            sb.append(column.getIdentifier()).append(",").append(column.getWidth());
        }
        return sb.toString();
    }

    /**
     * Get the {@link IRecord} for the selected row
     * 
     * @return the selected row's record or <code>null</code> if no row is selected
     */
    public IRecord getSelectedRecord()
    {
        final int selectedRow = getSelectedRow();
        if (selectedRow == -1)
        {
            return null;
        }
        return getModel().getRecord(convertRowIndexToModel(selectedRow));
    }

    @Override
    public RowOrientedRecordTableModel getModel()
    {
        return (RowOrientedRecordTableModel) super.getModel();
    }

    @Override
    public void setModel(TableModel dataModel)
    {
        if (dataModel instanceof RowOrientedRecordTableModel)
        {
            ((RowOrientedRecordTableModel) dataModel).setCellUpdatedHandler(this);
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
            RecordTableUtils.cellUpdater.schedule(() -> SwingUtilities.invokeLater(() -> {
                // trigger another render update to clear the update cell background
                if (convertedRowIndex < getRowCount())
                {
                    ((AbstractTableModel) RowOrientedRecordTable.this.getModel()).fireTableCellUpdated(
                            convertedRowIndex, convertedColumnIndex);
                }
            }), 500, TimeUnit.MILLISECONDS);
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

    @Override
    public final void cellUpdated(int row, int column)
    {
        final RecordTableUtils.CellUpdate coord = new RecordTableUtils.CellUpdate(row, column);
        this.updates.put(coord, coord);
    }
}
