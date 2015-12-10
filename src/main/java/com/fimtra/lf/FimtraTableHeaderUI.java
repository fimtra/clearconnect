/*
 * Copyright (c) 2014 James Lupton, Ramon Servadei, Paul Mackinlay, Fimtra
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
package com.fimtra.lf;

import java.awt.Component;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.RowSorter;
import javax.swing.RowSorter.SortKey;
import javax.swing.SortOrder;
import javax.swing.SwingUtilities;
import javax.swing.event.MouseInputListener;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.UIResource;
import javax.swing.plaf.basic.BasicTableHeaderUI;
import javax.swing.table.TableCellRenderer;


/**
 * A table header UI that shows the header text in a tooltip.
 * 
 * @author James
 */
public class FimtraTableHeaderUI extends BasicTableHeaderUI {

	private int selectedColumn = -1;
    
	/**
	 * Creates a new UI object for the given component.
	 *
	 * @param h
	 *            component to create UI object for
	 * @return the UI object
	 */
	public static ComponentUI createUI(JComponent h) {
		return new FimtraTableHeaderUI();
	}

	@Override
	protected void installDefaults() {
		TableCellRenderer prevRenderer = this.header.getDefaultRenderer();
		if (prevRenderer instanceof UIResource) {
			this.header.setDefaultRenderer(new HeaderRenderer());
		}
		super.installDefaults();
	}
	
	@Override
    protected MouseInputListener createMouseInputListener() {
        return new LocalMouseInputHandler();
    }

    /**
     * @inheritDoc
     */
    @Override
    protected void rolloverColumnUpdated(int oldColumn, int newColumn) {
        header.repaint(header.getHeaderRect(oldColumn));
        header.repaint(header.getHeaderRect(newColumn));
    }
	
	private class LocalMouseInputHandler extends MouseInputHandler{
		
		@Override
		public void mousePressed(MouseEvent e) {
			super.mousePressed(e);
			int viewCol = header.columnAtPoint(e.getPoint());
			if(viewCol > -1){
				int col = header.getTable().convertColumnIndexToModel(viewCol);
				if(e.isControlDown() && selectedColumn == col){
					selectedColumn = -1;
				} else {
					selectedColumn = col;
				}
			}
		}
		
		@Override
        public void mouseClicked(MouseEvent e) {
            if (!header.isEnabled()) {
                return;
            }
            if (e.getClickCount() % 2 == 0 &&
                    SwingUtilities.isLeftMouseButton(e)) {
                JTable table = header.getTable();
                RowSorter sorter;
                if (table != null && (sorter = table.getRowSorter()) != null) {
                    int columnIndex = header.columnAtPoint(e.getPoint());
                    if (columnIndex != -1) {
                    	
                        columnIndex = table.convertColumnIndexToModel(
                                columnIndex);
                            List<SortKey> keys = new ArrayList<SortKey>(sorter.getSortKeys());
                            SortKey sortKey;
                            int sortIndex;
                            for (sortIndex = keys.size() - 1; sortIndex >= 0; sortIndex--) {
                                if (keys.get(sortIndex).getColumn() == columnIndex) {
                                    break;
                                }
                            }
                            if (sortIndex == -1) {
                                // Key doesn't exist
                                sortKey = new SortKey(columnIndex, SortOrder.ASCENDING);
                                keys.add(0, sortKey);
                            }
                            else if (sortIndex == 0) {
                                // It's the primary sorting key, toggle it
                            	SortKey key = keys.get(0);
                            	if (key.getSortOrder() == SortOrder.ASCENDING) {
                                    keys.set(0, new SortKey(key.getColumn(), SortOrder.DESCENDING));
                                } else if (key.getSortOrder() == SortOrder.DESCENDING) {
                                    keys.set(0, new SortKey(key.getColumn(), SortOrder.UNSORTED));
                                } else {
                                	keys.set(0, new SortKey(key.getColumn(), SortOrder.ASCENDING));
                                }
                            }
                            else {
                                // It's not the first, but was sorted on, remove old
                                // entry, insert as first with ascending.
                                keys.remove(sortIndex);
                                keys.add(0, new SortKey(columnIndex, SortOrder.ASCENDING));
                            }
                            sorter.setSortKeys(keys);
                    }
                }
            }
        }
	}
	
	private class HeaderRenderer extends FimtraTableHeaderRenderer implements UIResource{
		private static final long serialVersionUID = 1L;

		@Override
		public Component getTableCellRendererComponent(JTable table,
				Object value, boolean isSelected, boolean hasFocus,
				int row, int column) {
			if(getRolloverColumn() == column){
				hasFocus = true;
			} 
			if(selectedColumn == table.convertColumnIndexToModel(column)){
				isSelected = true;
			}
			return super.getTableCellRendererComponent(table, value, isSelected, hasFocus,
					row, column);
		}
	}
}
