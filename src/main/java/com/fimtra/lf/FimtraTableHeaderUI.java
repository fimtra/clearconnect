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

import java.awt.Color;
import java.awt.Component;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Rectangle;
import java.io.Serializable;

import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JTable;
import javax.swing.RowSorter;
import javax.swing.SortOrder;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.UIResource;
import javax.swing.plaf.synth.SynthTableHeaderUI;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableModel;


/**
 * A table header UI that shows the header text in a tooltip.
 * 
 * @author James
 */
public class FimtraTableHeaderUI extends SynthTableHeaderUI {

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

	class HeaderRenderer extends FimtraTableCellHeaderRenderer {
		private static final long serialVersionUID = 1L;

		HeaderRenderer() {
			setHorizontalAlignment(SwingConstants.LEADING);
			setName("TableHeader.renderer");
		}

		@Override
		public Component getTableCellRendererComponent(JTable table,
				Object value, boolean isSelected, boolean hasFocus, int row,
				int column) {

			// stuff a variable into the client property of this renderer
			// indicating the sort order,
			// so that different rendering can be done for the header based on
			// sorted state.
			RowSorter<? extends TableModel> rs = table == null ? null : table.getRowSorter();
			java.util.List<? extends RowSorter.SortKey> sortKeys = rs == null ? null
					: rs.getSortKeys();
			if (sortKeys != null
					&& sortKeys.size() > 0
					&& sortKeys.get(0).getColumn() == table
							.convertColumnIndexToModel(column)) {
				switch (sortKeys.get(0).getSortOrder()) {
				case ASCENDING:
					putClientProperty("Table.sortOrder", "ASCENDING");
					break;
				case DESCENDING:
					putClientProperty("Table.sortOrder", "DESCENDING");
					break;
				case UNSORTED:
					putClientProperty("Table.sortOrder", "UNSORTED");
					break;
				default:
					throw new AssertionError("Cannot happen");
				}
			} else {
				putClientProperty("Table.sortOrder", "UNSORTED");
			}

			super.getTableCellRendererComponent(table, value, isSelected,
					hasFocus, row, column);

			return this;
		}

		@Override
		public void setBorder(Border border) {
			if (border != null && border.getClass().getName().contains("SynthBorder")) {
				super.setBorder(border);
			}
		}

		@Override
		public String getToolTipText() {
			return getText();
		}
	}
	
	@SuppressWarnings("synthetic-access")
	public class FimtraTableCellHeaderRenderer extends DefaultTableCellRenderer {
		private static final long serialVersionUID = 1L;

		private boolean horizontalTextPositionSet;
		private Icon sortArrow;
		private EmptyIcon emptyIcon = new EmptyIcon();

		public FimtraTableCellHeaderRenderer() {
			setHorizontalAlignment(SwingConstants.CENTER);
		}

		@Override
        public void setHorizontalTextPosition(int textPosition) {
			this.horizontalTextPositionSet = true;
			super.setHorizontalTextPosition(textPosition);
		}

        @Override
        public Component getTableCellRendererComponent(JTable table,
				Object value, boolean isSelected, boolean hasFocus, int row,
				int column) {
			Icon sortIcon = null;

			boolean isPaintingForPrint = false;

			if (table != null) {
				JTableHeader header = table.getTableHeader();
				if (header != null) {
					Color fgColor = null;
					Color bgColor = null;
					if (hasFocus) {
						fgColor = getUIColor(this,
								"TableHeader.focusCellForeground");
						bgColor = getUIColor(this,
								"TableHeader.focusCellBackground");
					}
					if (fgColor == null) {
						fgColor = header.getForeground();
					}
					if (bgColor == null) {
						bgColor = header.getBackground();
					}
					setForeground(fgColor);
					setBackground(bgColor);

					setFont(header.getFont());

					isPaintingForPrint = header.isPaintingForPrint();
				}

				if (!isPaintingForPrint && table.getRowSorter() != null) {
					if (!this.horizontalTextPositionSet) {
						// There is a row sorter, and the developer hasn't
						// set a text position, change to leading.
						setHorizontalTextPosition(SwingConstants.LEADING);
					}
					SortOrder sortOrder = getColumnSortOrder(table, column);
					if (sortOrder != null) {
						switch (sortOrder) {
						case ASCENDING:
							sortIcon = getUIIcon(this,
									"Table.ascendingSortIcon");
							break;
						case DESCENDING:
							sortIcon = getUIIcon(this,
									"Table.descendingSortIcon");
							break;
						case UNSORTED:
							sortIcon = getUIIcon(this, "Table.naturalSortIcon");
							break;
						}
					}
				}
			}

			setText(value == null ? "" : value.toString());
			setIcon(sortIcon);
			this.sortArrow = sortIcon;

			Border border = null;
			if (hasFocus) {
				border = getUIBorder(this, "TableHeader.focusCellBorder");
			}
			if (border == null) {
				border = getUIBorder(this, "TableHeader.cellBorder");
			}
			setBorder(border);

			return this;
		}

		@Override
		public String getToolTipText() {
			return getText();
		}

		@Override
		public void paintComponent(Graphics g) {
			boolean b = getUIBoolean(this, "TableHeader.rightAlignSortArrow",
					false);
			if (b && this.sortArrow != null) {
				// emptyIcon is used so that if the text in the header is right
				// aligned, or if the column is too narrow, then the text will
				// be sized appropriately to make room for the icon that is
				// about
				// to be painted manually here.
				this.emptyIcon.width = this.sortArrow.getIconWidth();
				this.emptyIcon.height = this.sortArrow.getIconHeight();
				setIcon(this.emptyIcon);
				super.paintComponent(g);
				Point position = computeIconPosition(g);
				this.sortArrow.paintIcon(this, g, position.x, position.y);
			} else {
				super.paintComponent(g);
			}
		}

		private Point computeIconPosition(Graphics g) {
			FontMetrics fontMetrics = g.getFontMetrics();
			Rectangle viewR = new Rectangle();
			Rectangle textR = new Rectangle();
			Rectangle iconR = new Rectangle();
			Insets i = getInsets();
			viewR.x = i.left;
			viewR.y = i.top;
			viewR.width = getWidth() - (i.left + i.right);
			viewR.height = getHeight() - (i.top + i.bottom);
			SwingUtilities.layoutCompoundLabel(this, fontMetrics, getText(),
					this.sortArrow, getVerticalAlignment(),
					getHorizontalAlignment(), getVerticalTextPosition(),
					getHorizontalTextPosition(), viewR, iconR, textR,
					getIconTextGap());
			int x = getWidth() - i.right - this.sortArrow.getIconWidth();
			int y = iconR.y;
			return new Point(x, y);
		}
	}

	private static Color getUIColor(JComponent c, String key) {
		Object ob = UIManager.get(key, c.getLocale());
		if (ob == null || !(ob instanceof Color)) {
			return null;
		}
		return (Color) ob;
	}

	private static Icon getUIIcon(JComponent c, String key) {
		Object iValue = UIManager.get(key, c.getLocale());
		if (iValue == null || !(iValue instanceof Icon)) {
			return null;
		}
		return (Icon) iValue;
	}

	private static Border getUIBorder(JComponent c, String key) {
		Object iValue = UIManager.get(key, c.getLocale());
		if (iValue == null || !(iValue instanceof Border)) {
			return null;
		}
		return (Border) iValue;
	}

	private static boolean getUIBoolean(JComponent c, String key,
			boolean defaultValue) {
		Object iValue = UIManager.get(key, c.getLocale());

		if (iValue == null || !(iValue instanceof Boolean)) {
			return defaultValue;
		}
		return ((Boolean) iValue).booleanValue();
	}

	public static SortOrder getColumnSortOrder(JTable table, int column) {
		SortOrder rv = null;
		if (table == null || table.getRowSorter() == null) {
			return rv;
		}
		java.util.List<? extends RowSorter.SortKey> sortKeys = table
				.getRowSorter().getSortKeys();
		if (sortKeys.size() > 0
				&& sortKeys.get(0).getColumn() == table
						.convertColumnIndexToModel(column)) {
			rv = sortKeys.get(0).getSortOrder();
		}
		return rv;
	}

	private class EmptyIcon implements Icon, Serializable {
		private static final long serialVersionUID = 1L;

		int width = 0;
		int height = 0;

		@Override
        public void paintIcon(Component c, Graphics g, int x, int y) {
		}

		@Override
        public int getIconWidth() {
			return this.width;
		}

		@Override
        public int getIconHeight() {
			return this.height;
		}
	}
}
