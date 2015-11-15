/*
 * Copyright (c) 2014 James Lupton, Ramon Servadei, Paul Mackinlay, Fimtra
 * All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in 
 * file 'LICENSE.txt', which is part of this source code package. 
 * The terms and conditions can also be found at http://fimtra.com/LICENSE.txt.
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
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.RowSorter;
import javax.swing.SortOrder;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.UIResource;
import javax.swing.plaf.synth.SynthTableHeaderUI;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;


/**
 * @author James
 *
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
		TableCellRenderer prevRenderer = header.getDefaultRenderer();
		if (prevRenderer instanceof UIResource) {
			header.setDefaultRenderer(new HeaderRenderer());
		}
		super.installDefaults();
	}

	class HeaderRenderer extends FimtraTableCellHeaderRenderer {
		private static final long serialVersionUID = 1L;

		HeaderRenderer() {
			setHorizontalAlignment(JLabel.LEADING);
			setName("TableHeader.renderer");
		}

		@Override
		public Component getTableCellRendererComponent(JTable table,
				Object value, boolean isSelected, boolean hasFocus, int row,
				int column) {

//			boolean hasRollover = (column == getRolloverColumn());
//			if (isSelected || hasRollover || hasFocus) {
//				SynthLookAndFeel.setSelectedUI((SynthLabelUI) SynthLookAndFeel
//						.getUIOfType(getUI(), SynthLabelUI.class), isSelected,
//						hasFocus, table.isEnabled(), hasRollover);
//			} else {
//				SynthLookAndFeel.resetSelectedUI();
//			}

			// stuff a variable into the client property of this renderer
			// indicating the sort order,
			// so that different rendering can be done for the header based on
			// sorted state.
			RowSorter rs = table == null ? null : table.getRowSorter();
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

	public class FimtraTableCellHeaderRenderer extends DefaultTableCellRenderer {
		private static final long serialVersionUID = 1L;

		private boolean horizontalTextPositionSet;
		private Icon sortArrow;
		private EmptyIcon emptyIcon = new EmptyIcon();

		public FimtraTableCellHeaderRenderer() {
			setHorizontalAlignment(JLabel.CENTER);
		}

		public void setHorizontalTextPosition(int textPosition) {
			horizontalTextPositionSet = true;
			super.setHorizontalTextPosition(textPosition);
		}

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
					if (!horizontalTextPositionSet) {
						// There is a row sorter, and the developer hasn't
						// set a text position, change to leading.
						setHorizontalTextPosition(JLabel.LEADING);
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
			sortArrow = sortIcon;

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
			if (b && sortArrow != null) {
				// emptyIcon is used so that if the text in the header is right
				// aligned, or if the column is too narrow, then the text will
				// be sized appropriately to make room for the icon that is
				// about
				// to be painted manually here.
				emptyIcon.width = sortArrow.getIconWidth();
				emptyIcon.height = sortArrow.getIconHeight();
				setIcon(emptyIcon);
				super.paintComponent(g);
				Point position = computeIconPosition(g);
				sortArrow.paintIcon(this, g, position.x, position.y);
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
					sortArrow, getVerticalAlignment(),
					getHorizontalAlignment(), getVerticalTextPosition(),
					getHorizontalTextPosition(), viewR, iconR, textR,
					getIconTextGap());
			int x = getWidth() - i.right - sortArrow.getIconWidth();
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

		public void paintIcon(Component c, Graphics g, int x, int y) {
		}

		public int getIconWidth() {
			return width;
		}

		public int getIconHeight() {
			return height;
		}
	}
}
