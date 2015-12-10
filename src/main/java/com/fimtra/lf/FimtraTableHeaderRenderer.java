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
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
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
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.JTableHeader;

/**
 * @author James
 *
 */
public class FimtraTableHeaderRenderer extends DefaultTableCellRenderer {
	private static final long serialVersionUID = 1L;

	private Icon sortArrow;
	private EmptyIcon emptyIcon = new EmptyIcon();
	
	private Border defaultBorder = new CompoundBorder(new HeaderBorder(Color.decode("#91969c")), new EmptyBorder(2, 4, 3, 4));
	private Color defaultColor1 = Color.decode("#edf0f6");
	private Color defaultColor2 = Color.decode("#dddfe4");
	private Color focusedColor1 = Color.decode("#ffffff");
	private Color focusedColor2 = Color.decode("#eef1f5");
	private Color selectedColor1 = Color.decode("#c7dae9");
	private Color selectedColor2 = Color.decode("#bdcddb");
	
	private Color color1 = defaultColor1;
	private Color color2 = defaultColor2;


	public FimtraTableHeaderRenderer() {
		//Uncomment the line below to tells nimbus l&f to take over painting this 
		//setName("TableHeader.renderer");
		setHorizontalAlignment(SwingConstants.LEADING);
		setHorizontalTextPosition(SwingConstants.LEADING);
	}

	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		Icon sortIcon = null;
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
				
				
					if(isSelected){
						color1 = selectedColor1;
						color2 = selectedColor2;
					} else if(hasFocus){
						color1 = focusedColor1;
						color2 = focusedColor2;
					} else {
						color1 = defaultColor1;
						color2 = defaultColor2;
					}
				}
				setForeground(fgColor);
				setBackground(bgColor);

				setFont(header.getFont());
			}

			if (table.getRowSorter() != null) {
				SortOrder sortOrder = getColumnSortOrder(table, column);
				if (sortOrder != null) {
					switch (sortOrder) {
					case ASCENDING:
						sortIcon = getUIIcon(this, "Table.ascendingSortIcon");
						break;
					case DESCENDING:
						sortIcon = getUIIcon(this, "Table.descendingSortIcon");
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
		if(border == null){
			border = defaultBorder;
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
	    /*    if we want to take over painting background.
	     * need to take over border as well*/
		Graphics2D g2d = (Graphics2D) g;
	        int w = getWidth();
	        int h = getHeight(); 
	        GradientPaint gp = new GradientPaint(
	                0, 0, color1,
	                0, h/2, color2, true);
	        

	        g2d.setPaint(gp);
	        g2d.fillRect(0, 0, w, h);
	        
		if (this.sortArrow != null) {
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

	private class HeaderBorder extends LineBorder{

		private static final long serialVersionUID = 1L;

		public HeaderBorder(Color color) {
			super(color);
		}
		
	    @Override
	    public Insets getBorderInsets(Component c, Insets insets) {
	        insets.set(0, 0, 1, 1);
	        return insets;
	    }
		
	    /**
	     * Paints the border for the specified component with the
	     * specified position and size.
	     * @param c the component for which this border is being painted
	     * @param g the paint graphics
	     * @param x the x position of the painted border
	     * @param y the y position of the painted border
	     * @param width the width of the painted border
	     * @param height the height of the painted border
	     */
	    public void paintBorder(Component c, Graphics g, int x, int y, int width, int height) {
            Color oldColor = g.getColor();
            g.setColor(this.lineColor);
            
            g.drawLine(width-1, y, width-1, y+height);
            g.drawLine(x, y+height-1, x+width, y+height-1);
            g.setColor(oldColor);
	    }
		
	}
}
