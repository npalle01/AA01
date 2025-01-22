#!/usr/bin/env python
import sys
import traceback
import pyodbc
import sqlparse

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import (
    Qt, QPointF, QTimer, QRegExp, QThreadPool, QRunnable, pyqtSignal, QObject
)
from PyQt5.QtGui import (
    QColor, QPen, QBrush, QFont, QSyntaxHighlighter, QTextCharFormat
)
from PyQt5.QtWidgets import (QGraphicsLineItem,
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QPushButton, QSplitter,
    QLineEdit, QLabel, QDialog, QFormLayout, QComboBox, QTableWidget,
    QTableWidgetItem, QTabWidget, QMessageBox, QGraphicsView,
    QGraphicsScene, QGraphicsRectItem, QGraphicsTextItem, QGraphicsItem,
    QProgressBar, QDialogButtonBox, QStatusBar, QGroupBox, QAbstractItemView,
    QSpinBox, QListWidget, QStyle, QMenu, QFrame, QInputDialog
)

pyodbc.pooling = True


###############################################################################
# Utility: Simple Button Helper
###############################################################################
def create_text_button(text: str, tooltip: str = "") -> QPushButton:
    btn = QPushButton(text)
    btn.setToolTip(tooltip)
    return btn


###############################################################################
# ODBCConnectDialog (Teradata)
###############################################################################
class ODBCConnectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Teradata (ODBC)")
        self.resize(400, 230)
        self._conn = None
        self._db_type = None

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Database Type (Fixed to Teradata):"))
        self.type_label = QLabel("Teradata")
        layout.addWidget(self.type_label)

        layout.addWidget(QLabel("ODBC DSN (Teradata Only):"))
        self.dsn_combo = QComboBox()
        try:
            dsn_map = pyodbc.dataSources()
            for dsn in sorted(dsn_map.keys()):
                self.dsn_combo.addItem(dsn)
        except:
            pass
        layout.addWidget(self.dsn_combo)

        layout.addWidget(QLabel("Username (optional):"))
        self.user_edit = QLineEdit()
        layout.addWidget(self.user_edit)

        layout.addWidget(QLabel("Password (optional):"))
        self.pass_edit = QLineEdit()
        self.pass_edit.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.pass_edit)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def on_ok(self):
        if not pyodbc:
            QMessageBox.critical(self, "Missing pyodbc", "pyodbc not installed.")
            return
        dsn = self.dsn_combo.currentText().strip()
        if not dsn:
            QMessageBox.warning(self, "Missing DSN", "Please pick a DSN.")
            return
        db_type = "Teradata"
        user = self.user_edit.text().strip()
        pwd = self.pass_edit.text().strip()

        conn_str = f"DSN={dsn};"
        if user:
            conn_str += f"UID={user};"
        if pwd:
            conn_str += f"PWD={pwd};"
        try:
            cn = pyodbc.connect(conn_str, autocommit=True)
            self._conn = cn
            self._db_type = db_type
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Connect Error", str(e))

    def get_connection(self):
        return self._conn

    def get_db_type(self):
        return self._db_type


###############################################################################
# LazySchema Worker and Tree
###############################################################################
class LazySchemaLoaderWorkerSignals(QObject):
    finished = pyqtSignal(list)
    error = pyqtSignal(str)


class LazySchemaLoaderWorker(QRunnable):
    def __init__(self, connection, database_name):
        super().__init__()
        self.connection = connection
        self.database_name = database_name
        self.signals = LazySchemaLoaderWorkerSignals()

    @QtCore.pyqtSlot()
    def run(self):
        try:
            cursor = self.connection.cursor()
            query = f"""
                SELECT TableName 
                FROM DBC.TablesV
                WHERE DatabaseName='{self.database_name}' AND TableKind='T'
                ORDER BY TableName
            """
            cursor.execute(query)
            results = cursor.fetchall()
            tables = [row[0] for row in results]
            self.signals.finished.emit(tables)
        except Exception as e:
            msg = f"Error loading tables for {self.database_name}: {e}\n{traceback.format_exc()}"
            self.signals.error.emit(msg)


class LazySchemaTreeWidget(QTreeWidget):
    """
    Extended to fetch real columns and store them into the parent's table_columns_map
    whenever a table is expanded.
    """
    def __init__(self, connection, parent_builder, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.parent_builder = parent_builder  # so we can update table_columns_map
        self.setHeaderHidden(True)
        self.setDragEnabled(True)
        self.threadpool = QThreadPool.globalInstance()
        self.populate_top_level()
        self.itemExpanded.connect(self.on_item_expanded)
        self.setSelectionMode(QAbstractItemView.SingleSelection)

    def populate_top_level(self):
        self.clear()
        conn_name = "Not Connected"
        if self.connection:
            try:
                dbms = self.connection.getinfo(pyodbc.SQL_DBMS_NAME).strip()
                if "TERADATA" in dbms.upper():
                    conn_name = dbms
            except:
                pass

        conn_item = QTreeWidgetItem([conn_name])
        conn_item.setData(0, Qt.UserRole, "connection")
        self.addTopLevelItem(conn_item)

        if not self.connection:
            return

        cursor = self.connection.cursor()
        db_names = []
        try:
            cursor.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
            db_names = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("Error fetching DB names:", e)

        if not db_names:
            no_db_item = QTreeWidgetItem(["<No databases found>"])
            conn_item.addChild(no_db_item)
            return

        for db in db_names:
            db_item = QTreeWidgetItem([db])
            db_item.setData(0, Qt.UserRole, "database")
            db_item.setData(0, Qt.UserRole+1, False)
            dummy = QTreeWidgetItem(["Loading..."])
            db_item.addChild(dummy)
            conn_item.addChild(db_item)

        self.expandItem(conn_item)

    def on_item_expanded(self, item):
        data_type = item.data(0, Qt.UserRole)
        loaded_flag = item.data(0, Qt.UserRole+1)
        if data_type=="database" and not loaded_flag:
            item.takeChildren()
            db_name = item.text(0)
            worker = LazySchemaLoaderWorker(self.connection, db_name)
            worker.signals.finished.connect(lambda tbls, i=item: self.populate_db_node(i,tbls))
            worker.signals.error.connect(self.handle_error)
            self.threadpool.start(worker)
        elif data_type=="table" and not loaded_flag:
            item.takeChildren()
            db_name = item.parent().text(0)
            tbl_name = item.text(0)
            cols = self.load_columns(db_name, tbl_name)
            if cols:
                for c in cols:
                    citem = QTreeWidgetItem([c])
                    citem.setData(0,Qt.UserRole,"column")
                    citem.setFlags(citem.flags() | Qt.ItemIsUserCheckable)
                    citem.setCheckState(0, Qt.Unchecked)
                    item.addChild(citem)

                # Also store them to parent's table_columns_map as "db_name.tbl_name"
                full_table_name = f"{db_name}.{tbl_name}"
                self.parent_builder.table_columns_map[full_table_name] = cols
            else:
                item.addChild(QTreeWidgetItem(["<No columns found>"]))
            item.setData(0, Qt.UserRole+1, True)

    def populate_db_node(self, db_item, tables):
        if not tables:
            db_item.addChild(QTreeWidgetItem(["<No tables found>"]))
            db_item.setData(0, Qt.UserRole+1, True)
            return
        for t in tables:
            titem = QTreeWidgetItem([t])
            titem.setData(0, Qt.UserRole, "table")
            titem.setData(0, Qt.UserRole+1, False)
            dummy = QTreeWidgetItem(["Loading columns..."])
            titem.addChild(dummy)
            db_item.addChild(titem)
        db_item.setData(0, Qt.UserRole+1, True)

    def load_columns(self, db_name, tbl_name):
        cols = []
        try:
            c = self.connection.cursor()
            c.execute(f"""
                SELECT ColumnName
                FROM DBC.ColumnsV
                WHERE DatabaseName='{db_name}' AND TableName='{tbl_name}'
                ORDER BY ColumnId
            """)
            rows = c.fetchall()
            cols = [r[0] for r in rows]
        except Exception as e:
            print(f"Error loading columns for {db_name}.{tbl_name}: {e}")
        return cols

    def handle_error(self, msg):
        QMessageBox.critical(self, "Schema Error", msg)

    def startDrag(self, supportedActions):
        """
        On drag, we embed the fully qualified "db.table" name into the text,
        so we can retrieve real columns in the drop.
        """
        item = self.currentItem()
        if item and item.parent() and item.data(0, Qt.UserRole)=="table":
            db_name = item.parent().text(0)
            tbl_name = item.text(0)
            full_name = f"{db_name}.{tbl_name}"
            drag = QtGui.QDrag(self)
            mime = QtCore.QMimeData()
            mime.setText(full_name)
            drag.setMimeData(mime)
            drag.exec_(supportedActions)


###############################################################################
# SQL Parser Stub
###############################################################################
class SQLParser:
    def __init__(self, sql):
        self.sql = sql
    def parse(self):
        if not self.sql.strip():
            raise ValueError("SQL is empty.")
        statements = sqlparse.parse(self.sql)
        if not statements:
            raise ValueError("No valid SQL found.")


###############################################################################
# SQL Highlighter
###############################################################################
class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, doc):
        super().__init__(doc)
        self.highlightingRules = []

        keywordFormat = QTextCharFormat()
        keywordFormat.setForeground(Qt.darkBlue)
        keywordFormat.setFontWeight(QFont.Bold)

        keywords = [
            "SELECT","FROM","WHERE","JOIN","INNER JOIN","LEFT JOIN","RIGHT JOIN",
            "FULL OUTER JOIN","GROUP BY","HAVING","ORDER BY","LIMIT","OFFSET",
            "UNION","UNION ALL","INTERSECT","EXCEPT","AS","ON","AND","OR","NOT",
            "IN","IS NULL","IS NOT NULL","EXISTS","COUNT","SUM","AVG","MIN","MAX",
            "INSERT","UPDATE","DELETE","VALUES"
        ]
        for word in keywords:
            pattern = QRegExp(r'\b' + word + r'\b', Qt.CaseInsensitive)
            self.highlightingRules.append((pattern, keywordFormat))

        stringFormat = QTextCharFormat()
        stringFormat.setForeground(Qt.darkRed)
        self.highlightingRules.append((QRegExp("'[^']*'"), stringFormat))
        self.highlightingRules.append((QRegExp('"[^"]*"'), stringFormat))

        commentFormat = QTextCharFormat()
        commentFormat.setForeground(Qt.green)
        self.highlightingRules.append((QRegExp("--[^\n]*"), commentFormat))
        self.highlightingRules.append((QRegExp("/\\*.*\\*/"), commentFormat))

    def highlightBlock(self, text):
        for pattern, fmt in self.highlightingRules:
            idx = pattern.indexIn(text)
            while idx>=0:
                length = pattern.matchedLength()
                self.setFormat(idx, length, fmt)
                idx = pattern.indexIn(text, idx+length)
        self.setCurrentBlockState(0)


###############################################################################
# Canvas Items (ERDTableItem, DerivedColumnItem, CombineQueryItem, etc.)
###############################################################################
class ERDTableItem(QGraphicsRectItem):
    """
    A rectangle representing a table, with robust removal logic
    (no crashes on remove).
    """
    def __init__(self, table_name, columns, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.columns = columns
        self.join_lines = []
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable |
                      QGraphicsItem.ItemSendsGeometryChanges)

        width = 200
        y_titlebar_height = 20
        self.setRect(0,0,width,50)
        self.setPen(QPen(Qt.black,2))
        self.setBrush(QBrush(QColor(240,240,240)))

        # Title bar
        self.title_bar = QGraphicsRectItem(self)
        self.title_bar.setRect(0,0,width,y_titlebar_height)
        self.title_bar.setBrush(QColor(200,220,240))

        self.title_text = QGraphicsTextItem(table_name, self.title_bar)
        font = QFont("Arial",9,QFont.Bold)
        self.title_text.setFont(font)
        txt_rect = self.title_text.boundingRect()
        self.title_text.setPos((width - txt_rect.width())/2, 1)

        offset_y = y_titlebar_height
        for col in columns:
            col_txt = QGraphicsTextItem(col, self)
            col_txt.setPos(5, offset_y)
            col_txt.setFont(QFont("Arial",8))
            offset_y += 15
        total_height = max(50, offset_y+5)
        self.setRect(0,0,width,total_height)

    def itemChange(self, change, value):
        if change == QGraphicsItem.ItemPositionChange:
            # update join lines
            for jl in self.join_lines:
                jl.update_position()
        return super().itemChange(change, value)

    def contextMenuEvent(self, event):
        menu = QMenu()
        remove_action = menu.addAction("Remove Table")
        chosen = menu.exec_(event.screenPos().toPoint())
        if chosen==remove_action:
            self.remove_self()

    def remove_self(self):
        scene = self.scene()
        if not scene:
            return
        # Remove all join lines first
        for jl in list(self.join_lines):
            if jl.scene():
                jl.scene().removeItem(jl)
            if jl in self.join_lines:
                self.join_lines.remove(jl)
            # Also remove from other item
            if jl.start_item==self and hasattr(jl.end_item,"join_lines"):
                if jl in jl.end_item.join_lines:
                    jl.end_item.join_lines.remove(jl)
            elif jl.end_item==self and hasattr(jl.start_item,"join_lines"):
                if jl in jl.start_item.join_lines:
                    jl.start_item.join_lines.remove(jl)
        # now remove table
        scene.removeItem(self)


class JoinLine(QGraphicsLineItem):
    def __init__(self, start_item, end_item, join_type="INNER JOIN", condition=""):
        super().__init__()
        self.start_item = start_item
        self.end_item = end_item
        self.join_type = join_type
        self.condition = condition
        self.setZValue(-1)

        pen = QPen(Qt.black, 2)
        self.setPen(pen)

        self.label = QGraphicsTextItem(join_type, self)
        self.label.setDefaultTextColor(Qt.blue)
        self.update_position()

    def update_position(self):
        sr = self.start_item.rect()
        er = self.end_item.rect()
        s = self.start_item.scenePos() + QPointF(sr.width()/2, sr.height()/2)
        e = self.end_item.scenePos() + QPointF(er.width()/2, er.height()/2)
        self.setLine(QtCore.QLineF(s,e))
        mid_x = (s.x()+e.x())/2
        mid_y = (s.y()+e.y())/2
        self.label.setPos(mid_x, mid_y)


class DerivedColumnItem(QGraphicsRectItem):
    def __init__(self, alias, expression, x=0, y=0):
        super().__init__(0,0,220,60)
        self.alias = alias
        self.expression = expression
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(255,230,200)))
        self.setPen(QPen(Qt.darkBlue,2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        txt = QGraphicsTextItem(f"Derived:\n{alias} = {expression}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial",8,QFont.Bold))
        txt.setPos(5,5)


class CombineQueryItem(QGraphicsRectItem):
    def __init__(self, operator, second_sql, x=0, y=0):
        super().__init__(0,0,260,80)
        self.operator = operator
        self.second_sql = second_sql
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(210,255,210)))
        self.setPen(QPen(Qt.darkGreen,2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        preview = second_sql[:25] + "..." if len(second_sql) > 25 else second_sql
        txt = QGraphicsTextItem(f"Combine:\n{operator}\n{preview}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial",8,QFont.Bold))
        txt.setPos(5,5)


class CompleteQueryItem(QGraphicsRectItem):
    def __init__(self, columns, x=0, y=0):
        super().__init__(0,0,200,100)
        self.columns = columns
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(250,250,180)))
        self.setPen(QPen(Qt.red,2))
        self.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)

        col_text = "\n".join(columns) if columns else "<No columns>"
        txt = QGraphicsTextItem(f"Result Columns:\n{col_text}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial",8,QFont.Bold))
        txt.setPos(5,5)


###############################################################################
# FilterPanel, GroupByPanel, SortLimitPanel
###############################################################################
class AddFilterDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Filter")
        self.selected_column = None
        self.selected_operator = None
        self.selected_value = None

        layout = QFormLayout(self)

        self.column_combo = QComboBox()
        self.column_combo.addItems(available_columns)
        layout.addRow("Column:", self.column_combo)

        self.operator_combo = QComboBox()
        self.operator_combo.addItems(["=", "<>", "<", ">", "<=", ">=", "IS NULL", "IS NOT NULL"])
        layout.addRow("Operator:", self.operator_combo)

        self.value_combo = QComboBox()
        self.value_combo.addItems(["123","'ABC'","'XYZ'","1000"])
        layout.addRow("Value:", self.value_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def on_ok(self):
        c = self.column_combo.currentText()
        o = self.operator_combo.currentText()
        v = self.value_combo.currentText()
        if not c:
            QMessageBox.warning(self, "No Column", "Must choose a column.")
            return
        self.selected_column = c
        self.selected_operator = o
        self.selected_value = v
        self.accept()

    def get_filter(self):
        return (self.selected_column, self.selected_operator, self.selected_value)


class FilterPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Filters", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.where_tab = QWidget()
        self.having_tab = QWidget()
        self.tabs.addTab(self.where_tab, "WHERE")
        self.tabs.addTab(self.having_tab, "HAVING")

        # WHERE
        self.where_layout = QVBoxLayout(self.where_tab)
        self.where_table = QTableWidget(0,3)
        self.where_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.where_table.horizontalHeader().setStretchLastSection(True)
        self.where_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.where_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.where_layout.addWidget(self.where_table)

        wh_btns = QHBoxLayout()
        add_w = create_text_button("Add WHERE")
        add_w.clicked.connect(lambda: self.add_filter("WHERE"))
        rm_w = create_text_button("Remove WHERE")
        rm_w.clicked.connect(lambda: self.remove_filter("WHERE"))
        wh_btns.addWidget(add_w)
        wh_btns.addWidget(rm_w)
        self.where_layout.addLayout(wh_btns)

        # HAVING
        self.having_layout = QVBoxLayout(self.having_tab)
        self.having_table = QTableWidget(0,3)
        self.having_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.having_table.horizontalHeader().setStretchLastSection(True)
        self.having_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.having_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.having_layout.addWidget(self.having_table)

        hv_btns = QHBoxLayout()
        add_h = create_text_button("Add HAVING")
        add_h.clicked.connect(lambda: self.add_filter("HAVING"))
        rm_h = create_text_button("Remove HAVING")
        rm_h.clicked.connect(lambda: self.remove_filter("HAVING"))
        hv_btns.addWidget(add_h)
        hv_btns.addWidget(rm_h)
        self.having_layout.addLayout(hv_btns)

    def add_filter(self, clause):
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns selected.")
            return
        dlg = AddFilterDialog(columns, self)
        if dlg.exec_()==QDialog.Accepted:
            col, op, val = dlg.get_filter()
            if clause=="WHERE":
                table = self.where_table
            else:
                table = self.having_table
            row = table.rowCount()
            table.insertRow(row)
            table.setItem(row,0,QTableWidgetItem(col))
            table.setItem(row,1,QTableWidgetItem(op))
            table.setItem(row,2,QTableWidgetItem(val))
            self.builder.generate_sql()

    def remove_filter(self, clause):
        table = self.where_table if clause=="WHERE" else self.having_table
        rows = sorted([x.row() for x in table.selectionModel().selectedRows()], reverse=True)
        for r in rows:
            table.removeRow(r)
        self.builder.generate_sql()

    def get_filters(self, clause):
        table = self.where_table if clause=="WHERE" else self.having_table
        res=[]
        for r in range(table.rowCount()):
            c = table.item(r,0).text()
            o = table.item(r,1).text()
            v = table.item(r,2).text()
            res.append((c,o,v))
        return res


class AddGroupByDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add GroupBy")
        self.selected_column = None
        layout = QVBoxLayout(self)
        self.col_combo = QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addWidget(QLabel("Choose column to Group By:"))
        layout.addWidget(self.col_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept_data)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def accept_data(self):
        col = self.col_combo.currentText()
        if not col:
            QMessageBox.warning(self, "Error", "No column chosen.")
            return
        self.selected_column = col
        self.accept()

    def get_column(self):
        return self.selected_column


class AddAggregateDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Aggregate")
        self.selected_func = None
        self.selected_col = None
        self.selected_alias = None

        layout = QFormLayout(self)

        self.func_combo = QComboBox()
        self.func_combo.addItems(["COUNT","SUM","AVG","MIN","MAX"])
        layout.addRow("Function:", self.func_combo)

        self.col_combo = QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addRow("Column:", self.col_combo)

        self.alias_combo = QComboBox()
        self.alias_combo.addItems(["AggVal","AggResult","MyAgg"])
        layout.addRow("Alias:", self.alias_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def on_ok(self):
        f = self.func_combo.currentText()
        c = self.col_combo.currentText()
        a = self.alias_combo.currentText()
        if not c:
            QMessageBox.warning(self, "Error", "Must pick a column.")
            return
        self.selected_func = f
        self.selected_col = c
        self.selected_alias = a
        self.accept()

    def get_aggregate(self):
        return (self.selected_func, self.selected_col, self.selected_alias)


class GroupByPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Group By and Aggregates", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.group_by_table = QTableWidget(0,1)
        self.group_by_table.setHorizontalHeaderLabels(["Group By Columns"])
        self.group_by_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.group_by_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.group_by_table)

        gb_btn = QHBoxLayout()
        add_gb = create_text_button("Add GroupBy")
        add_gb.clicked.connect(self.add_group_by)
        rem_gb = create_text_button("Remove GroupBy")
        rem_gb.clicked.connect(self.remove_group_by)
        gb_btn.addWidget(add_gb)
        gb_btn.addWidget(rem_gb)
        layout.addLayout(gb_btn)

        self.aggregates_table = QTableWidget(0,3)
        self.aggregates_table.setHorizontalHeaderLabels(["Function","Column","Alias"])
        self.aggregates_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.aggregates_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.aggregates_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.aggregates_table.customContextMenuRequested.connect(self.open_context_menu)
        layout.addWidget(self.aggregates_table)

        agg_btn = QHBoxLayout()
        add_agg = create_text_button("Add Agg")
        add_agg.clicked.connect(self.add_aggregate)
        rem_agg = create_text_button("Remove Agg")
        rem_agg.clicked.connect(self.remove_aggregate)
        agg_btn.addWidget(add_agg)
        agg_btn.addWidget(rem_agg)
        layout.addLayout(agg_btn)

    def add_group_by(self):
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns available.")
            return
        dlg = AddGroupByDialog(columns, self)
        if dlg.exec_()==QDialog.Accepted:
            col = dlg.get_column()
            if col:
                row = self.group_by_table.rowCount()
                self.group_by_table.insertRow(row)
                self.group_by_table.setItem(row,0,QTableWidgetItem(col))
                self.builder.generate_sql()

    def remove_group_by(self):
        rows = sorted([x.row() for x in self.group_by_table.selectionModel().selectedRows()], reverse=True)
        for r in rows:
            self.group_by_table.removeRow(r)
        self.builder.generate_sql()

    def add_aggregate(self):
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns available.")
            return
        dlg = AddAggregateDialog(columns, self)
        if dlg.exec_()==QDialog.Accepted:
            func, col, alias = dlg.get_aggregate()
            row = self.aggregates_table.rowCount()
            self.aggregates_table.insertRow(row)
            self.aggregates_table.setItem(row, 0, QTableWidgetItem(func))
            self.aggregates_table.setItem(row, 1, QTableWidgetItem(col))
            self.aggregates_table.setItem(row, 2, QTableWidgetItem(alias))
            self.builder.generate_sql()

    def remove_aggregate(self):
        rows = sorted([x.row() for x in self.aggregates_table.selectionModel().selectedRows()], reverse=True)
        for r in rows:
            self.aggregates_table.removeRow(r)
        self.builder.generate_sql()

    def open_context_menu(self, pos):
        menu = QMenu()
        edit_action = menu.addAction("Edit")
        delete_action = menu.addAction("Delete")
        act = menu.exec_(self.aggregates_table.viewport().mapToGlobal(pos))
        if act==edit_action:
            sel = self.aggregates_table.selectedItems()
            if not sel:
                return
            row = sel[0].row()
            columns = self.builder.get_all_possible_columns_for_dialog()
            dlg = AddAggregateDialog(columns, self)
            if dlg.exec_()==QDialog.Accepted:
                f,c,a = dlg.get_aggregate()
                self.aggregates_table.setItem(row,0,QTableWidgetItem(f))
                self.aggregates_table.setItem(row,1,QTableWidgetItem(c))
                self.aggregates_table.setItem(row,2,QTableWidgetItem(a))
                self.builder.generate_sql()
        elif act==delete_action:
            self.remove_aggregate()

    def get_group_by(self):
        gb_cols = []
        for r in range(self.group_by_table.rowCount()):
            it = self.group_by_table.item(r,0)
            if it:
                gb_cols.append(it.text())
        return gb_cols

    def get_aggregates(self):
        ags = []
        for r in range(self.aggregates_table.rowCount()):
            f_item = self.aggregates_table.item(r,0)
            c_item = self.aggregates_table.item(r,1)
            a_item = self.aggregates_table.item(r,2)
            if f_item and c_item and a_item:
                ags.append((f_item.text(), c_item.text(), a_item.text()))
        return ags


class AddSortDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Sort")
        self.selected_column = None
        self.selected_direction = None
        layout = QFormLayout()

        self.col_combo = QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addRow("Column:", self.col_combo)

        self.dir_combo = QComboBox()
        self.dir_combo.addItems(["ASC","DESC"])
        layout.addRow("Direction:", self.dir_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def on_ok(self):
        c = self.col_combo.currentText()
        d = self.dir_combo.currentText()
        if not c:
            QMessageBox.warning(self, "No col", "Pick a column.")
            return
        self.selected_column = c
        self.selected_direction = d
        self.accept()

    def get_sort_info(self):
        return (self.selected_column, self.selected_direction)


class SortLimitPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Sort and Limit", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.sort_table = QTableWidget(0,2)
        self.sort_table.setHorizontalHeaderLabels(["Column","Direction"])
        self.sort_table.horizontalHeader().setStretchLastSection(True)
        self.sort_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sort_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.sort_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.sort_table.customContextMenuRequested.connect(self.open_context_menu)
        layout.addWidget(self.sort_table)

        btn_layout = QHBoxLayout()
        add_sort = create_text_button("Add Sort")
        add_sort.clicked.connect(self.add_sort_dialog)
        rem_sort = create_text_button("Remove Sort")
        rem_sort.clicked.connect(self.remove_sort)
        btn_layout.addWidget(add_sort)
        btn_layout.addWidget(rem_sort)
        layout.addLayout(btn_layout)

        limit_offset_h = QHBoxLayout()
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(0,9999999)
        self.limit_spin.setValue(0)
        self.limit_spin.setSuffix(" (Limit)")
        self.limit_spin.setSpecialValueText("No Limit")
        self.limit_spin.valueChanged.connect(lambda: self.builder.generate_sql())
        limit_offset_h.addWidget(self.limit_spin)

        self.offset_spin = QSpinBox()
        self.offset_spin.setRange(0,9999999)
        self.offset_spin.setValue(0)
        self.offset_spin.setSuffix(" (Offset)")
        self.offset_spin.setSpecialValueText("No Offset")
        self.offset_spin.valueChanged.connect(lambda: self.builder.generate_sql())
        limit_offset_h.addWidget(self.offset_spin)

        layout.addLayout(limit_offset_h)

    def add_sort_dialog(self):
        cols = self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self, "No columns", "No columns available.")
            return
        dlg = AddSortDialog(cols, self)
        if dlg.exec_()==QDialog.Accepted:
            c,d = dlg.get_sort_info()
            row = self.sort_table.rowCount()
            self.sort_table.insertRow(row)
            self.sort_table.setItem(row,0,QTableWidgetItem(c))
            self.sort_table.setItem(row,1,QTableWidgetItem(d))
            self.builder.generate_sql()

    def remove_sort(self):
        rows = sorted([x.row() for x in self.sort_table.selectionModel().selectedRows()], reverse=True)
        for r in rows:
            self.sort_table.removeRow(r)
        self.builder.generate_sql()

    def open_context_menu(self, pos):
        menu = QMenu()
        edit_action = menu.addAction("Edit")
        delete_action = menu.addAction("Delete")
        act = menu.exec_(self.sort_table.viewport().mapToGlobal(pos))
        if act==edit_action:
            sel = self.sort_table.selectedItems()
            if not sel:
                return
            row = sel[0].row()
            cols = self.builder.get_all_possible_columns_for_dialog()
            dlg = AddSortDialog(cols, self)
            if dlg.exec_()==QDialog.Accepted:
                c,d = dlg.get_sort_info()
                self.sort_table.setItem(row,0,QTableWidgetItem(c))
                self.sort_table.setItem(row,1,QTableWidgetItem(d))
                self.builder.generate_sql()
        elif act==delete_action:
            self.remove_sort()

    def get_order_bys(self):
        res=[]
        for r in range(self.sort_table.rowCount()):
            col = self.sort_table.item(r,0).text()
            dr = self.sort_table.item(r,1).text()
            res.append(f"{col} {dr}")
        return res

    def get_limit(self):
        val = self.limit_spin.value()
        return val if val>0 else None

    def get_offset(self):
        val = self.offset_spin.value()
        return val if val>0 else None


class SQLImportTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("SQL Import Tab (Stub)"))
        self.setLayout(layout)


###############################################################################
# EnhancedCanvasGraphicsView
###############################################################################
class EnhancedCanvasGraphicsView(QGraphicsView):
    def __init__(self, builder, parent=None):
        super().__init__(parent)
        self.builder = builder
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QGraphicsView.RubberBandDrag)

        self.join_lines = []
        self.mapping_lines = []
        self.table_items = {}
        self.subquery_items = {}

        self.operation_red_line = None
        self.complete_query_item = None
        self.target_table_item = None

        self.validation_timer = QTimer()
        self.validation_timer.setInterval(800)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.builder.validate_sql)

    def dragEnterEvent(self, e):
        if e.mimeData().hasText():
            e.acceptProposedAction()

    def dragMoveEvent(self, e):
        e.acceptProposedAction()

    def dropEvent(self, e):
        text = e.mimeData().text()  # e.g. "DBNAME.myTable"
        pos = self.mapToScene(e.pos())
        self.builder.handle_drop(text, pos)
        e.acceptProposedAction()

    def drawBackground(self, painter, rect):
        # Light grid
        grid = 20
        left = int(rect.left()) - (int(rect.left())%grid)
        top = int(rect.top()) - (int(rect.top())%grid)

        lines = []
        x = left
        while x<rect.right():
            lines.append(QtCore.QLineF(x, rect.top(), x, rect.bottom()))
            x+=grid
        y = top
        while y<rect.bottom():
            lines.append(QtCore.QLineF(rect.left(), y, rect.right(), y))
            y+=grid
        painter.setPen(QPen(QColor(220,220,220),1))
        painter.drawLines(lines)

    def add_table(self, alias, columns, pos):
        """
        Create an ERDTableItem with the real columns from the schema.
        """
        if alias in self.table_items:
            QMessageBox.warning(self, "Duplicate", f"'{alias}' is already on canvas.")
            return
        item = ERDTableItem(alias, columns)
        item.setPos(pos)
        self.scene.addItem(item)
        self.table_items[alias] = item
        self.builder.generate_sql()
        self.validation_timer.start()

    def add_vertical_red_line(self, x=450):
        if self.operation_red_line:
            self.scene.removeItem(self.operation_red_line)
        line_item = QtWidgets.QGraphicsLineItem(x,0,x,3000)
        pen = QPen(Qt.red,2,Qt.DashDotLine)
        line_item.setPen(pen)
        line_item.setZValue(-10)
        self.scene.addItem(line_item)
        self.operation_red_line = line_item

    def add_complete_query_item(self, columns, x=100,y=200):
        if self.complete_query_item:
            self.scene.removeItem(self.complete_query_item)
            self.complete_query_item = None
        cqi = CompleteQueryItem(columns,x,y)
        self.scene.addItem(cqi)
        self.complete_query_item = cqi

    def add_target_table_item(self, db_name, table_name, columns, x=500,y=200):
        pass

    def create_mapping_line(self, source_text_item, target_text_item):
        pass

    def remove_subquery(self, alias):
        if alias in self.subquery_items:
            item = self.subquery_items[alias]
            self.scene.removeItem(item)
            del self.subquery_items[alias]
            self.builder.generate_sql()
            self.validation_timer.start()

    def add_join(self, table1, table2, join_type, condition):
        if table1 not in self.table_items or table2 not in self.table_items:
            QMessageBox.warning(self, "Join Error", "Check tables on canvas.")
            return
        start_item = self.table_items[table1]
        end_item = self.table_items[table2]
        jline = JoinLine(start_item, end_item, join_type, condition)
        self.scene.addItem(jline)
        start_item.join_lines.append(jline)
        end_item.join_lines.append(jline)
        self.builder.joins.append({
            "table1": table1,
            "table2": table2,
            "type": join_type,
            "condition": condition,
        })
        self.builder.generate_sql()
        self.validation_timer.start()

    def clear_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene.removeItem(ml)
        self.mapping_lines.clear()


###############################################################################
# SubVQBDialog for building second query
###############################################################################
class SubVQBDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Build Second Query (Full VQB)")
        self.resize(900,600)
        self.operator = "UNION"
        self.second_sql = ""

        layout = QVBoxLayout(self)
        self.setLayout(layout)

        # operator row
        op_layout = QHBoxLayout()
        op_layout.addWidget(QLabel("Combine Operator:"))
        self.op_combo = QComboBox()
        self.op_combo.addItems(["UNION","UNION ALL","INTERSECT","EXCEPT"])
        op_layout.addWidget(self.op_combo)
        op_layout.addStretch()
        layout.addLayout(op_layout)

        # sub vqb
        self.sub_vqb = VisualQueryBuilderTab()
        layout.addWidget(self.sub_vqb)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def on_ok(self):
        self.operator = self.op_combo.currentText()
        self.second_sql = self.sub_vqb.sql_display.toPlainText().strip()
        if not self.second_sql:
            QMessageBox.warning(self, "No SQL", "Second query is empty.")
            return
        self.accept()

    def getResult(self):
        return (self.operator, self.second_sql)


###############################################################################
# Main VQB
###############################################################################
class VisualQueryBuilderTab(QWidget):
    """
    The main builder, which also keeps a dict of table_columns_map so we
    can place real columns on the canvas.
    """
    def __init__(self, parent=None):
        super().__init__(parent)

        self.connections = {}
        self.joins = []
        self.operation_mode = "SELECT"
        self.combination_operator = None
        self.second_query = None

        # key = "DBNAME.tblName", value = list of column names
        self.table_columns_map = {}

        QApplication.setStyle("Windows")
        self.threadpool = QThreadPool.globalInstance()

        self.initUI()

    def initUI(self):
        main_layout = QVBoxLayout(self)

        # Connection row
        conn_layout = QHBoxLayout()
        self.status_light = QFrame()
        self.status_light.setFixedSize(15,15)
        self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red;}")
        self.server_label = QLabel("Not Connected")
        conn_btn = QPushButton("Connect")
        conn_btn.clicked.connect(self.open_connect_dialog)
        conn_layout.addWidget(self.status_light)
        conn_layout.addWidget(self.server_label)
        conn_layout.addWidget(conn_btn)
        conn_layout.addStretch()
        main_layout.addLayout(conn_layout)

        # Toolbar row
        toolbar_layout = QHBoxLayout()
        refresh_btn = QPushButton("Refresh Schema")
        refresh_btn.clicked.connect(self.refresh_schema)
        toolbar_layout.addWidget(refresh_btn)

        alias_btn = QPushButton("Manage Aliases")
        alias_btn.clicked.connect(self.manage_aliases)
        toolbar_layout.addWidget(alias_btn)

        window_fn_btn = QPushButton("Window Function (Stub)")
        toolbar_layout.addWidget(window_fn_btn)

        combine_btn = QPushButton("Combine Query (Full Sub VQB)")
        combine_btn.clicked.connect(self.combine_with_full_vqb)
        toolbar_layout.addWidget(combine_btn)

        derived_btn = QPushButton("Add Derived Column (Complex)")
        derived_btn.clicked.connect(self.derived_complex)
        toolbar_layout.addWidget(derived_btn)

        self.operation_combo = QComboBox()
        self.operation_combo.addItems(["SELECT (No Operation)","INSERT","UPDATE","DELETE"])
        self.operation_combo.currentIndexChanged.connect(self.toggle_operation_mode)
        toolbar_layout.addWidget(self.operation_combo)

        toolbar_layout.addStretch()
        main_layout.addLayout(toolbar_layout)

        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        self.schema_canvas_tab = QWidget()
        self.tabs.addTab(self.schema_canvas_tab, "Schema & Canvas")

        self.query_config_tab = QWidget()
        self.tabs.addTab(self.query_config_tab, "Query Configuration")

        self.sql_preview_tab = QWidget()
        self.tabs.addTab(self.sql_preview_tab, "SQL Preview")

        self.sql_import_tab = SQLImportTab(self)
        self.tabs.addTab(self.sql_import_tab, "SQL Import")

        self.status_bar = QStatusBar()
        main_layout.addWidget(self.status_bar)

        self.setLayout(main_layout)

        self.setup_schema_canvas_tab()
        self.setup_query_config_tab()
        self.setup_sql_preview_tab()

    def setup_schema_canvas_tab(self):
        layout = QVBoxLayout(self.schema_canvas_tab)
        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search tables/columns...")
        self.search_bar.textChanged.connect(self.filter_schema_tree)
        layout.addWidget(self.search_bar)

        splitter = QSplitter(Qt.Horizontal)

        conn = None
        if self.connections:
            first_key = list(self.connections.keys())[0]
            conn = self.connections[first_key]['connection']

        # Pass self to LazySchemaTreeWidget so it can update self.table_columns_map
        self.schema_tree = LazySchemaTreeWidget(conn, parent_builder=self)
        self.schema_tree.itemChanged.connect(self.handle_item_changed)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.addWidget(self.schema_tree)
        splitter.addWidget(left_panel)

        self.canvas = EnhancedCanvasGraphicsView(builder=self)
        splitter.addWidget(self.canvas)
        splitter.setStretchFactor(0,1)
        splitter.setStretchFactor(1,3)
        layout.addWidget(splitter)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

    def setup_query_config_tab(self):
        layout = QHBoxLayout(self.query_config_tab)

        self.filter_panel = FilterPanel(self)
        layout.addWidget(self.filter_panel,2)

        self.group_by_panel = GroupByPanel(self)
        layout.addWidget(self.group_by_panel,3)

        self.sort_limit_panel = SortLimitPanel(self)
        layout.addWidget(self.sort_limit_panel,2)

        self.query_config_tab.setLayout(layout)

    def setup_sql_preview_tab(self):
        layout = QVBoxLayout(self.sql_preview_tab)

        header = QHBoxLayout()
        header.addWidget(QLabel("Generated SQL:"))
        run_btn = QPushButton("Run SQL")
        run_btn.clicked.connect(self.run_sql_query)
        header.addWidget(run_btn, alignment=Qt.AlignRight)
        layout.addLayout(header)

        self.sql_display = QTextEdit()
        self.sql_display.setReadOnly(True)
        self.sql_highlighter = SQLHighlighter(self.sql_display.document())
        layout.addWidget(self.sql_display)

        self.validation_label = QLabel("SQL Status: Unknown")
        layout.addWidget(self.validation_label)

        self.sql_preview_tab.setLayout(layout)

    ###########################################################################
    # Connection logic
    ###########################################################################
    def open_connect_dialog(self):
        dlg = ODBCConnectDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            c = dlg.get_connection()
            db_type = dlg.get_db_type()
            if c and db_type and db_type.upper()=="TERADATA":
                alias = f"{db_type}_{len(self.connections)+1}"
                self.connections[alias] = {"connection": c}
                self.update_connection_status(True, f"{db_type} ({alias})")
                self.load_schema(alias)
            else:
                QMessageBox.warning(self, "Only Teradata", "DSN restricted to Teradata")

    def load_schema(self, alias):
        if alias not in self.connections:
            return
        conn = self.connections[alias]['connection']
        self.schema_tree.connection = conn
        self.schema_tree.populate_top_level()
        self.status_bar.showMessage(f"Schema loaded => {alias}", 3000)

    def refresh_schema(self):
        if self.connections:
            first_key = list(self.connections.keys())[0]
            self.load_schema(first_key)
        else:
            QMessageBox.information(self, "No Connection", "Please connect first.")

    def update_connection_status(self, connected, info=""):
        if connected:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green; }")
            self.server_label.setText(info)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red; }")
            self.server_label.setText("Not Connected")

    ###########################################################################
    # Utility
    ###########################################################################
    def run_sql_query(self):
        sql = self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self, "Empty SQL", "No SQL to run.")
            return
        QMessageBox.information(self, "SQL Execution", f"Executing:\n\n{sql}")

    def filter_schema_tree(self, text):
        for i in range(self.schema_tree.topLevelItemCount()):
            item = self.schema_tree.topLevelItem(i)
            self.filter_item(item, text)

    def filter_item(self, item, txt):
        low_txt = txt.lower()
        match = low_txt in item.text(0).lower()
        child_match = False
        for i in range(item.childCount()):
            child_match = self.filter_item(item.child(i), txt) or child_match
        item.setHidden(not (match or child_match))
        return (match or child_match)

    def handle_item_changed(self, item, col):
        if item.childCount()>0:
            st = item.checkState(0)
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, st)
        else:
            parent = item.parent()
            if parent:
                c = sum(parent.child(i).checkState(0)==Qt.Checked for i in range(parent.childCount()))
                if c==parent.childCount():
                    parent.setCheckState(0, Qt.Checked)
                elif c>0:
                    parent.setCheckState(0, Qt.PartiallyChecked)
                else:
                    parent.setCheckState(0, Qt.Unchecked)
        self.generate_sql()

    def manage_aliases(self):
        QMessageBox.information(self, "Alias mgmt", "Stub alias management")

    ###########################################################################
    # Combine with Full VQB
    ###########################################################################
    def combine_with_full_vqb(self):
        subdlg = SubVQBDialog(self)
        if subdlg.exec_() == QDialog.Accepted:
            op, second_sql = subdlg.getResult()
            citem = CombineQueryItem(op, second_sql, x=300,y=300)
            self.canvas.scene.addItem(citem)
            self.generate_sql()

    def derived_complex(self):
        cols = self.get_selected_columns()
        if not cols:
            QMessageBox.warning(self, "No Columns", "No columns selected. Expand schema, check columns first.")
            return
        dlg = AddDerivedColumnComplexDialog(self, cols)
        if dlg.exec_()==QDialog.Accepted:
            alias, expr = dlg.get_expression_data()
            item = DerivedColumnItem(alias, expr, x=200,y=200)
            self.canvas.scene.addItem(item)
            self.generate_sql()

    ###########################################################################
    # Handle Drop from schema => place table with real columns
    ###########################################################################
    def handle_drop(self, full_name, pos):
        """
        full_name is "DBNAME.tblName" from the schema tree drag.
        We look up table_columns_map to find the real columns.
        If not found, fallback to ["id","col1","col2"].
        """
        if full_name in self.table_columns_map:
            columns = self.table_columns_map[full_name]
        else:
            columns = ["id","col1","col2"]  # fallback
        self.canvas.add_table(full_name, columns, pos)

    ###########################################################################
    # Panels usage
    ###########################################################################
    def get_selected_columns(self):
        """
        Return checked 'table.column' from the schema tree.
        """
        cols=[]
        for i in range(self.schema_tree.topLevelItemCount()):
            conn_item = self.schema_tree.topLevelItem(i)
            for j in range(conn_item.childCount()):
                db_item = conn_item.child(j)
                if db_item.data(0,Qt.UserRole)=="database":
                    for k in range(db_item.childCount()):
                        tbl_item = db_item.child(k)
                        if tbl_item.data(0,Qt.UserRole)=="table":
                            for l in range(tbl_item.childCount()):
                                col_item = tbl_item.child(l)
                                if col_item.data(0,Qt.UserRole)=="column" and col_item.checkState(0)==Qt.Checked:
                                    tname = f"{db_item.text(0)}.{tbl_item.text(0)}"
                                    c = col_item.text(0)
                                    cols.append(f"{tname}.{c}")
        return cols

    def get_selected_tables(self):
        """
        Return the alias keys for the tables on the canvas (in self.canvas.table_items).
        """
        return list(self.canvas.table_items.keys())

    def get_all_possible_columns_for_dialog(self):
        """
        Return the columns from the checked items or from the table_columns_map
        if you want. Here we just do the same as get_selected_columns for no-freehand approach.
        """
        return self.get_selected_columns()

    ###########################################################################
    # Operation Toggle
    ###########################################################################
    def toggle_operation_mode(self):
        idx = self.operation_combo.currentIndex()
        if idx==0:
            self.operation_mode="SELECT"
            if self.canvas.operation_red_line:
                self.canvas.scene.removeItem(self.canvas.operation_red_line)
                self.canvas.operation_red_line=None
            if self.canvas.complete_query_item:
                self.canvas.scene.removeItem(self.canvas.complete_query_item)
                self.canvas.complete_query_item=None
            if self.canvas.target_table_item:
                self.canvas.scene.removeItem(self.canvas.target_table_item)
                self.canvas.target_table_item=None
            self.canvas.clear_mapping_lines()
        else:
            mode = self.operation_combo.currentText()
            self.operation_mode = mode
            self.activate_operation_mode(mode)
        self.generate_sql()

    def activate_operation_mode(self, mode):
        self.canvas.add_vertical_red_line(450)
        col_list = self.build_select_list_for_display()
        self.canvas.add_complete_query_item(col_list, x=100,y=200)

    def build_select_list_for_display(self):
        selected_cols = self.get_selected_columns()
        derived_items = [it for it in self.canvas.scene.items() if isinstance(it, DerivedColumnItem)]
        derived_selects = [f"{d.expression} AS {d.alias}" for d in derived_items]

        ags = self.group_by_panel.get_aggregates()
        agg_selects = [f"{f}({c}) AS {a}" for (f,c,a) in ags]

        final_list = list(selected_cols)+derived_selects+agg_selects
        if not final_list:
            final_list=["*"]
        return final_list

    ###########################################################################
    # Generate & Validate SQL
    ###########################################################################
    def generate_sql(self):
        scene_items = self.canvas.scene.items()
        derived_items = [it for it in scene_items if isinstance(it, DerivedColumnItem)]
        combine_items = [it for it in scene_items if isinstance(it, CombineQueryItem)]

        if self.operation_mode=="INSERT":
            sql = self._generate_insert_sql(derived_items)
        elif self.operation_mode=="UPDATE":
            sql = self._generate_update_sql(derived_items)
        elif self.operation_mode=="DELETE":
            sql = self._generate_delete_sql()
        else:
            sql = self._generate_select_sql(derived_items, combine_items)

        self.sql_display.setPlainText(sql)
        self.validation_label.setText("SQL Status: Generated")
        self.validation_label.setStyleSheet("color: green;")
        self.validate_sql()

    def validate_sql(self):
        txt = self.sql_display.toPlainText().strip()
        if not txt:
            self.validation_label.setText("SQL Status: No SQL.")
            self.validation_label.setStyleSheet("color: orange;")
            return
        try:
            parser = SQLParser(txt)
            parser.parse()
            self.validation_label.setText("SQL Status: Valid.")
            self.validation_label.setStyleSheet("color: green;")
        except Exception as e:
            self.validation_label.setText(f"SQL Status: Invalid - {e}")
            self.validation_label.setStyleSheet("color: red;")

    def _generate_select_sql(self, derived_items, combine_items):
        tbls = self.get_selected_tables()
        if not tbls:
            return "-- No tables => no SELECT."
        dcols = [f"{d.expression} AS {d.alias}" for d in derived_items]
        scols = self.get_selected_columns()
        if not scols and not dcols:
            selects = ["*"]
        else:
            selects = list(scols)+list(dcols)

        # incorporate aggregates
        ags = self.group_by_panel.get_aggregates()
        for (f,c,a) in ags:
            selects.append(f"{f}({c}) AS {a}")

        lines=[]
        lines.append("SELECT " + ", ".join(selects))
        lines.append("FROM " + tbls[0])

        for jdict in self.joins:
            lines.append(f"{jdict['type']} {jdict['table2']} ON {jdict['condition']}")

        wfs = self.filter_panel.get_filters("WHERE")
        if wfs:
            conds = [f"{col} {op} {val}" for (col,op,val) in wfs]
            lines.append("WHERE " + " AND ".join(conds))

        gb = self.group_by_panel.get_group_by()
        if gb:
            lines.append("GROUP BY " + ", ".join(gb))

        hv = self.filter_panel.get_filters("HAVING")
        if hv:
            conds = [f"{col} {op} {val}" for (col,op,val) in hv]
            lines.append("HAVING " + " AND ".join(conds))

        obys = self.sort_limit_panel.get_order_bys()
        if obys:
            lines.append("ORDER BY " + ", ".join(obys))

        lm = self.sort_limit_panel.get_limit()
        if lm is not None:
            lines.append(f"LIMIT {lm}")
        off = self.sort_limit_panel.get_offset()
        if off is not None:
            lines.append(f"OFFSET {off}")

        final_sql="\n".join(lines)
        if combine_items:
            citem = combine_items[0]
            final_sql = f"{final_sql}\n{citem.operator}\n(\n{citem.second_sql}\n)"
        return final_sql

    def _generate_insert_sql(self, derived_items):
        return "-- Insert logic stub..."

    def _generate_update_sql(self, derived_items):
        return "-- Update logic stub..."

    def _generate_delete_sql(self):
        return "-- Delete logic stub..."


###############################################################################
# DerivedColumnComplexDialog (Optional)
###############################################################################
class AddDerivedColumnComplexDialog(QDialog):
    def __init__(self, parent=None, available_columns=None):
        super().__init__(parent)
        self.setWindowTitle("Complex Derived Column Builder")
        self.available_columns = available_columns or []
        self.expression_tokens = []
        self.alias = None

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.expression_preview = QLineEdit()
        self.expression_preview.setReadOnly(True)
        form.addRow("Expression Preview:", self.expression_preview)

        layout.addLayout(form)

        token_layout = QHBoxLayout()

        self.col_combo = QComboBox()
        self.col_combo.addItems(["(Pick Col)"]+self.available_columns)
        self.col_btn = create_text_button("Add Column")
        self.col_btn.clicked.connect(self.add_column_token)
        token_layout.addWidget(self.col_combo)
        token_layout.addWidget(self.col_btn)

        self.op_combo = QComboBox()
        self.op_combo.addItems(["+","-","*","/","=","<",">","<=",">=","<>"])
        self.op_btn = create_text_button("Add Operator")
        self.op_btn.clicked.connect(self.add_operator_token)
        token_layout.addWidget(self.op_combo)
        token_layout.addWidget(self.op_btn)

        self.func_combo = QComboBox()
        self.func_combo.addItems(["UPPER","LOWER","ABS","COALESCE","SUBSTR"])
        self.func_btn = create_text_button("Add Function(...)")
        self.func_btn.clicked.connect(self.add_function_token)
        token_layout.addWidget(self.func_combo)
        token_layout.addWidget(self.func_btn)

        self.paren_open_btn = create_text_button("(")
        self.paren_open_btn.clicked.connect(lambda: self.add_token("("))
        self.paren_close_btn = create_text_button(")")
        self.paren_close_btn.clicked.connect(lambda: self.add_token(")"))
        token_layout.addWidget(self.paren_open_btn)
        token_layout.addWidget(self.paren_close_btn)

        self.undo_btn = create_text_button("Undo Last")
        self.undo_btn.clicked.connect(self.remove_last_token)
        token_layout.addWidget(self.undo_btn)

        layout.addLayout(token_layout)

        alias_form = QFormLayout()
        self.alias_edit = QLineEdit()
        alias_form.addRow("Alias:", self.alias_edit)
        layout.addLayout(alias_form)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def add_column_token(self):
        col = self.col_combo.currentText()
        if col and col!="(Pick Col)":
            self.add_token(col)

    def add_operator_token(self):
        op = self.op_combo.currentText()
        self.add_token(op)

    def add_function_token(self):
        fn = self.func_combo.currentText()
        if fn:
            self.add_token(fn + "(")

    def add_token(self, token):
        self.expression_tokens.append(token)
        self.refresh_preview()

    def remove_last_token(self):
        if self.expression_tokens:
            self.expression_tokens.pop()
        self.refresh_preview()

    def refresh_preview(self):
        self.expression_preview.setText(" ".join(self.expression_tokens))

    def on_ok(self):
        if not self.expression_tokens:
            QMessageBox.warning(self, "No Expression", "No tokens in expression.")
            return
        a = self.alias_edit.text().strip()
        if not a:
            QMessageBox.warning(self, "Missing Alias", "Alias is required.")
            return
        self.alias = a
        self.accept()

    def get_expression_data(self):
        expr = "".join(self.expression_tokens)
        return (self.alias, expr)


###############################################################################
# Main Launch
###############################################################################
if __name__=="__main__":
    app = QApplication(sys.argv)
    main_window = QMainWindow()
    builder_tab = VisualQueryBuilderTab(parent=main_window)
    main_window.setCentralWidget(builder_tab)
    main_window.setWindowTitle("VQB - Real Columns, No Crash on Remove Table")
    main_window.resize(1200,800)
    main_window.show()
    sys.exit(app.exec_())
