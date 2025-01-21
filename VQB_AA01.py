#!/usr/bin/env python
import sys
import os
import traceback
import pickle
import pyodbc
import sqlparse

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import (
    Qt, QPointF, QTimer, QRegExp, QThreadPool, QRunnable, pyqtSignal, QObject
)
from PyQt5.QtGui import (
    QColor, QCursor, QPen, QBrush, QFont, QSyntaxHighlighter, QTextCharFormat, QDrag
)
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QPushButton, QSplitter,
    QLineEdit, QLabel, QDialog, QFormLayout, QComboBox, QTableWidget,
    QTableWidgetItem, QTabWidget, QMessageBox, QGraphicsView,
    QGraphicsScene, QGraphicsRectItem, QGraphicsTextItem, QGraphicsItem,
    QProgressBar, QDialogButtonBox, QStatusBar, QGroupBox, QAbstractItemView,
    QSpinBox, QFileDialog, QListWidget, QStyle, QMenu, QFrame
)

# Enable pyodbc connection pooling for Teradata
pyodbc.pooling = True


###############################################################################
# Simple Button Helper
###############################################################################
def create_text_button(text: str, tooltip: str = "") -> QPushButton:
    btn = QPushButton(text)
    btn.setToolTip(tooltip)
    return btn


###############################################################################
# ODBCConnectDialog (Teradata Only)
###############################################################################
class ODBCConnectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to Teradata (ODBC)")
        self.resize(400, 230)
        self._conn = None
        self._db_type = None

        layout = QVBoxLayout(self)

        # Database Type (fixed to Teradata)
        layout.addWidget(QLabel("Database Type (Fixed to Teradata):"))
        self.type_label = QLabel("Teradata")
        layout.addWidget(self.type_label)

        # DSN selection
        layout.addWidget(QLabel("ODBC DSN (Teradata Only):"))
        self.dsn_combo = QComboBox()
        if pyodbc:
            try:
                dsn_map = pyodbc.dataSources()
                for dsn in sorted(dsn_map.keys()):
                    self.dsn_combo.addItem(dsn)
            except:
                pass
        layout.addWidget(self.dsn_combo)

        # User/Password
        layout.addWidget(QLabel("Username (optional):"))
        self.user_edit = QLineEdit()
        layout.addWidget(self.user_edit)

        layout.addWidget(QLabel("Password (optional):"))
        self.pass_edit = QLineEdit()
        self.pass_edit.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.pass_edit)

        # OK/Cancel
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def on_ok(self):
        if not pyodbc:
            QMessageBox.critical(self, "pyodbc missing", "pyodbc is not installed.")
            return

        dsn = self.dsn_combo.currentText().strip()
        if not dsn:
            QMessageBox.warning(self, "Missing DSN", "Please pick a DSN first.")
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
# Background Worker for Lazy Load
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
            err = (
                f"Error loading tables for '{self.database_name}': {e}\n"
                f"{traceback.format_exc()}"
            )
            self.signals.error.emit(err)


###############################################################################
# LazySchemaTreeWidget
###############################################################################
class LazySchemaTreeWidget(QTreeWidget):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.setHeaderHidden(True)
        self.setDragEnabled(True)
        self.setSelectionMode(QAbstractItemView.SingleSelection)
        self.threadpool = QThreadPool.globalInstance()
        self.itemExpanded.connect(self.on_item_expanded)
        self.populate_top_level()

    def populate_top_level(self):
        self.clear()
        conn_name = "Teradata"
        if self.connection:
            try:
                name_check = self.connection.getinfo(pyodbc.SQL_DBMS_NAME).strip()
                if "TERADATA" in name_check.upper():
                    conn_name = name_check
            except:
                pass
        else:
            conn_name = "Not Connected"

        conn_item = QTreeWidgetItem([conn_name])
        conn_item.setData(0, Qt.UserRole, "connection")
        self.addTopLevelItem(conn_item)

        if not self.connection:
            return

        # Attempt to fetch database names
        cursor = self.connection.cursor()
        db_names = []
        try:
            cursor.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
            db_names = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("[ERROR] Failed to fetch DB names:", e)

        if not db_names:
            no_db_item = QTreeWidgetItem(["<No databases found>"])
            conn_item.addChild(no_db_item)
            return

        for db in db_names:
            db_item = QTreeWidgetItem([db])
            db_item.setData(0, Qt.UserRole, "database")
            db_item.setData(0, Qt.UserRole + 1, False)
            dummy = QTreeWidgetItem(["Loading..."])
            db_item.addChild(dummy)
            conn_item.addChild(db_item)

        self.expandItem(conn_item)

    def on_item_expanded(self, item):
        data_type = item.data(0, Qt.UserRole)
        loaded_flag = item.data(0, Qt.UserRole + 1)

        if data_type == "database" and not loaded_flag:
            item.takeChildren()
            db_name = item.text(0)
            worker = LazySchemaLoaderWorker(self.connection, db_name)
            worker.signals.finished.connect(lambda tbls, it=item: self.populate_database_node(it, tbls))
            worker.signals.error.connect(self.handle_error)
            self.threadpool.start(worker)

        elif data_type == "table" and not loaded_flag:
            # Load columns from DBC.ColumnsV
            item.takeChildren()
            db_name = item.parent().text(0)
            table_name = item.text(0)
            columns = self.load_columns_for_table(db_name, table_name)
            if columns:
                for col in columns:
                    col_item = QTreeWidgetItem([col])
                    col_item.setData(0, Qt.UserRole, "column")
                    # We allow user check/uncheck columns
                    col_item.setFlags(col_item.flags() | Qt.ItemIsUserCheckable)
                    col_item.setCheckState(0, Qt.Unchecked)
                    item.addChild(col_item)
            else:
                item.addChild(QTreeWidgetItem(["<No columns found>"]))
            item.setData(0, Qt.UserRole + 1, True)

    def populate_database_node(self, db_item, tables):
        if not tables:
            db_item.addChild(QTreeWidgetItem(["<No tables found>"]))
            db_item.setData(0, Qt.UserRole + 1, True)
            return

        for tbl in tables:
            tbl_item = QTreeWidgetItem([tbl])
            tbl_item.setData(0, Qt.UserRole, "table")
            tbl_item.setData(0, Qt.UserRole + 1, False)
            dummy = QTreeWidgetItem(["Loading columns..."])
            tbl_item.addChild(dummy)
            db_item.addChild(tbl_item)

        db_item.setData(0, Qt.UserRole + 1, True)

    def load_columns_for_table(self, db_name, table_name):
        columns = []
        if not self.connection:
            return columns
        try:
            cursor = self.connection.cursor()
            query = f"""
                SELECT ColumnName
                FROM DBC.ColumnsV
                WHERE DatabaseName='{db_name}' AND TableName='{table_name}'
                ORDER BY ColumnId
            """
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [row[0] for row in results]
        except Exception as e:
            print(f"[ERROR] Failed to load columns for {db_name}.{table_name}: {e}")
        return columns

    def handle_error(self, msg):
        QMessageBox.critical(self, "Schema Load Error", msg)

    def startDrag(self, supportedActions):
        item = self.currentItem()
        if item and item.parent() and item.data(0, Qt.UserRole) == "table":
            drag = QDrag(self)
            mime = QtCore.QMimeData()
            mime.setText(item.text(0))  # table name
            drag.setMimeData(mime)
            drag.exec_(supportedActions)


###############################################################################
# SQL Parser Stub
###############################################################################
class SQLParser:
    def __init__(self, sql):
        self.sql = sql
        self.parsed = None
        self.alias_map = {}
        self.tables = []
        self.joins = []
        self.select_columns = []
        self.derived_columns = []
        self.where_conditions = []
        self.group_by = []
        self.having_conditions = []
        self.order_by = []
        self.limit = None
        self.offset = None
        self.subqueries = []

    def parse(self):
        if not self.sql.strip():
            raise ValueError("SQLParser Error: SQL string is empty.")
        try:
            statements = sqlparse.parse(self.sql)
            if not statements:
                raise ValueError("No valid SQL found.")
            self.parsed = statements[0]
        except Exception as e:
            raise ValueError(f"Error parsing SQL: {e}")
        self.extract_components(self.parsed)
        return {
            "alias_map": self.alias_map,
            "tables": self.tables,
            "joins": self.joins,
            "select_columns": self.select_columns,
            "derived_columns": self.derived_columns,
            "where_conditions": self.where_conditions,
            "group_by": self.group_by,
            "having_conditions": self.having_conditions,
            "order_by": self.order_by,
            "limit": self.limit,
            "offset": self.offset,
            "subqueries": self.subqueries,
        }

    def extract_components(self, token_list, parent_alias=None):
        tokens = [str(tok).strip() for tok in token_list.tokens if str(tok).strip()]
        if "FROM" in tokens:
            idx = tokens.index("FROM")
            if len(tokens) > idx + 1:
                self.tables.append(tokens[idx + 1])
        if "SELECT" in tokens:
            idx = tokens.index("SELECT")
            if len(tokens) > idx + 1:
                self.select_columns.append(tokens[idx + 1])


###############################################################################
# SQL Highlighter
###############################################################################
class SQLHighlighter(QSyntaxHighlighter):
    def __init__(self, document):
        super().__init__(document)
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
            index = pattern.indexIn(text)
            while index >= 0:
                length = pattern.matchedLength()
                self.setFormat(index, length, fmt)
                index = pattern.indexIn(text, index + length)
        self.setCurrentBlockState(0)


###############################################################################
# JoinLine
###############################################################################
class JoinLine(QtWidgets.QGraphicsLineItem):
    def __init__(self, start_item, end_item, join_type="INNER JOIN", condition="", is_subquery=False):
        super().__init__()
        self.start_item = start_item
        self.end_item = end_item
        self.join_type = join_type
        self.condition = condition
        self.is_subquery = is_subquery

        pen = QPen(Qt.black, 2)
        if is_subquery:
            pen.setStyle(Qt.DashLine)
            pen.setColor(Qt.darkGray)
        self.setPen(pen)
        self.setZValue(-1)

        self.label = QGraphicsTextItem(join_type, self)
        self.label.setDefaultTextColor(Qt.blue)
        self.label.setZValue(1)

        self.update_position()

    def update_position(self):
        start = self.start_item.scenePos() + QPointF(
            self.start_item.rect().width() / 2,
            self.start_item.rect().height() / 2
        )
        end = self.end_item.scenePos() + QPointF(
            self.end_item.rect().width() / 2,
            self.end_item.rect().height() / 2
        )
        self.setLine(QtCore.QLineF(start, end))
        mid_x = (start.x() + end.x()) / 2
        mid_y = (start.y() + end.y()) / 2
        self.label.setPos(mid_x, mid_y)


###############################################################################
# Additional Canvas Items
###############################################################################
class DerivedColumnItem(QGraphicsRectItem):
    def __init__(self, alias, expression, x=0, y=0):
        super().__init__(0, 0, 220, 60)
        self.alias = alias
        self.expression = expression
        self.setPos(x, y)
        self.setBrush(QBrush(QColor(255, 230, 200)))
        self.setPen(QPen(Qt.darkBlue, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        txt = QGraphicsTextItem(f"Derived:\n{alias} = {expression}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


class CombineQueryItem(QGraphicsRectItem):
    def __init__(self, operator, second_sql, x=0, y=0):
        super().__init__(0, 0, 260, 80)
        self.operator = operator
        self.second_sql = second_sql
        self.setPos(x, y)
        self.setBrush(QBrush(QColor(210, 255, 210)))
        self.setPen(QPen(Qt.darkGreen, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        preview = second_sql[:25] + "..." if len(second_sql) > 25 else second_sql
        txt = QGraphicsTextItem(f"Combine:\n{operator}\n{preview}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


###############################################################################
# NEW ENHANCEMENT: Operation Data Mapping
###############################################################################
class CompleteQueryItem(QGraphicsRectItem):
    """
    Displays the columns from the current SELECT. This item is placed on the
    left side of a vertical red line to represent the "source dataset" that
    will feed an INSERT/UPDATE/DELETE.
    """
    def __init__(self, columns, x=0, y=0):
        super().__init__(0, 0, 200, 100)
        self.columns = columns  # list of column names from the SELECT
        self.setPos(x, y)
        self.setBrush(QBrush(QColor(250, 250, 180)))
        self.setPen(QPen(Qt.red, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        # Just display the columns
        col_text = "\n".join(columns) if columns else "<No columns>"
        txt = QGraphicsTextItem(f"Result Columns:\n{col_text}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


class MappedColumnLine(QtWidgets.QGraphicsLineItem):
    """
    A line that connects a column in the CompleteQueryItem (source) to a column
    in the target table. Used to track the column mapping for INSERT/UPDATE/DELETE.
    """
    def __init__(self, source_text_item, target_text_item):
        super().__init__()
        self.source_text_item = source_text_item
        self.target_text_item = target_text_item
        pen = QPen(Qt.darkRed, 2, Qt.SolidLine)
        self.setPen(pen)
        self.setZValue(2)
        self.update_position()

    def update_position(self):
        # We'll connect the center of each text item bounding rect
        src_pos = self.source_text_item.mapToScene(
            self.source_text_item.boundingRect().center()
        )
        tgt_pos = self.target_text_item.mapToScene(
            self.target_text_item.boundingRect().center()
        )
        self.setLine(QtCore.QLineF(src_pos, tgt_pos))


class TargetTableItem(QGraphicsRectItem):
    """
    A rectangle showing the columns of a target table. The user can map
    source columns -> target columns with lines.
    """
    def __init__(self, db_name, table_name, columns, x=0, y=0):
        super().__init__(0, 0, 200, 100)
        self.db_name = db_name
        self.table_name = table_name
        self.columns = columns  # actual column names
        self.setPos(x, y)
        self.setBrush(QBrush(QColor(220, 220, 255)))
        self.setPen(QPen(Qt.black, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        # We'll create child QGraphicsTextItems for each column
        self.text_items = []
        # Title
        title = QGraphicsTextItem(f"Target: {table_name}", self)
        title.setFont(QFont("Arial", 8, QFont.Bold))
        title.setPos(5, 2)
        offset_y = 20
        for col in columns:
            txt_item = QGraphicsTextItem(col, self)
            txt_item.setPos(5, offset_y)
            txt_item.setFont(QFont("Arial", 8))
            offset_y += 15
            self.text_items.append(txt_item)
        rect_height = max(80, offset_y + 5)
        self.setRect(0, 0, 200, rect_height)


###############################################################################
# Filter, GroupBy, Sort Panels - Now with No Freehand Input
###############################################################################
# ENHANCEMENT: We remove direct QInputDialogs. Instead, use dialogs with combos.

class AddFilterDialog(QDialog):
    """
    Dialog that forces user to select a column, an operator, and a value
    from controlled combos (no freehand). In real usage, you'd populate
    columns and possible values from your schema or data samples.
    """
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Filter")
        self.setModal(True)
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

        # For demonstration, a few dummy values
        self.value_combo = QComboBox()
        self.value_combo.addItems(["123", "'ABC'", "'XYZ'", "1000"])
        layout.addRow("Value:", self.value_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept_data)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def accept_data(self):
        self.selected_column = self.column_combo.currentText()
        self.selected_operator = self.operator_combo.currentText()
        self.selected_value = self.value_combo.currentText()
        self.accept()

    def get_filter(self):
        return (self.selected_column, self.selected_operator, self.selected_value)


class FilterPanel(QGroupBox):
    """
    A group box containing a tab widget for WHERE and HAVING filters.
    Now uses AddFilterDialog to avoid freehand input.
    """
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

        # WHERE table
        self.where_layout = QVBoxLayout(self.where_tab)
        self.where_table = QTableWidget(0, 3)
        self.where_table.setHorizontalHeaderLabels(["Column", "Operator", "Value"])
        self.where_table.horizontalHeader().setStretchLastSection(True)
        self.where_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.where_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.where_layout.addWidget(self.where_table)

        wh_btn = QHBoxLayout()
        add_wh = create_text_button("Add WHERE")
        add_wh.clicked.connect(lambda: self.add_filter("WHERE"))
        rem_wh = create_text_button("Remove WHERE")
        rem_wh.clicked.connect(lambda: self.remove_filter("WHERE"))
        wh_btn.addWidget(add_wh)
        wh_btn.addWidget(rem_wh)
        self.where_layout.addLayout(wh_btn)

        # HAVING table
        self.having_layout = QVBoxLayout(self.having_tab)
        self.having_table = QTableWidget(0, 3)
        self.having_table.setHorizontalHeaderLabels(["Column", "Operator", "Value"])
        self.having_table.horizontalHeader().setStretchLastSection(True)
        self.having_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.having_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.having_layout.addWidget(self.having_table)

        hv_btn = QHBoxLayout()
        add_hv = create_text_button("Add HAVING")
        add_hv.clicked.connect(lambda: self.add_filter("HAVING"))
        rem_hv = create_text_button("Remove HAVING")
        rem_hv.clicked.connect(lambda: self.remove_filter("HAVING"))
        hv_btn.addWidget(add_hv)
        hv_btn.addWidget(rem_hv)
        self.having_layout.addLayout(hv_btn)

    def add_filter(self, clause):
        # We'll gather columns from the builder's "get_selected_columns" or all columns we know
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns available for filtering.")
            return

        dlg = AddFilterDialog(columns, self)
        if dlg.exec_() == QDialog.Accepted:
            col, op, val = dlg.get_filter()
            table = self.where_table if clause == "WHERE" else self.having_table
            row = table.rowCount()
            table.insertRow(row)
            table.setItem(row, 0, QTableWidgetItem(col))
            table.setItem(row, 1, QTableWidgetItem(op))
            table.setItem(row, 2, QTableWidgetItem(val))
            self.builder.generate_sql()

    def remove_filter(self, clause):
        table = self.where_table if clause == "WHERE" else self.having_table
        selected_rows = sorted(
            [idx.row() for idx in table.selectionModel().selectedRows()],
            reverse=True
        )
        for row in selected_rows:
            table.removeRow(row)
        self.builder.generate_sql()

    def get_filters(self, clause):
        table = self.where_table if clause == "WHERE" else self.having_table
        fltrs = []
        for r in range(table.rowCount()):
            col = table.item(r, 0).text()
            op = table.item(r, 1).text()
            val = table.item(r, 2).text()
            fltrs.append((col, op, val))
        return fltrs


class AddGroupByDialog(QDialog):
    """
    Forces user to pick from a known set of columns.
    """
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add GroupBy")
        self.setModal(True)
        self.selected_column = None

        layout = QVBoxLayout(self)

        self.column_combo = QComboBox()
        self.column_combo.addItems(available_columns)
        layout.addWidget(QLabel("Choose column to Group By:"))
        layout.addWidget(self.column_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept_data)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def accept_data(self):
        self.selected_column = self.column_combo.currentText()
        self.accept()

    def get_column(self):
        return self.selected_column


class AddAggregateDialog(QDialog):
    """
    Forces user to pick function, column, alias from combos or minimal free text for alias if desired.
    But we will demonstrate forced combos for function, column, and a small set for alias.
    """
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Aggregate")
        self.setModal(True)
        self.selected_func = None
        self.selected_col = None
        self.selected_alias = None

        layout = QFormLayout(self)

        self.func_combo = QComboBox()
        self.func_combo.addItems(["COUNT", "SUM", "AVG", "MIN", "MAX"])
        layout.addRow("Function:", self.func_combo)

        self.col_combo = QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addRow("Column:", self.col_combo)

        # We'll allow a few possible aliases for demonstration
        self.alias_combo = QComboBox()
        self.alias_combo.addItems(["AggVal", "AggResult", "MyAgg"])
        layout.addRow("Alias:", self.alias_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept_data)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def accept_data(self):
        self.selected_func = self.func_combo.currentText()
        self.selected_col = self.col_combo.currentText()
        self.selected_alias = self.alias_combo.currentText()
        self.accept()

    def get_aggregate(self):
        return (self.selected_func, self.selected_col, self.selected_alias)


class GroupByPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Group By and Aggregates", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

        # Group By table
        self.group_by_table = QTableWidget(0, 1)
        self.group_by_table.setHorizontalHeaderLabels(["Group By Columns"])
        self.group_by_table.horizontalHeader().setStretchLastSection(True)
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

        # Aggregates table
        self.aggregates_table = QTableWidget(0, 3)
        self.aggregates_table.setHorizontalHeaderLabels(["Function", "Column", "Alias"])
        self.aggregates_table.horizontalHeader().setStretchLastSection(True)
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
        if dlg.exec_() == QDialog.Accepted:
            col = dlg.get_column()
            if col:
                row = self.group_by_table.rowCount()
                self.group_by_table.insertRow(row)
                self.group_by_table.setItem(row, 0, QTableWidgetItem(col))
                self.builder.generate_sql()

    def remove_group_by(self):
        selected_rows = sorted(
            [idx.row() for idx in self.group_by_table.selectionModel().selectedRows()],
            reverse=True
        )
        for row in selected_rows:
            self.group_by_table.removeRow(row)
        self.builder.generate_sql()

    def add_aggregate(self):
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns available.")
            return
        dlg = AddAggregateDialog(columns, self)
        if dlg.exec_() == QDialog.Accepted:
            func, col, alias = dlg.get_aggregate()
            row = self.aggregates_table.rowCount()
            self.aggregates_table.insertRow(row)
            self.aggregates_table.setItem(row, 0, QTableWidgetItem(func))
            self.aggregates_table.setItem(row, 1, QTableWidgetItem(col))
            self.aggregates_table.setItem(row, 2, QTableWidgetItem(alias))
            self.builder.generate_sql()

    def remove_aggregate(self):
        selected_rows = sorted(
            [idx.row() for idx in self.aggregates_table.selectionModel().selectedRows()],
            reverse=True
        )
        for row in selected_rows:
            self.aggregates_table.removeRow(row)
        self.builder.generate_sql()

    def open_context_menu(self, pos):
        menu = QMenu()
        edit_action = menu.addAction("Edit")
        delete_action = menu.addAction("Delete")

        act = menu.exec_(self.aggregates_table.viewport().mapToGlobal(pos))
        if act == edit_action:
            sel = self.aggregates_table.selectedItems()
            if not sel:
                return
            row = sel[0].row()
            current_func = self.aggregates_table.item(row, 0).text()
            current_col = self.aggregates_table.item(row, 1).text()
            current_alias = self.aggregates_table.item(row, 2).text()

            columns = self.builder.get_all_possible_columns_for_dialog()
            dlg = AddAggregateDialog(columns, self)
            # Prefill combos (not shown here for brevity)
            if dlg.exec_() == QDialog.Accepted:
                func, col, alias = dlg.get_aggregate()
                self.aggregates_table.setItem(row, 0, QTableWidgetItem(func))
                self.aggregates_table.setItem(row, 1, QTableWidgetItem(col))
                self.aggregates_table.setItem(row, 2, QTableWidgetItem(alias))
                self.builder.generate_sql()

        elif act == delete_action:
            self.remove_aggregate()

    def get_group_by(self):
        cols = []
        for r in range(self.group_by_table.rowCount()):
            cols.append(self.group_by_table.item(r, 0).text())
        return cols

    def get_aggregates(self):
        aggs = []
        for r in range(self.aggregates_table.rowCount()):
            func = self.aggregates_table.item(r, 0).text()
            col = self.aggregates_table.item(r, 1).text()
            alias = self.aggregates_table.item(r, 2).text()
            aggs.append((func, col, alias))
        return aggs


class AddSortDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Sort")
        self.setModal(True)
        self.selected_column = None
        self.selected_direction = None

        layout = QFormLayout()

        self.col_combo = QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addRow("Sort Column:", self.col_combo)

        self.dir_combo = QComboBox()
        self.dir_combo.addItems(["ASC", "DESC"])
        layout.addRow("Direction:", self.dir_combo)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept_data)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def accept_data(self):
        self.selected_column = self.col_combo.currentText()
        self.selected_direction = self.dir_combo.currentText()
        self.accept()

    def get_sort_info(self):
        return (self.selected_column, self.selected_direction)


class SortLimitPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Sort and Limit", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

        # Sort table
        self.sort_table = QTableWidget(0, 2)
        self.sort_table.setHorizontalHeaderLabels(["Column", "Direction"])
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

        # Limit/Offset
        limit_layout = QHBoxLayout()
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(0, 9999999)
        self.limit_spin.setValue(0)
        self.limit_spin.setSuffix(" (Limit)")
        self.limit_spin.setSpecialValueText("No Limit")
        self.limit_spin.valueChanged.connect(lambda _: self.builder.generate_sql())
        limit_layout.addWidget(self.limit_spin)

        self.offset_spin = QSpinBox()
        self.offset_spin.setRange(0, 9999999)
        self.offset_spin.setValue(0)
        self.offset_spin.setSuffix(" (Offset)")
        self.offset_spin.setSpecialValueText("No Offset")
        self.offset_spin.valueChanged.connect(lambda _: self.builder.generate_sql())
        limit_layout.addWidget(self.offset_spin)

        layout.addLayout(limit_layout)

    def add_sort_dialog(self):
        columns = self.builder.get_all_possible_columns_for_dialog()
        if not columns:
            QMessageBox.warning(self, "No Columns", "No columns available to sort.")
            return
        dlg = AddSortDialog(columns, self)
        if dlg.exec_() == QDialog.Accepted:
            col, direction = dlg.get_sort_info()
            row = self.sort_table.rowCount()
            self.sort_table.insertRow(row)
            self.sort_table.setItem(row, 0, QTableWidgetItem(col))
            self.sort_table.setItem(row, 1, QTableWidgetItem(direction))
            self.builder.generate_sql()

    def remove_sort(self):
        selected_rows = sorted(
            [idx.row() for idx in self.sort_table.selectionModel().selectedRows()],
            reverse=True
        )
        for row in selected_rows:
            self.sort_table.removeRow(row)
        self.builder.generate_sql()

    def open_context_menu(self, pos):
        menu = QMenu()
        edit_action = menu.addAction("Edit")
        delete_action = menu.addAction("Delete")
        act = menu.exec_(self.sort_table.viewport().mapToGlobal(pos))

        if act == edit_action:
            sel = self.sort_table.selectedItems()
            if not sel:
                return
            row = sel[0].row()
            columns = self.builder.get_all_possible_columns_for_dialog()
            dlg = AddSortDialog(columns, self)
            # Prefill combos (not shown) for brevity
            if dlg.exec_() == QDialog.Accepted:
                col, direction = dlg.get_sort_info()
                self.sort_table.setItem(row, 0, QTableWidgetItem(col))
                self.sort_table.setItem(row, 1, QTableWidgetItem(direction))
                self.builder.generate_sql()
        elif act == delete_action:
            self.remove_sort()

    def get_order_bys(self):
        orders = []
        for r in range(self.sort_table.rowCount()):
            col = self.sort_table.item(r, 0).text()
            direction = self.sort_table.item(r, 1).text()
            orders.append(f"{col} {direction}")
        return orders

    def get_limit(self):
        val = self.limit_spin.value()
        return val if val > 0 else None

    def get_offset(self):
        val = self.offset_spin.value()
        return val if val > 0 else None


###############################################################################
# Window Function, Combine Queries, etc.
###############################################################################
class CombineQueriesDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Combine Queries")
        self.resize(600, 400)
        self.operator = None
        self.second_sql = None
        main_layout = QVBoxLayout(self)

        form = QFormLayout()
        self.operator_combo = QComboBox()
        self.operator_combo.addItems(["UNION", "UNION ALL", "INTERSECT", "EXCEPT"])
        form.addRow("Operator:", self.operator_combo)
        main_layout.addLayout(form)

        self.sql_text_edit = QTextEdit()
        self.sql_text_edit.setPlaceholderText("Enter the second SELECT query here...")
        main_layout.addWidget(self.sql_text_edit)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.validate_and_accept)
        btns.rejected.connect(self.reject)
        main_layout.addWidget(btns)
        self.setLayout(main_layout)

    def validate_and_accept(self):
        op = self.operator_combo.currentText().strip()
        second_sql = self.sql_text_edit.toPlainText().strip()
        if not second_sql or not op:
            QMessageBox.warning(self, "Input Error", "Both operator and second SQL are required.")
            return
        if not second_sql.upper().startswith("SELECT"):
            QMessageBox.warning(self, "Input Error", "The second query must begin with 'SELECT'.")
            return
        self.operator = op
        self.second_sql = second_sql
        self.accept()

    def get_data(self):
        return self.operator, self.second_sql


class WindowFunctionDialog(QDialog):
    def __init__(self, parent=None, available_columns=[]):
        super().__init__(parent)
        self.setWindowTitle("Add/Edit Window Function")
        self.setModal(True)
        self.resize(500, 400)
        self.available_columns = available_columns
        self.function = None
        self.alias = None
        self.final_expression = None

        main_layout = QVBoxLayout(self)
        form = QFormLayout()

        self.function_combo = QComboBox()
        self.function_combo.addItems(["ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG", "LEAD"])
        form.addRow("Function:", self.function_combo)

        self.partition_label = QLabel("Partition Columns (multi-select):")
        self.partition_list = QListWidget()
        self.partition_list.addItems(self.available_columns)
        self.partition_list.setSelectionMode(QAbstractItemView.MultiSelection)

        self.order_label = QLabel("Order Columns (multi-select):")
        self.order_list = QListWidget()
        self.order_list.addItems(self.available_columns)
        self.order_list.setSelectionMode(QAbstractItemView.MultiSelection)

        self.desc_checkbox = QtWidgets.QCheckBox("Order Descending?")
        self.desc_checkbox.setChecked(False)

        self.alias_edit = QLineEdit()
        self.alias_edit.setPlaceholderText("Enter alias for window function")

        form.addRow(self.partition_label, self.partition_list)
        form.addRow(self.order_label, self.order_list)
        form.addRow("Descending?", self.desc_checkbox)
        form.addRow("Alias:", self.alias_edit)
        main_layout.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.validate_and_accept)
        btns.rejected.connect(self.reject)
        main_layout.addWidget(btns)
        self.setLayout(main_layout)

    def validate_and_accept(self):
        fn = self.function_combo.currentText()
        part_cols = [item.text() for item in self.partition_list.selectedItems()]
        order_cols = [item.text() for item in self.order_list.selectedItems()]
        desc = self.desc_checkbox.isChecked()
        alias = self.alias_edit.text().strip()

        if not alias:
            QMessageBox.warning(self, "Input Error", "Alias is required.")
            return

        partition_str = ", ".join(part_cols) if part_cols else ""
        order_str = ", ".join(order_cols) if order_cols else ""
        order_clause = ""
        if order_str:
            order_clause = f"ORDER BY {order_str} {'DESC' if desc else ''}".strip()

        over_parts = []
        if partition_str:
            over_parts.append(f"PARTITION BY {partition_str}")
        if order_clause:
            over_parts.append(order_clause)

        if over_parts:
            over_expr = " OVER (" + " ".join(over_parts) + ")"
        else:
            over_expr = " OVER ()"

        self.final_expression = f"{fn}(){over_expr}"
        self.function = fn
        self.alias = alias
        self.accept()

    def get_expression(self):
        return self.alias, self.final_expression


class AddDerivedColumnDialog(QDialog):
    def __init__(self, parent=None, available_columns=[]):
        super().__init__(parent)
        self.setWindowTitle("Add/Edit Derived Column")
        self.setModal(True)
        self.resize(600, 400)
        self.available_columns = available_columns
        self.alias = None
        self.expression = None

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("e.g. TotalPrice")
        form.addRow("Column Name (Alias):", self.name_edit)

        # Note: This is the only place we keep freehand as requested
        self.expr_edit = QLineEdit()
        self.expr_edit.setPlaceholderText("Enter SQL expression (freehand allowed)")
        form.addRow("Expression:", self.expr_edit)

        layout.addLayout(form)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.validate_and_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def validate_and_accept(self):
        self.alias = self.name_edit.text().strip()
        self.expression = self.expr_edit.text().strip()

        if not self.alias or not self.expression:
            QMessageBox.warning(self, "Input Error", "Both alias and expression are required.")
            return
        if self.expression.count("(") != self.expression.count(")"):
            QMessageBox.warning(self, "Input Error", "Unbalanced parentheses in expression.")
            return
        self.accept()

    def get_data(self):
        return self.alias, self.expression


###############################################################################
# Alias Management
###############################################################################
class AliasManagementDialog(QDialog):
    def __init__(self, parent=None, current_aliases=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Aliases")
        self.current_aliases = current_aliases or []
        self.new_aliases = {}

        layout = QVBoxLayout(self)

        self.list_widget = QListWidget()
        for alias in self.current_aliases:
            self.list_widget.addItem(alias)
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()
        edit_btn = QPushButton("Edit Alias")
        edit_btn.clicked.connect(self.edit_alias)
        btn_layout.addWidget(edit_btn)
        layout.addLayout(btn_layout)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def edit_alias(self):
        item = self.list_widget.currentItem()
        if not item:
            return
        old_alias = item.text()
        new_alias, ok = QInputDialog.getText(self, "Edit Alias", "Enter new alias:", text=old_alias)
        if ok and new_alias.strip() and new_alias != old_alias:
            item.setText(new_alias)
            self.new_aliases[old_alias] = new_alias

    def get_alias_mapping(self):
        return self.new_aliases


###############################################################################
# SQL Import Tab (Stub)
###############################################################################
class SQLImportTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        label = QLabel("SQL Import Tab (Stub)")
        layout.addWidget(label)
        self.setLayout(layout)


###############################################################################
# Enhanced CanvasGraphicsView
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

        # ENHANCEMENT: We'll keep references for operation
        self.operation_red_line = None
        self.complete_query_item = None
        self.target_table_item = None

        self.validation_timer = QTimer()
        self.validation_timer.setInterval(800)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.builder.validate_sql)

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        event.acceptProposedAction()

    def dropEvent(self, event):
        text = event.mimeData().text()
        pos = self.mapToScene(event.pos())
        self.builder.handle_drop(text, pos)
        event.acceptProposedAction()

    def drawBackground(self, painter, rect):
        # Light grid
        grid = 20
        left = int(rect.left()) - (int(rect.left()) % grid)
        top = int(rect.top()) - (int(rect.top()) % grid)

        lines = []
        x = left
        while x < int(rect.right()):
            lines.append(QtCore.QLineF(x, rect.top(), x, rect.bottom()))
            x += grid

        y = top
        while y < int(rect.bottom()):
            lines.append(QtCore.QLineF(rect.left(), y, rect.right(), y))
            y += grid

        painter.setPen(QPen(QColor(220, 220, 220), 1))
        painter.drawLines(lines)

    def add_join(self, table1, table2, join_type, condition, is_subquery=False):
        if table1 not in self.table_items and table1 not in self.subquery_items:
            QMessageBox.warning(self, "Join Error", f"'{table1}' not on canvas.")
            return
        if table2 not in self.table_items and table2 not in self.subquery_items:
            QMessageBox.warning(self, "Join Error", f"'{table2}' not on canvas.")
            return

        start_item = self.table_items.get(table1, self.subquery_items.get(table1))
        end_item = self.table_items.get(table2, self.subquery_items.get(table2))
        join_line = JoinLine(start_item, end_item, join_type, condition, is_subquery)
        self.scene.addItem(join_line)
        self.join_lines.append(join_line)
        self.builder.joins.append({
            'table1': table1,
            'table2': table2,
            'type': join_type,
            'condition': condition,
            'is_subquery': is_subquery
        })
        self.builder.generate_sql()
        self.validation_timer.start()

    def remove_join(self, join):
        self.scene.removeItem(join)
        if join in self.join_lines:
            self.join_lines.remove(join)
        for jdict in self.builder.joins[:]:
            # We'll just remove if the tables match
            if (jdict['table1'] == join.start_item or jdict['table2'] == join.end_item
                    or jdict['table2'] == join.start_item or jdict['table1'] == join.end_item):
                self.builder.joins.remove(jdict)

        self.builder.generate_sql()
        self.validation_timer.start()

    def clear_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene.removeItem(ml)
        self.mapping_lines = []

    def add_table(self, alias, original, pos):
        if alias in self.table_items or alias in self.subquery_items:
            QMessageBox.warning(self, "Duplicate", f"'{alias}' already on canvas.")
            return
        rect = QGraphicsRectItem(0, 0, 200, 100)
        rect.setBrush(QBrush(QColor(220, 220, 255)))
        rect.setPen(QPen(Qt.darkGray, 2))
        rect.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)
        rect.setPos(pos)
        label = QGraphicsTextItem(f"Table: {original}\nAS {alias}", rect)
        label.setDefaultTextColor(Qt.black)
        label.setFont(QFont("Arial", 10, QFont.Bold))
        label.setPos(5, 5)

        self.scene.addItem(rect)
        self.table_items[alias] = rect

        self.builder.generate_sql()
        self.validation_timer.start()

    def remove_subquery(self, alias):
        if alias in self.subquery_items:
            item = self.subquery_items[alias]
            for jl in [j for j in self.join_lines if j.start_item == item or j.end_item == item]:
                self.remove_join(jl)
            self.scene.removeItem(item)
            del self.subquery_items[alias]
            self.builder.generate_sql()
            self.validation_timer.start()

    def contextMenuEvent(self, event):
        item = self.itemAt(event.pos())
        if isinstance(item, QGraphicsRectItem):
            table_name = None
            for k, v in self.table_items.items():
                if v == item:
                    table_name = k
                    break
            if not table_name:
                for k, v in self.subquery_items.items():
                    if v == item:
                        table_name = k
                        break
            if table_name:
                menu = QMenu()
                if table_name in self.table_items:
                    add_join = menu.addAction("Add Join")
                    remove_joins = menu.addAction("Remove Joins")
                    chosen = menu.exec_(self.mapToGlobal(event.pos()))
                    if chosen == add_join:
                        self.builder.initiate_join(table_name)
                    elif chosen == remove_joins:
                        for jn in [jl for jl in self.join_lines
                                   if jl.start_item == item or jl.end_item == item]:
                            self.remove_join(jn)
                else:
                    remove_sub = menu.addAction("Remove Subquery")
                    chosen = menu.exec_(self.mapToGlobal(event.pos()))
                    if chosen == remove_sub:
                        self.builder.remove_subquery(table_name)
                return

        super().contextMenuEvent(event)

    # ENHANCEMENT: Operation-related additions
    def add_vertical_red_line(self, x=400):
        if self.operation_red_line:
            self.scene.removeItem(self.operation_red_line)
        line_item = QtWidgets.QGraphicsLineItem(x, 0, x, 2000)
        pen = QPen(Qt.red, 2, Qt.DashDotLine)
        line_item.setPen(pen)
        line_item.setZValue(-10)
        self.scene.addItem(line_item)
        self.operation_red_line = line_item

    def add_complete_query_item(self, columns, x=50, y=200):
        if self.complete_query_item:
            self.scene.removeItem(self.complete_query_item)
            self.complete_query_item = None

        cqi = CompleteQueryItem(columns, x, y)
        self.scene.addItem(cqi)
        self.complete_query_item = cqi

    def add_target_table_item(self, db_name, table_name, columns, x=500, y=200):
        if self.target_table_item:
            self.scene.removeItem(self.target_table_item)
            self.target_table_item = None

        tti = TargetTableItem(db_name, table_name, columns, x, y)
        self.scene.addItem(tti)
        self.target_table_item = tti

    def create_mapping_line(self, source_text_item, target_text_item):
        ml = MappedColumnLine(source_text_item, target_text_item)
        self.scene.addItem(ml)
        self.mapping_lines.append(ml)
        return ml


###############################################################################
# Main Query Builder
###############################################################################
class VisualQueryBuilderTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.connections = {}
        self.schema_cache_files = {}
        self.joins = []
        self.mapping = {}  # not used deeply here
        self.operation_mode = "SELECT"   # or "UPDATE", "DELETE", "INSERT"

        self.combination_operator = None
        self.second_query = None

        QApplication.setStyle("Windows")
        self.threadpool = QThreadPool.globalInstance()

        self.initUI()

    def initUI(self):
        main_layout = QVBoxLayout(self)

        # Connection row
        conn_layout = QHBoxLayout()
        self.status_light = QFrame()
        self.status_light.setFixedSize(15, 15)
        self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red; }")
        self.server_label = QLabel("Not Connected")

        conn_button = QPushButton("Connect")
        conn_button.clicked.connect(self.open_connect_dialog)

        conn_layout.addWidget(self.status_light)
        conn_layout.addWidget(self.server_label)
        conn_layout.addWidget(conn_button)
        conn_layout.addStretch()
        main_layout.addLayout(conn_layout)

        # Toolbar row (includes Operation toggles)
        toolbar_layout = QHBoxLayout()
        refresh_btn = QPushButton("Refresh Schema")
        refresh_btn.clicked.connect(self.refresh_schema)
        toolbar_layout.addWidget(refresh_btn)

        alias_btn = QPushButton("Manage Aliases")
        alias_btn.clicked.connect(self.manage_aliases)
        toolbar_layout.addWidget(alias_btn)

        window_fn_btn = QPushButton("Window Function")
        window_fn_btn.clicked.connect(self.open_window_function_dialog)
        toolbar_layout.addWidget(window_fn_btn)

        combine_btn = QPushButton("Add Combine Query Node")
        combine_btn.clicked.connect(self.add_combine_query_node)
        toolbar_layout.addWidget(combine_btn)

        derived_btn = QPushButton("Add Derived Column Node")
        derived_btn.clicked.connect(self.add_derived_column_node)
        toolbar_layout.addWidget(derived_btn)

        # Operation toggles
        self.operation_combo = QComboBox()
        self.operation_combo.addItems(["SELECT (No Operation)", "INSERT", "UPDATE", "DELETE"])
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

        self.schema_tree = LazySchemaTreeWidget(conn)
        self.schema_tree.itemDoubleClicked.connect(self.suggest_joins)
        self.schema_tree.itemChanged.connect(self.handle_item_changed)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.addWidget(self.schema_tree)
        splitter.addWidget(left_panel)

        self.canvas = EnhancedCanvasGraphicsView(builder=self)
        splitter.addWidget(self.canvas)

        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)
        layout.addWidget(splitter)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

    def setup_query_config_tab(self):
        layout = QHBoxLayout(self.query_config_tab)

        self.filter_panel = FilterPanel(builder=self)
        layout.addWidget(self.filter_panel, 2)

        self.group_by_panel = GroupByPanel(builder=self)
        layout.addWidget(self.group_by_panel, 3)

        self.sort_limit_panel = SortLimitPanel(builder=self)
        layout.addWidget(self.sort_limit_panel, 2)

        self.query_config_tab.setLayout(layout)

    def setup_sql_preview_tab(self):
        layout = QVBoxLayout(self.sql_preview_tab)

        header = QHBoxLayout()
        header.addWidget(QLabel("Generated SQL:"))
        run_sql = QPushButton("Run SQL")
        run_sql.clicked.connect(self.run_sql_query)
        header.addWidget(run_sql, alignment=Qt.AlignRight)
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
            conn = dlg.get_connection()
            db_type = dlg.get_db_type()
            if conn and db_type and db_type.upper() == "TERADATA":
                alias = f"{db_type}_{len(self.connections) + 1}"
                conn_info = {
                    "type": db_type,
                    "connection": conn
                }
                self.connections[alias] = conn_info
                self.schema_cache_files[alias] = f"schema_cache_{alias}.pkl"
                self.update_connection_status(True, f"{db_type} ({alias})")
                self.status_bar.showMessage(f"Connected as {alias}.", 5000)
                self.load_schema(alias)
            else:
                QMessageBox.warning(self, "Only Teradata Allowed", "This is restricted to Teradata DSNs only.")

    def load_schema(self, alias):
        if alias not in self.connections:
            return
        conn = self.connections[alias]['connection']
        self.schema_tree.connection = conn
        self.schema_tree.populate_top_level()
        self.status_bar.showMessage("Schema loaded.", 3000)

    def refresh_schema(self):
        if self.connections:
            first_key = list(self.connections.keys())[0]
            self.load_schema(first_key)
        else:
            QMessageBox.information(self, "Not Connected", "Please connect first.")

    def update_connection_status(self, connected, info=""):
        if connected:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green; }")
            self.server_label.setText(info)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red; }")
            self.server_label.setText("Not Connected")

    ###########################################################################
    # Utility / Handlers
    ###########################################################################
    def filter_schema_tree(self, text):
        for i in range(self.schema_tree.topLevelItemCount()):
            item = self.schema_tree.topLevelItem(i)
            self.filter_item(item, text)

    def filter_item(self, item, text):
        txt = text.lower()
        match = txt in item.text(0).lower()
        child_match = False
        for i in range(item.childCount()):
            child_match = self.filter_item(item.child(i), text) or child_match
        item.setHidden(not (match or child_match))
        return match or child_match

    def handle_item_changed(self, item, col):
        if item.childCount() > 0:
            st = item.checkState(0)
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, st)
        else:
            parent = item.parent()
            if parent:
                count = sum(parent.child(i).checkState(0) == Qt.Checked
                            for i in range(parent.childCount()))
                if count == parent.childCount():
                    parent.setCheckState(0, Qt.Checked)
                elif count > 0:
                    parent.setCheckState(0, Qt.PartiallyChecked)
                else:
                    parent.setCheckState(0, Qt.Unchecked)
        self.generate_sql()

    def run_sql_query(self):
        sql = self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self, "Empty SQL", "No SQL to run.")
            return
        QMessageBox.information(self, "SQL Execution", f"Executing:\n\n{sql}")

    ###########################################################################
    # Join
    ###########################################################################
    def suggest_joins(self, item, col):
        # Placeholder
        pass

    def initiate_join(self, table_name):
        selected = self.get_selected_tables()
        if len(selected) < 2:
            QMessageBox.warning(self, "Join Error", "At least two tables are required.")
            return
        others = [t for t in selected if t != table_name]
        if not others:
            return
        # We'll just do a minimal approach: pick the second from combos
        second = others[0]
        # Hard-coded example
        join_type = "INNER JOIN"
        condition = f"{table_name}.id = {second}.id"
        self.canvas.add_join(table_name, second, join_type, condition)

    def remove_subquery(self, alias):
        self.canvas.remove_subquery(alias)

    ###########################################################################
    # Drag & Drop from the Tree
    ###########################################################################
    def handle_drop(self, text, pos):
        alias = text
        original = text
        self.canvas.add_table(alias, original, pos)

    ###########################################################################
    # Panels usage
    ###########################################################################
    def get_selected_tables(self):
        return list(self.canvas.table_items.keys()) + list(self.canvas.subquery_items.keys())

    def get_selected_columns(self):
        cols = []
        for i in range(self.schema_tree.topLevelItemCount()):
            conn_item = self.schema_tree.topLevelItem(i)
            for j in range(conn_item.childCount()):
                db_item = conn_item.child(j)
                if db_item.data(0, Qt.UserRole) == "database":
                    for k in range(db_item.childCount()):
                        tbl_item = db_item.child(k)
                        if tbl_item.data(0, Qt.UserRole) == "table":
                            for l in range(tbl_item.childCount()):
                                col_item = tbl_item.child(l)
                                if (col_item.data(0, Qt.UserRole) == "column"
                                        and col_item.checkState(0) == Qt.Checked):
                                    table_name = tbl_item.text(0)
                                    column_name = col_item.text(0)
                                    cols.append(f"{table_name}.{column_name}")
        return cols

    def get_all_possible_columns_for_dialog(self):
        """
        For the new no-freehand approach, we gather all possible columns from
        the checked items. (In real usage, you might gather from all schema items
        or only from the tables on the canvas, etc.)
        """
        return self.get_selected_columns()

    ###########################################################################
    # Alias management
    ###########################################################################
    def manage_aliases(self):
        current = list(self.canvas.table_items.keys()) + list(self.canvas.subquery_items.keys())
        if not current:
            QMessageBox.information(self, "No Items", "Nothing on canvas to alias.")
            return
        d = AliasManagementDialog(self, current)
        if d.exec_() == QDialog.Accepted:
            self.update_aliases(d.get_alias_mapping())

    def update_aliases(self, mapping):
        for orig, new in mapping.items():
            if orig in self.canvas.table_items:
                item = self.canvas.table_items[orig]
                for ch in item.childItems():
                    if isinstance(ch, QGraphicsTextItem):
                        self.canvas.scene.removeItem(ch)
                lbl = QGraphicsTextItem(f"{orig} AS {new}", item)
                lbl.setDefaultTextColor(Qt.black)
                lbl.setFont(QFont("Arial", 10, QFont.Bold))
                lbl.setPos(5, 5)
                self.canvas.table_items[new] = item
                del self.canvas.table_items[orig]
            elif orig in self.canvas.subquery_items:
                item = self.canvas.subquery_items[orig]
                for ch in item.childItems():
                    if isinstance(ch, QGraphicsTextItem):
                        self.canvas.scene.removeItem(ch)
                lbl = QGraphicsTextItem(f"Subquery AS {new}", item)
                lbl.setDefaultTextColor(Qt.black)
                lbl.setFont(QFont("Arial", 10, QFont.Bold))
                lbl.setPos(5, 5)
                self.canvas.subquery_items[new] = item
                del self.canvas.subquery_items[orig]
        self.generate_sql()

    ###########################################################################
    # Derived, Combine, Window
    ###########################################################################
    def add_derived_column_node(self):
        available = self.get_selected_columns()
        dlg = AddDerivedColumnDialog(self, available)
        if dlg.exec_() == QDialog.Accepted:
            alias, expr = dlg.get_data()
            item = DerivedColumnItem(alias, expr, x=200, y=200)
            self.canvas.scene.addItem(item)
            self.generate_sql()

    def add_combine_query_node(self):
        dlg = CombineQueriesDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            op, second_sql = dlg.get_data()
            cq_item = CombineQueryItem(op, second_sql, x=300, y=300)
            self.canvas.scene.addItem(cq_item)
            self.generate_sql()

    def open_window_function_dialog(self):
        available = self.get_selected_columns()
        if not available:
            QMessageBox.warning(self, "No Columns", "Select columns before adding window function.")
            return
        dlg = WindowFunctionDialog(self, available)
        if dlg.exec_() == QDialog.Accepted:
            alias, expr = dlg.get_expression()
            item = DerivedColumnItem(alias, expr, x=250, y=250)
            self.canvas.scene.addItem(item)
            self.generate_sql()

    ###########################################################################
    # Operation Toggle
    ###########################################################################
    def toggle_operation_mode(self):
        index = self.operation_combo.currentIndex()
        if index == 0:
            # SELECT
            self.operation_mode = "SELECT"
            # Remove any operation items or lines
            if self.canvas.operation_red_line:
                self.canvas.scene.removeItem(self.canvas.operation_red_line)
                self.canvas.operation_red_line = None
            if self.canvas.complete_query_item:
                self.canvas.scene.removeItem(self.canvas.complete_query_item)
                self.canvas.complete_query_item = None
            if self.canvas.target_table_item:
                self.canvas.scene.removeItem(self.canvas.target_table_item)
                self.canvas.target_table_item = None
            self.canvas.clear_mapping_lines()
        else:
            mode = self.operation_combo.currentText()
            self.operation_mode = mode
            self.activate_operation_mode(mode)
        self.generate_sql()

    def activate_operation_mode(self, mode):
        """
        For demonstration:
        1) We place the vertical red line
        2) We place a "CompleteQueryItem" on the left with the user’s SELECT columns
        3) The user can then drag a table from the schema to the right side.
           They can map columns (not fully automated here, but we show concept).
        """
        self.canvas.add_vertical_red_line(x=450)
        # Gather the columns from the current SELECT
        # For simplicity, we show columns that appear in the final SELECT set.
        # We'll do a quick parse or just reuse get_select_list logic from generate_sql.
        col_list = self.build_select_list_for_display()
        self.canvas.add_complete_query_item(col_list, x=100, y=200)
        # The user then can drag a table on the right side.
        # We'll let them do it manually. No immediate logic needed here.

    def build_select_list_for_display(self):
        """
        Return a list of the columns that would appear in the final SELECT clause
        (including derived columns, aggregates, etc.).
        """
        # Use the same logic as in _generate_select_sql but just gather the item strings
        selected_cols = self.get_selected_columns()
        derived_items = [it for it in self.canvas.scene.items() if isinstance(it, DerivedColumnItem)]
        derived_selects = [f"{it.expression} AS {it.alias}" for it in derived_items]

        aggregates = self.group_by_panel.get_aggregates()
        agg_selects = [f"{func}({col}) AS {alias}" for (func, col, alias) in aggregates]

        final_list = list(selected_cols) + derived_selects + agg_selects
        if not final_list:
            final_list = ["*"]
        return final_list

    ###########################################################################
    # Validate & Generate SQL
    ###########################################################################
    def validate_sql(self):
        sql_text = self.sql_display.toPlainText().strip()
        if not sql_text:
            self.validation_label.setText("SQL Status: No SQL to validate.")
            self.validation_label.setStyleSheet("color: orange;")
            return
        try:
            parser = SQLParser(sql_text)
            parsed_info = parser.parse()
            if not parsed_info["select_columns"] and not any(
                k in sql_text.upper() for k in ["INSERT", "UPDATE", "DELETE"]
            ):
                self.validation_label.setText("SQL Status: Invalid - missing SELECT columns.")
                self.validation_label.setStyleSheet("color: red;")
                return
            if not parsed_info["tables"] and not any(
                k in sql_text.upper() for k in ["INSERT", "UPDATE", "DELETE"]
            ):
                self.validation_label.setText("SQL Status: Invalid - no tables found in FROM.")
                self.validation_label.setStyleSheet("color: red;")
                return
            self.validation_label.setText("SQL Status: Valid.")
            self.validation_label.setStyleSheet("color: green;")
        except Exception as e:
            self.validation_label.setText("SQL Status: Invalid - " + str(e))
            self.validation_label.setStyleSheet("color: red;")

    def generate_sql(self):
        scene_items = self.canvas.scene.items()
        derived_items = [it for it in scene_items if isinstance(it, DerivedColumnItem)]
        combine_items = [it for it in scene_items if isinstance(it, CombineQueryItem)]

        if self.operation_mode == "INSERT":
            sql = self._generate_insert_sql(derived_items)
        elif self.operation_mode == "UPDATE":
            sql = self._generate_update_sql(derived_items)
        elif self.operation_mode == "DELETE":
            sql = self._generate_delete_sql()
        else:
            # SELECT
            sql = self._generate_select_sql(derived_items, combine_items)

        self.sql_display.setPlainText(sql)
        self.validation_label.setText("SQL Status: Generated")
        self.validation_label.setStyleSheet("color: green;")

    def _generate_select_sql(self, derived_items, combine_items):
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            return "-- No tables => no SELECT."

        # Collect final list of SELECT columns
        derived_selects = [f"{it.expression} AS {it.alias}" for it in derived_items]
        checked_cols = self.get_selected_columns()

        select_parts = []
        if checked_cols:
            select_parts.extend(checked_cols)
        if derived_selects:
            select_parts.extend(derived_selects)
        if not select_parts:
            select_parts.append("*")

        from_part = selected_tables[0]
        join_parts = []
        for jdict in self.joins:
            jt = jdict['type']
            t2 = jdict['table2']
            cond = jdict['condition']
            join_parts.append(f"{jt} {t2} ON {cond}")

        # WHERE
        where_filters = self.filter_panel.get_filters("WHERE")
        where_parts = []
        for c, o, v in where_filters:
            op_upper = o.upper()
            if op_upper in ["IS NULL", "IS NOT NULL"]:
                where_parts.append(f"{c} {op_upper}")
            else:
                where_parts.append(f"{c} {o} {v}")

        # GROUP BY
        group_parts = self.group_by_panel.get_group_by()
        # HAVING
        having_filters = self.filter_panel.get_filters("HAVING")
        having_parts = []
        for c, o, v in having_filters:
            op_upper = o.upper()
            if op_upper in ["IS NULL", "IS NOT NULL"]:
                having_parts.append(f"{c} {op_upper}")
            else:
                having_parts.append(f"{c} {o} {v}")

        # Aggregates
        aggregates = self.group_by_panel.get_aggregates()
        for func, col, alias in aggregates:
            select_parts.append(f"{func}({col}) AS {alias}")

        # ORDER BY
        order_parts = self.sort_limit_panel.get_order_bys()
        limit_val = self.sort_limit_panel.get_limit()
        offset_val = self.sort_limit_panel.get_offset()

        lines = []
        lines.append("SELECT " + ", ".join(select_parts))
        lines.append("FROM " + from_part)
        for jp in join_parts:
            lines.append(jp)
        if where_parts:
            lines.append("WHERE " + " AND ".join(where_parts))
        if group_parts:
            lines.append("GROUP BY " + ", ".join(group_parts))
        if having_parts:
            lines.append("HAVING " + " AND ".join(having_parts))
        if order_parts:
            lines.append("ORDER BY " + ", ".join(order_parts))
        if limit_val is not None:
            lines.append(f"LIMIT {limit_val}")
        if offset_val is not None:
            lines.append(f"OFFSET {offset_val}")

        final_sql = "\n".join(lines)

        if combine_items:
            citem = combine_items[0]
            final_sql = f"{final_sql}\n{citem.operator}\n(\n{citem.second_sql}\n)"

        if self.combination_operator and self.second_query:
            final_sql = f"{final_sql}\n{self.combination_operator}\n(\n{self.second_query}\n)"

        return final_sql

    # ENHANCEMENT: Insert/Update/Delete now rely on mapped columns
    def _generate_insert_sql(self, derived_items):
        """
        We'll assume the user has the "CompleteQueryItem" for the SELECT side
        and a "TargetTableItem" for the target table. We glean column mappings
        from the lines in self.canvas.mapping_lines.
        """
        cqi = self.canvas.complete_query_item
        tti = self.canvas.target_table_item
        if not cqi or not tti:
            return "-- Incomplete setup for INSERT.\n-- Drag a target table to the right of the red line."

        # Gather mapped columns
        # We check each MappedColumnLine: the text in the source is "X" or "func(...) AS X",
        # the text in the target is the actual column name.
        mapped_pairs = []
        for ml in self.canvas.mapping_lines:
            if ml.source_text_item.parentItem() == cqi and ml.target_text_item.parentItem() == tti:
                source_col = ml.source_text_item.toPlainText()
                target_col = ml.target_text_item.toPlainText()
                mapped_pairs.append((source_col, target_col))

        if not mapped_pairs:
            return "-- No column mappings done, cannot build INSERT.\n"

        # Build columns & select expressions
        target_cols = []
        select_exprs = []
        for s, t in mapped_pairs:
            target_cols.append(t)
            # if user has "AS xyz" in source col text, we might parse it
            # For demonstration, let's just use the entire source text
            if " AS " in s.upper():
                # parse out the expression
                expr_part = s.split(" AS ")[0]
                select_exprs.append(expr_part.strip())
            else:
                select_exprs.append(s.strip())

        target_col_list = ", ".join(target_cols)
        select_expr_list = ", ".join(select_exprs)
        table_full_name = f"{tti.db_name}.{tti.table_name}"

        sql = f"INSERT INTO {table_full_name} ({target_col_list})\nSELECT {select_expr_list}\n/* FROM ... (the same SELECT logic) */"
        return sql

    def _generate_update_sql(self, derived_items):
        cqi = self.canvas.complete_query_item
        tti = self.canvas.target_table_item
        if not cqi or not tti:
            return "-- Incomplete setup for UPDATE.\n-- Drag a target table to the right of the red line."

        mapped_pairs = []
        for ml in self.canvas.mapping_lines:
            if ml.source_text_item.parentItem() == cqi and ml.target_text_item.parentItem() == tti:
                source_col = ml.source_text_item.toPlainText()
                target_col = ml.target_text_item.toPlainText()
                mapped_pairs.append((source_col, target_col))

        if not mapped_pairs:
            return "-- No column mappings done, cannot build UPDATE.\n"

        table_full_name = f"{tti.db_name}.{tti.table_name}"
        set_clauses = []
        for s, t in mapped_pairs:
            if " AS " in s.upper():
                expr_part = s.split(" AS ")[0]
                set_clauses.append(f"{t} = {expr_part}")
            else:
                set_clauses.append(f"{t} = {s}")

        sql = (
            f"UPDATE {table_full_name}\n"
            f"SET {', '.join(set_clauses)}\n"
            f"/* Typically need a WHERE to identify which rows to update. */"
        )
        return sql

    def _generate_delete_sql(self):
        cqi = self.canvas.complete_query_item
        tti = self.canvas.target_table_item
        if not cqi or not tti:
            return "-- Incomplete setup for DELETE.\n-- Drag a target table to the right of the red line."

        # For DELETE, we typically match some key columns. We'll assume user mapped them
        mapped_pairs = []
        for ml in self.canvas.mapping_lines:
            if ml.source_text_item.parentItem() == cqi and ml.target_text_item.parentItem() == tti:
                source_col = ml.source_text_item.toPlainText()
                target_col = ml.target_text_item.toPlainText()
                mapped_pairs.append((source_col, target_col))

        table_full_name = f"{tti.db_name}.{tti.table_name}"
        # We'll just demonstrate a possible "JOIN-based" DELETE
        join_conds = []
        for s, t in mapped_pairs:
            join_conds.append(f"{table_full_name}.{t} = SourceData.{s}")

        cond_str = " AND ".join(join_conds) if join_conds else "1=2"
        sql = (
            f"DELETE {table_full_name}\n"
            f"FROM {table_full_name}\n"
            f"JOIN (\n  /* The SELECT logic here or a temp table? */\n) AS SourceData\n"
            f"  ON {cond_str};"
        )
        return sql


###############################################################################
# Main Launch
###############################################################################
if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = QMainWindow()
    builder_tab = VisualQueryBuilderTab(parent=main_window)
    main_window.setCentralWidget(builder_tab)
    main_window.setWindowTitle("Visual Query Builder - Teradata Only (Enhanced)")
    main_window.resize(1200, 800)
    main_window.show()
    sys.exit(app.exec_())
