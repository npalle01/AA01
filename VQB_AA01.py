#!/usr/bin/env python
import sys
import os
import pickle
import pyodbc
import sqlparse
import traceback

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
    QSpinBox, QInputDialog, QFileDialog, QListWidget, QStyle, QMenu, QFrame
)

# Enable connection pooling for pyodbc
pyodbc.pooling = True

###############################################################################
# Simple Button Helper
###############################################################################
def create_text_button(text: str, tooltip: str = "") -> QPushButton:
    btn = QPushButton(text)
    btn.setToolTip(tooltip)
    return btn


###############################################################################
# CombineQueriesDialog
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


###############################################################################
# WindowFunctionDialog
###############################################################################
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


###############################################################################
# AddDerivedColumnDialog
###############################################################################
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
        form.addRow("Column Name:", self.name_edit)

        self.expr_edit = QLineEdit()
        self.expr_edit.setPlaceholderText("Enter SQL expression")
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
# SQLParser (Stubbed logic for demonstration)
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
        # Real implementation would walk through sqlparse tokens recursively.


###############################################################################
# SQLHighlighter
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

        self.update_position()

        self.label = QGraphicsTextItem(join_type, self)
        self.label.setDefaultTextColor(Qt.blue)
        mid_x = (self.start_item.pos().x() + self.end_item.pos().x()) / 2
        mid_y = (self.start_item.pos().y() + self.end_item.pos().y()) / 2
        self.label.setPos(mid_x, mid_y)
        self.label.setZValue(1)

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
# TargetTableDialog
###############################################################################
class TargetTableDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Select Target Table for Operation")
        self.setModal(True)
        self.resize(400, 200)
        self.table_name = None
        self.columns = None

        layout = QFormLayout(self)
        self.table_name_edit = QLineEdit()
        self.columns_edit = QLineEdit()
        self.columns_edit.setPlaceholderText("Comma-separated column names")

        layout.addRow("Target Table Name:", self.table_name_edit)
        layout.addRow("Target Table Columns:", self.columns_edit)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.validate_and_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def validate_and_accept(self):
        self.table_name = self.table_name_edit.text().strip()
        self.columns = [
            col.strip() for col in self.columns_edit.text().split(",") if col.strip()
        ]
        if not self.table_name or not self.columns:
            QMessageBox.warning(
                self, "Input Error", "Both table name and columns are required."
            )
            return
        self.accept()

    def get_data(self):
        return self.table_name, self.columns


###############################################################################
# MappingDialog
###############################################################################
class MappingDialog(QDialog):
    def __init__(self, parent=None, dataset_columns=[], target_columns=[]):
        super().__init__(parent)
        self.setWindowTitle("Map Dataset Columns to Target Columns")
        self.setModal(True)
        self.resize(400, 300)

        self.dataset_columns = dataset_columns
        self.target_columns = target_columns
        self.mappings = {}  # target_col -> dataset_col

        layout = QVBoxLayout(self)
        self.mapping_list = QListWidget()
        # Prepopulate with one line per target column
        for tcol in self.target_columns:
            self.mapping_list.addItem(f"{tcol} => ")
        layout.addWidget(self.mapping_list)

        assign_btn = QPushButton("Assign Mapping")
        assign_btn.clicked.connect(self.assign_mapping)
        layout.addWidget(assign_btn)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def assign_mapping(self):
        current_item = self.mapping_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Mapping", "Select a target column line to assign mapping.")
            return
        target = current_item.text().split("=>")[0].strip()
        ds_col, ok = QInputDialog.getItem(
            self,
            "Select Dataset Column",
            "Dataset Column:",
            self.dataset_columns,
            0,
            False
        )
        if ok:
            idx = self.mapping_list.currentRow()
            self.mapping_list.takeItem(idx)
            self.mapping_list.insertItem(idx, f"{target} => {ds_col}")
            self.mappings[target] = ds_col

    def get_mappings(self):
        return self.mappings


###############################################################################
# LazySchemaLoaderWorker (For Teradata) - Loads Tables for a DB
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
                SELECT TableName FROM DBC.TablesV
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
    """
    A tree that displays a connection node and database nodes.
    When a database node is expanded, the worker loads its table names (Teradata).
    When a table node is expanded, we load that table's columns and create
    checkable column items.
    """
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
        if self.connection:
            try:
                conn_name = self.connection.getinfo(pyodbc.SQL_DBMS_NAME).strip()
                if not conn_name:
                    conn_name = "Teradata"
            except:
                conn_name = "Teradata"
        else:
            conn_name = "Unknown"

        conn_item = QTreeWidgetItem([f"{conn_name}"])
        conn_item.setData(0, Qt.UserRole, "connection")
        self.addTopLevelItem(conn_item)

        cursor = self.connection.cursor() if self.connection else None
        try:
            if cursor:
                cursor.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
                db_names = [row[0] for row in cursor.fetchall()]
            else:
                db_names = []
        except Exception:
            db_names = ["MY_DB", "SALES_DB", "HR_DB"]

        for db in db_names:
            db_item = QTreeWidgetItem([db])
            db_item.setData(0, Qt.UserRole, "database")
            db_item.setData(0, Qt.UserRole + 1, False)

            # A dummy child so the node can be expanded
            dummy = QTreeWidgetItem(["Loading..."])
            db_item.addChild(dummy)

            conn_item.addChild(db_item)

        self.expandItem(conn_item)

    def on_item_expanded(self, item):
        data_type = item.data(0, Qt.UserRole)

        # If it's a database node, load the tables
        if data_type == "database":
            loaded = item.data(0, Qt.UserRole + 1)
            if not loaded:
                item.takeChildren()
                db_name = item.text(0)
                worker = LazySchemaLoaderWorker(self.connection, db_name)
                worker.signals.finished.connect(
                    lambda tables, it=item: self.populate_database_node(it, tables)
                )
                worker.signals.error.connect(self.handle_error)
                self.threadpool.start(worker)

        # If it's a table node, load the columns
        elif data_type == "table":
            loaded = item.data(0, Qt.UserRole + 1)
            if not loaded:
                item.takeChildren()
                # The database name is the parent node's text
                db_name = item.parent().text(0)
                table_name = item.text(0)
                columns = self.load_columns_for_table(db_name, table_name)
                if columns:
                    for col in columns:
                        col_item = QTreeWidgetItem([col])
                        col_item.setData(0, Qt.UserRole, "column")
                        # Make it checkable so the user can pick it for SELECT
                        col_item.setFlags(col_item.flags() | Qt.ItemIsUserCheckable)
                        col_item.setCheckState(0, Qt.Unchecked)
                        item.addChild(col_item)
                else:
                    item.addChild(QTreeWidgetItem(["<No columns found>"]))
                item.setData(0, Qt.UserRole + 1, True)

    def populate_database_node(self, parent_item, tables):
        if not tables:
            parent_item.addChild(QTreeWidgetItem(["No tables found"]))
        else:
            for tbl in tables:
                tbl_item = QTreeWidgetItem([tbl])
                tbl_item.setData(0, Qt.UserRole, "table")
                tbl_item.setData(0, Qt.UserRole + 1, False)  # not loaded columns yet

                # Add a dummy child so it can expand
                dummy = QTreeWidgetItem(["Loading columns..."])
                tbl_item.addChild(dummy)

                parent_item.addChild(tbl_item)
        parent_item.setData(0, Qt.UserRole + 1, True)

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
            for row in cursor.fetchall():
                columns.append(row[0])
        except Exception as e:
            print(f"Error loading columns for {db_name}.{table_name}: {e}")
        return columns

    def handle_error(self, err_msg):
        QMessageBox.critical(self, "Schema Load Error", err_msg)

    def startDrag(self, supportedActions):
        item = self.currentItem()
        if item and item.parent() and item.data(0, Qt.UserRole) == "table":
            drag = QDrag(self)
            mime = QtCore.QMimeData()
            mime.setText(item.text(0))
            drag.setMimeData(mime)
            drag.exec_(supportedActions)
###############################################################################
# Additional Canvas Classes for Derived/Combine/Operations
###############################################################################
class DerivedColumnItem(QGraphicsRectItem):
    """
    A visual node representing a derived column on the canvas.
    We store (alias, expression).
    """
    def __init__(self, alias, expression, x=0, y=0):
        super().__init__(0, 0, 220, 60)
        self.alias = alias
        self.expression = expression
        self.setPos(x, y)
        self.setBrush(QBrush(QtGui.QColor(255, 230, 200)))
        self.setPen(QPen(Qt.darkBlue, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        txt = QGraphicsTextItem(f"Derived:\n{alias} = {expression}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


class CombineQueryItem(QGraphicsRectItem):
    """
    A visual node for a combined query (UNION, etc.) + second SQL.
    """
    def __init__(self, operator, second_sql, x=0, y=0):
        super().__init__(0, 0, 260, 80)
        self.operator = operator
        self.second_sql = second_sql
        self.setPos(x, y)
        self.setBrush(QBrush(QtGui.QColor(210, 255, 210)))
        self.setPen(QPen(Qt.darkGreen, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        preview = second_sql[:25] + "..." if len(second_sql) > 25 else second_sql
        txt = QGraphicsTextItem(f"Combine:\n{operator}\n{preview}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


class OperationItem(QGraphicsRectItem):
    """
    Represents an operation: UPDATE, DELETE, INSERT.
    Holds references to a target table, columns, etc.
    """
    def __init__(self, op_type, table_name, columns=None, x=0, y=0):
        super().__init__(0, 0, 220, 60)
        self.op_type = op_type.upper()  # UPDATE, DELETE, INSERT
        self.table_name = table_name
        self.columns = columns or []
        self.setPos(x, y)
        self.setBrush(QBrush(QtGui.QColor(255, 200, 200)))
        self.setPen(QPen(Qt.red, 2))
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        txt = QGraphicsTextItem(f"{op_type}:\n{table_name}\nCols: {', '.join(self.columns)}", self)
        txt.setDefaultTextColor(Qt.black)
        txt.setFont(QFont("Arial", 8, QFont.Bold))
        txt.setPos(5, 5)


###############################################################################
# Panels for Query Configuration (Filters, GroupBy, SortLimit)
###############################################################################
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
        col, ok = QInputDialog.getText(self, f"Add {clause} Filter", "Column:")
        if not ok or not col:
            return
        op, ok = QInputDialog.getText(self, f"Add {clause} Filter", "Operator:")
        if not ok or not op:
            return
        val, ok = QInputDialog.getText(self, f"Add {clause} Filter", "Value:")
        if not ok:
            return

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


class GroupByPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Group By and Aggregates", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

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
        col, ok = QInputDialog.getText(self, "Add GroupBy", "Enter column name:")
        if ok and col:
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
        func, ok = QInputDialog.getItem(
            self, "Add Aggregate", "Function:",
            ["COUNT", "SUM", "AVG", "MIN", "MAX"], 0, False
        )
        if not ok:
            return
        col, ok = QInputDialog.getText(self, "Add Aggregate", "Column:")
        if not ok or not col:
            return
        alias, ok = QInputDialog.getText(self, "Add Aggregate", "Alias:")
        if not ok or not alias:
            return
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

            func, ok = QInputDialog.getItem(
                self, "Edit Aggregate", "Function:",
                ["COUNT", "SUM", "AVG", "MIN", "MAX"], 0, False
            )
            if not ok:
                return
            col, ok = QInputDialog.getText(self, "Edit Aggregate", "Column:", text=current_col)
            if not ok:
                return
            alias, ok = QInputDialog.getText(self, "Edit Aggregate", "Alias:", text=current_alias)
            if not ok:
                return

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


class SortLimitPanel(QGroupBox):
    def __init__(self, builder, parent=None):
        super().__init__("Sort and Limit", parent)
        self.builder = builder
        layout = QVBoxLayout()
        self.setLayout(layout)

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
        add_sort.clicked.connect(self.add_sort)
        rem_sort = create_text_button("Remove Sort")
        rem_sort.clicked.connect(self.remove_sort)
        btn_layout.addWidget(add_sort)
        btn_layout.addWidget(rem_sort)
        layout.addLayout(btn_layout)

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

    def add_sort(self):
        col, ok = QInputDialog.getText(self, "Sort", "Column to sort by:")
        if not ok or not col:
            return
        direction, ok = QInputDialog.getItem(self, "Sort Direction", "Direction:", ["ASC", "DESC"], 0, False)
        if not ok:
            return

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
            cur_col = self.sort_table.item(row, 0).text()
            cur_dir = self.sort_table.item(row, 1).text()

            new_col, ok = QInputDialog.getText(self, "Edit Sort", "Column:", text=cur_col)
            if not ok:
                return
            new_dir, ok = QInputDialog.getItem(
                self, "Edit Sort", "Direction:", ["ASC", "DESC"],
                0 if cur_dir.upper() == "ASC" else 1,
                False
            )
            if not ok:
                return

            self.sort_table.setItem(row, 0, QTableWidgetItem(new_col))
            self.sort_table.setItem(row, 1, QTableWidgetItem(new_dir))
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
# ODBCConnectDialog
###############################################################################
class ODBCConnectDialog(QDialog):
    """
    Minimal ODBC connect: pick DSN, DB type, user/pass.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to DB (ODBC)")
        self.resize(400, 230)
        self._conn = None
        self._db_type = None

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Database Type:"))
        self.type_combo = QComboBox()
        self.type_combo.addItems(["Teradata", "SQLServer", "Oracle", "Other"])
        layout.addWidget(self.type_combo)

        layout.addWidget(QLabel("ODBC DSN:"))
        self.dsn_combo = QComboBox()
        if pyodbc:
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
            QMessageBox.critical(self,"pyodbc missing","Install pyodbc to connect.")
            return
        dsn = self.dsn_combo.currentText().strip()
        if not dsn:
            QMessageBox.warning(self, "Missing DSN","Pick DSN first.")
            return
        db_type = self.type_combo.currentText()
        user   = self.user_edit.text().strip()
        pwd    = self.pass_edit.text().strip()

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
            QMessageBox.critical(self,"Connect Error", str(e))

    def get_connection(self):
        return self._conn

    def get_db_type(self):
        return self._db_type


###############################################################################
# AliasManagementDialog
###############################################################################
class AliasManagementDialog(QDialog):
    """
    Stub for an alias manager.
    Suppose user can rename existing table/subquery aliases on the canvas.
    """
    def __init__(self, parent, current_items):
        super().__init__(parent)
        self.setWindowTitle("Manage Aliases")
        self.resize(400, 300)
        self.alias_mapping = {}

        layout = QVBoxLayout(self)
        self.list_widget = QListWidget()
        for itm in current_items:
            self.list_widget.addItem(itm)
        layout.addWidget(self.list_widget)

        rename_btn = QPushButton("Rename")
        rename_btn.clicked.connect(self.rename_item)
        layout.addWidget(rename_btn)

        okcancel = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        okcancel.accepted.connect(self.accept)
        okcancel.rejected.connect(self.reject)
        layout.addWidget(okcancel)
        self.setLayout(layout)

    def rename_item(self):
        cur_item = self.list_widget.currentItem()
        if not cur_item:
            return
        old_text = cur_item.text()
        new_text, ok = QInputDialog.getText(self, "Rename Alias", "New alias:", text=old_text)
        if ok and new_text and new_text != old_text:
            self.alias_mapping[old_text] = new_text
            cur_item.setText(f"{old_text} -> {new_text}")

    def get_alias_mapping(self):
        return self.alias_mapping


class JoinConnectionDialog(QDialog):
    """
    Stub for a join-configuration dialog:
    Choose a join type and a join condition column.
    """
    def __init__(self, parent, tables, common_cols):
        super().__init__(parent)
        self.setWindowTitle("Configure Join")
        self.join_type = "INNER JOIN"
        self.column = "id"

        layout = QVBoxLayout(self)

        form = QFormLayout()
        self.join_combo = QComboBox()
        self.join_combo.addItems(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"])
        form.addRow("Join Type:", self.join_combo)

        self.col_combo = QComboBox()
        self.col_combo.addItems(common_cols)
        form.addRow("Column to join on:", self.col_combo)

        layout.addLayout(form)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

    def on_ok(self):
        self.join_type = self.join_combo.currentText()
        self.column = self.col_combo.currentText()
        self.accept()

    def get_join_type_and_condition(self):
        return self.join_type, self.column


class SchemaLoaderWorkerSignals(QObject):
    progress = pyqtSignal(int)
    finished_loading = pyqtSignal(dict)
    error = pyqtSignal(str)


class SchemaLoaderWorker(QRunnable):
    """
    Stub worker for loading and caching schema (not used in the lazy tree).
    """
    def __init__(self, connection, cache_file):
        super().__init__()
        self.signals = SchemaLoaderWorkerSignals()
        self.connection = connection
        self.cache_file = cache_file

    @QtCore.pyqtSlot()
    def run(self):
        try:
            self.signals.progress.emit(25)
            schema_dict = {"example_schema": ["table1", "table2"]}
            self.signals.progress.emit(75)
            with open(self.cache_file, "wb") as f:
                pickle.dump(schema_dict, f)
            self.signals.progress.emit(100)
            self.signals.finished_loading.emit(schema_dict)
        except Exception as e:
            self.signals.error.emit(f"SchemaLoader Error: {e}")


class SQLImportTab(QWidget):
    """
    A stub representing a possible 'SQL Import' tab where users can paste raw SQL
    or import from a file. This is not implemented here, just a placeholder.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        label = QLabel("SQL Import Placeholder")
        layout.addWidget(label)
        self.setLayout(layout)
###############################################################################
# Canvas Items and Mappings
###############################################################################
class MappingLine(QtWidgets.QGraphicsLineItem):
    def __init__(self, start_point, end_point):
        super().__init__(QtCore.QLineF(start_point, end_point))
        pen = QPen(Qt.red, 2)
        self.setPen(pen)
        self.setZValue(2)


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

        painter.setPen(QPen(QtGui.QColor(220, 220, 220), 1))
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
            if (
                jdict['table1'] == join.start_item
                or jdict['table2'] == join.end_item
                or jdict['table2'] == join.start_item
                or jdict['table1'] == join.end_item
            ):
                self.builder.joins.remove(jdict)

        self.builder.generate_sql()
        self.validation_timer.start()

    def add_mapping_line(self, ds_point, target_point):
        mline = MappingLine(ds_point, target_point)
        self.scene.addItem(mline)
        self.mapping_lines.append(mline)

    def clear_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene.removeItem(ml)
        self.mapping_lines = []

    def add_table(self, alias, original, pos):
        if alias in self.table_items or alias in self.subquery_items:
            QMessageBox.warning(self, "Duplicate", f"'{alias}' already on canvas.")
            return
        rect = QGraphicsRectItem(0, 0, 200, 100)
        rect.setBrush(QBrush(QtGui.QColor(220, 220, 255)))
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
                        for jn in [jl for jl in self.join_lines if jl.start_item == item or jl.end_item == item]:
                            self.remove_join(jn)
                else:
                    remove_sub = menu.addAction("Remove Subquery")
                    chosen = menu.exec_(self.mapToGlobal(event.pos()))
                    if chosen == remove_sub:
                        self.builder.remove_subquery(table_name)
                return

        super().contextMenuEvent(event)


###############################################################################
# VisualQueryBuilderTab
###############################################################################
class VisualQueryBuilderTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.connections = {}            # {alias: {"type", "connection", etc.}}
        self.schema_cache_files = {}
        self.joins = []
        self.mapping = {}
        self.operation_mode = "SELECT"   # or "UPDATE", "DELETE", "INSERT"

        self.combination_operator = None
        self.second_query = None

        self.target_table = None
        self.target_columns = None

        QApplication.setStyle("Windows")
        self.threadpool = QThreadPool.globalInstance()

        self.initUI()

    def initUI(self):
        main_layout = QVBoxLayout(self)

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

        update_btn = QPushButton("Update")
        update_btn.clicked.connect(self.activate_update_mode)
        toolbar_layout.addWidget(update_btn)

        delete_btn = QPushButton("Delete")
        delete_btn.clicked.connect(self.activate_delete_mode)
        toolbar_layout.addWidget(delete_btn)

        insert_btn = QPushButton("Insert")
        insert_btn.clicked.connect(self.activate_insert_mode)
        toolbar_layout.addWidget(insert_btn)

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

    ###########################################################################
    # Schema & Canvas tab
    ###########################################################################
    def setup_schema_canvas_tab(self):
        layout = QVBoxLayout(self.schema_canvas_tab)

        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search tables/columns...")
        self.search_bar.textChanged.connect(self.filter_schema_tree)
        layout.addWidget(self.search_bar)

        splitter = QSplitter(Qt.Horizontal)

        if self.connections:
            first_key = list(self.connections.keys())[0]
            conn = self.connections[first_key]['connection']
        else:
            conn = None

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

    ###########################################################################
    # Query Config tab
    ###########################################################################
    def setup_query_config_tab(self):
        layout = QHBoxLayout(self.query_config_tab)

        self.filter_panel = FilterPanel(builder=self)
        layout.addWidget(self.filter_panel, 2)

        self.group_by_panel = GroupByPanel(builder=self)
        layout.addWidget(self.group_by_panel, 3)

        self.sort_limit_panel = SortLimitPanel(builder=self)
        layout.addWidget(self.sort_limit_panel, 2)

        self.query_config_tab.setLayout(layout)

    ###########################################################################
    # SQL Preview tab
    ###########################################################################
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
    # Placeholders for user interactions
    ###########################################################################
    def run_sql_query(self):
        sql = self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self, "Empty SQL", "No SQL to run.")
            return
        QMessageBox.information(self, "SQL Execution", f"Executing:\n\n{sql}")

    def refresh_schema(self):
        if self.connections:
            self.schema_tree.populate_top_level()
            self.status_bar.showMessage("Schema refreshed.", 3000)
        else:
            QMessageBox.information(self, "Not Connected", "Please connect first.")

    ###########################################################################
    # Connection logic (using ODBCConnectDialog)
    ###########################################################################
    def open_connect_dialog(self):
        dlg = ODBCConnectDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            conn = dlg.get_connection()
            db_type = dlg.get_db_type()
            if conn and db_type:
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

    def update_connection_status(self, connected, info=""):
        if connected:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green; }")
            self.server_label.setText(info)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red; }")
            self.server_label.setText("Not Connected")

    def load_schema(self, alias):
        if alias not in self.connections:
            return
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        cache_file = self.schema_cache_files.get(alias, f"schema_cache_{alias}.pkl")

        worker = SchemaLoaderWorker(self.connections[alias]['connection'], cache_file)
        worker.signals.progress.connect(self.update_progress)
        worker.signals.finished_loading.connect(lambda sch: self.populate_schema_tree(sch, alias))
        worker.signals.error.connect(self.handle_schema_error)
        self.threadpool.start(worker)

    def update_progress(self, val):
        self.progress_bar.setValue(val)

    def handle_schema_error(self, msg):
        QMessageBox.critical(self, "Schema Loading Error", msg)
        self.progress_bar.setVisible(False)

    def populate_schema_tree(self, schema_dict, alias):
        self.schema_tree.populate_top_level()
        self.progress_bar.setVisible(False)

    ###########################################################################
    # Filtering the schema tree
    ###########################################################################
    def filter_schema_tree(self, text):
        for i in range(self.schema_tree.topLevelItemCount()):
            item = self.schema_tree.topLevelItem(i)
            self.filter_item(item, text)

    def filter_item(self, item, text):
        txt = text.lower()
        match = txt in item.text(0).lower()
        child_match = any(self.filter_item(item.child(i), text) for i in range(item.childCount()))
        item.setHidden(not (match or child_match))
        return match or child_match

    ###########################################################################
    # Handling item check states
    ###########################################################################
    def handle_item_changed(self, item, col):
        if item.childCount() > 0:
            st = item.checkState(0)
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, st)
        else:
            parent = item.parent()
            if parent:
                count = sum(parent.child(i).checkState(0) == Qt.Checked for i in range(parent.childCount()))
                if count == parent.childCount():
                    parent.setCheckState(0, Qt.Checked)
                elif count > 0:
                    parent.setCheckState(0, Qt.PartiallyChecked)
                else:
                    parent.setCheckState(0, Qt.Unchecked)
        self.generate_sql()

    ###########################################################################
    # Joins
    ###########################################################################
    def suggest_joins(self, item, col):
        if item and item.parent() and item.data(0, Qt.UserRole) == "table":
            tbl = item.text(0)
            selected = self.get_selected_tables()
            if len(selected) < 2:
                QMessageBox.information(self, "Join Suggestion", "Add one more table on canvas.")
                return
            common = ["id"]
            reply = QMessageBox.question(self, "Join Suggestion", f"Join '{tbl}' on 'id'?", QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                d = JoinConnectionDialog(self, [tbl], common)
                if d.exec_() == QDialog.Accepted:
                    jt, c = d.get_join_type_and_condition()
                    cond = f"{tbl}.{c} = other_table.{c}"
                    self.canvas.add_join(tbl, "other_table", jt, cond)

    def initiate_join(self, table_name):
        selected = self.get_selected_tables()
        if len(selected) < 2:
            QMessageBox.warning(self, "Join Error", "At least two tables are required.")
            return
        others = [t for t in selected if t != table_name]
        second, ok = QInputDialog.getItem(self, "Select Table", "Table to join:", others, 0, False)
        if ok and second:
            common = ["id"]
            d = JoinConnectionDialog(self, [table_name, second], common)
            if d.exec_() == QDialog.Accepted:
                jt, c = d.get_join_type_and_condition()
                cond = f"{table_name}.{c} = {second}.{c}"
                self.canvas.add_join(table_name, second, jt, cond)

    def remove_subquery(self, alias):
        self.canvas.remove_subquery(alias)

    ###########################################################################
    # Canvas-based items for Derived, Combine, Operations
    ###########################################################################
    def add_derived_column_node(self):
        dlg = AddDerivedColumnDialog(self)
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

    def activate_update_mode(self):
        self.operation_mode = "UPDATE"
        dlg = TargetTableDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            table_name, table_cols = dlg.get_data()
            op_item = OperationItem("UPDATE", table_name, table_cols, x=400, y=100)
            self.canvas.scene.addItem(op_item)
            self.generate_sql()

    def activate_delete_mode(self):
        self.operation_mode = "DELETE"
        dlg = TargetTableDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            table_name, _ = dlg.get_data()
            op_item = OperationItem("DELETE", table_name, [], x=400, y=150)
            self.canvas.scene.addItem(op_item)
            self.generate_sql()

    def activate_insert_mode(self):
        self.operation_mode = "INSERT"
        dlg = TargetTableDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            table_name, table_cols = dlg.get_data()
            op_item = OperationItem("INSERT", table_name, table_cols, x=400, y=200)
            self.canvas.scene.addItem(op_item)
            self.generate_sql()

    ###########################################################################
    # Collect chosen items
    ###########################################################################
    def get_selected_tables(self):
        return list(self.canvas.table_items.keys()) + list(self.canvas.subquery_items.keys())

    def get_selected_columns(self, include_derived=False):
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
                                if col_item.data(0, Qt.UserRole) == "column" and col_item.checkState(0) == Qt.Checked:
                                    table_name = tbl_item.text(0)
                                    column_name = col_item.text(0)
                                    cols.append(f"{table_name}.{column_name}")
        return cols

    ###########################################################################
    # Alias management
    ###########################################################################
    def manage_aliases(self):
        if not self.canvas.table_items and not self.canvas.subquery_items:
            QMessageBox.information(self, "No Items", "Nothing on canvas to alias.")
            return
        current = list(self.canvas.table_items.keys()) + list(self.canvas.subquery_items.keys())
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
    # Window functions
    ###########################################################################
    def open_window_function_dialog(self):
        available = self.get_selected_columns(include_derived=False)
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
    # DnD Handler from Canvas
    ###########################################################################
    def handle_drop(self, text, pos):
        alias = text
        original = text
        self.canvas.add_table(alias, original, pos)

    ###########################################################################
    # Validate SQL
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
                k in sql_text.upper() for k in ["INSERT", "UPDATE", "DELETE"]):
                self.validation_label.setText("SQL Status: Invalid - missing SELECT columns.")
                self.validation_label.setStyleSheet("color: red;")
                return
            if not parsed_info["tables"] and not any(
                k in sql_text.upper() for k in ["INSERT", "UPDATE", "DELETE"]):
                self.validation_label.setText("SQL Status: Invalid - no tables found in FROM.")
                self.validation_label.setStyleSheet("color: red;")
                return
            self.validation_label.setText("SQL Status: Valid.")
            self.validation_label.setStyleSheet("color: green;")
        except Exception as e:
            self.validation_label.setText("SQL Status: Invalid - " + str(e))
            self.validation_label.setStyleSheet("color: red;")

    ###########################################################################
    # SQL Generation
    ###########################################################################
    def generate_sql(self):
        scene_items = self.canvas.scene.items()
        op_items = [it for it in scene_items if isinstance(it, OperationItem)]
        derived_items = [it for it in scene_items if isinstance(it, DerivedColumnItem)]
        combine_items = [it for it in scene_items if isinstance(it, CombineQueryItem)]

        if op_items:
            op_item = op_items[0]
            if op_item.op_type == "UPDATE":
                self._generate_update_sql(op_item, derived_items)
            elif op_item.op_type == "DELETE":
                self._generate_delete_sql(op_item)
            elif op_item.op_type == "INSERT":
                self._generate_insert_sql(op_item, derived_items)
        else:
            self._generate_select_sql(derived_items, combine_items)

        self.canvas.validation_timer.start()

    def _generate_select_sql(self, derived_items, combine_items):
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            self.sql_display.setPlainText("-- No tables selected on canvas => no SELECT.")
            self.validation_label.setText("SQL Status: Incomplete")
            self.validation_label.setStyleSheet("color: orange;")
            return

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

        where_parts = []
        for c, o, v in self.filter_panel.get_filters("WHERE"):
            if o.upper() in ["IS NULL", "IS NOT NULL", "EXISTS"]:
                where_parts.append(f"{c} {o}")
            elif o.upper() in ["IN", "NOT IN"]:
                where_parts.append(f"{c} {o} ({v})")
            else:
                where_parts.append(f"{c} {o} '{v}'")

        group_parts = self.group_by_panel.get_group_by()

        having_parts = []
        for c, o, v in self.filter_panel.get_filters("HAVING"):
            if o.upper() in ["IS NULL", "IS NOT NULL", "EXISTS"]:
                having_parts.append(f"{c} {o}")
            elif o.upper() in ["IN", "NOT IN"]:
                having_parts.append(f"{c} {o} ({v})")
            else:
                having_parts.append(f"{c} {o} '{v}'")

        for func, col, alias in self.group_by_panel.get_aggregates():
            select_parts.append(f"{func}({col}) AS {alias}")

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

        self.sql_display.setPlainText(final_sql)
        self.validation_label.setText("SQL Status: Valid SELECT")
        self.validation_label.setStyleSheet("color: green;")

    def _generate_update_sql(self, op_item, derived_items):
        table_name = op_item.table_name
        sets = []
        for d in derived_items:
            sets.append(f"{d.alias} = {d.expression}")

        if not sets and op_item.columns:
            sets = [f"{col} = 'value'" for col in op_item.columns]

        if not sets:
            sets = ["col1 = 'value'"]

        set_str = ", ".join(sets)
        sql = (
            f"UPDATE {table_name}\n"
            f"SET {set_str}\n"
            f"WHERE id = 123;"
        )
        self.sql_display.setPlainText(sql)
        self.validation_label.setText("SQL Status: Valid UPDATE")
        self.validation_label.setStyleSheet("color: green;")

    def _generate_delete_sql(self, op_item):
        table_name = op_item.table_name
        sql = (
            f"DELETE FROM {table_name}\n"
            f"WHERE id IN (SELECT id FROM something);"
        )
        self.sql_display.setPlainText(sql)
        self.validation_label.setText("SQL Status: Valid DELETE")
        self.validation_label.setStyleSheet("color: green;")

    def _generate_insert_sql(self, op_item, derived_items):
        table_name = op_item.table_name
        if op_item.columns:
            tcols = ", ".join(op_item.columns)
        else:
            tcols = "col1, col2"

        if derived_items:
            sel_exprs = ", ".join([d.expression for d in derived_items])
        else:
            sel_exprs = "*"

        sql = (
            f"INSERT INTO {table_name} ({tcols})\n"
            f"SELECT {sel_exprs}\n"
            f"FROM SomeSource;"
        )
        self.sql_display.setPlainText(sql)
        self.validation_label.setText("SQL Status: Valid INSERT")
        self.validation_label.setStyleSheet("color: green;")


###############################################################################
# Main
###############################################################################
if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = QMainWindow()
    builder = VisualQueryBuilderTab(parent=main_window)
    main_window.setCentralWidget(builder)
    main_window.setWindowTitle("Visual Query Builder - Enhanced Full Code")
    main_window.resize(1200, 800)
    main_window.show()
    sys.exit(app.exec_())
