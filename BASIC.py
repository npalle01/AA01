#!/usr/bin/env python
# PART 1 of 4

import sys
import traceback
import pyodbc
import sqlparse

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import (
    Qt, QPointF, QTimer, QRegExp, QThreadPool, QRunnable, pyqtSignal, QObject
)
from PyQt5.QtGui import (
    QColor, QPen, QBrush, QFont, QSyntaxHighlighter, QTextCharFormat, QDrag
)
from PyQt5.QtWidgets import (QGraphicsItemGroup,QGraphicsLineItem,
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QPushButton, QSplitter,
    QLineEdit, QLabel, QDialog, QFormLayout, QComboBox, QTableWidget,
    QTableWidgetItem, QTabWidget, QMessageBox, QGraphicsView,
    QGraphicsScene, QGraphicsRectItem, QGraphicsTextItem, QGraphicsItem,
    QDialogButtonBox, QStatusBar, QGroupBox, QAbstractItemView,
    QSpinBox, QInputDialog, QListWidget, QMenu, QFrame, QProgressBar
)

pyodbc.pooling = True  # Enable connection pooling for Teradata

def create_text_button(text: str, tooltip: str = "") -> QPushButton:
    """
    Simple helper to create a text button with an optional tooltip.
    """
    btn = QPushButton(text)
    btn.setToolTip(tooltip)
    return btn

###############################################################################
# ODBCConnectDialog (Teradata Only)
###############################################################################
class ODBCConnectDialog(QDialog):
    """
    Minimal ODBC connect dialog (Teradata only).
    """
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

        # Username / Password
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
# SQL Parser Stub
###############################################################################
class SQLParser:
    """
    A minimal stub for parsing SQL. Uses sqlparse in a limited manner.
    """
    def __init__(self, sql):
        self.sql = sql

    def parse(self):
        sql_str = self.sql.strip()
        if not sql_str:
            raise ValueError("SQL is empty.")

        try:
            statements = sqlparse.parse(sql_str)
            if not statements:
                raise ValueError("No valid SQL found.")
        except Exception as e:
            raise ValueError(f"Error parsing SQL: {e}")

        # We'll do minimal checks
        # e.g., if it doesn't contain a FROM or is not recognized as INSERT, etc.
        # Real logic can be expanded
        return True

###############################################################################
# SQLHighlighter
###############################################################################
class SQLHighlighter(QSyntaxHighlighter):
    """
    Simple syntax highlighter for SQL.
    """
    def __init__(self, document):
        super().__init__(document)
        self.rules = []

        keyword_format = QTextCharFormat()
        keyword_format.setForeground(Qt.darkBlue)
        keyword_format.setFontWeight(QFont.Bold)

        keywords = [
            "SELECT","FROM","WHERE","JOIN","INNER","LEFT","RIGHT","FULL",
            "GROUP","BY","HAVING","ORDER","LIMIT","OFFSET","UNION","INTERSECT",
            "EXCEPT","AS","ON","AND","OR","NOT","IN","IS","NULL","EXISTS",
            "COUNT","SUM","AVG","MIN","MAX","INSERT","UPDATE","DELETE","VALUES"
        ]
        for word in keywords:
            pattern = QRegExp(r'\b' + word + r'\b', Qt.CaseInsensitive)
            self.rules.append((pattern, keyword_format))

        # String format
        string_format = QTextCharFormat()
        string_format.setForeground(Qt.darkRed)
        self.rules.append((QRegExp("'[^']*'"), string_format))
        self.rules.append((QRegExp('"[^"]*"'), string_format))

        # Comment format
        comment_format = QTextCharFormat()
        comment_format.setForeground(Qt.green)
        self.rules.append((QRegExp("--[^\n]*"), comment_format))
        self.rules.append((QRegExp("/\\*.*\\*/"), comment_format))

    def highlightBlock(self, text):
        for (pattern, fmt) in self.rules:
            index = pattern.indexIn(text)
            while index >= 0:
                length = pattern.matchedLength()
                self.setFormat(index, length, fmt)
                index = pattern.indexIn(text, index + length)
        self.setCurrentBlockState(0)
# PART 2 of 4

###############################################################################
# LazySchemaTreeWidget
###############################################################################
class LazySchemaLoaderWorkerSignals(QObject):
    finished = pyqtSignal(list)  # Emitted with list of tables
    error = pyqtSignal(str)      # Emitted with an error message

class LazySchemaLoaderWorker(QRunnable):
    """
    Worker that loads table names for a Teradata database (schema) in another thread.
    """
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
            err = f"Error loading tables for '{self.database_name}': {e}\n{traceback.format_exc()}"
            self.signals.error.emit(err)

class LazySchemaTreeWidget(QTreeWidget):
    """
    Displays connection (Teradata), databases, tables, columns in a hierarchical tree.
    Supports drag-and-drop of table names to the canvas.
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
        """
        Create a root node for the connection, then list the available databases.
        """
        self.clear()
        if not self.connection:
            conn_item = QTreeWidgetItem(["No Connection"])
            conn_item.setData(0, Qt.UserRole, "connection")
            self.addTopLevelItem(conn_item)
            return

        # Attempt to get a "name" from the connection
        try:
            conn_name = self.connection.getinfo(pyodbc.SQL_DBMS_NAME).strip()
            if "TERADATA" not in conn_name.upper():
                conn_name = "Teradata"
        except:
            conn_name = "Teradata"

        conn_item = QTreeWidgetItem([conn_name])
        conn_item.setData(0, Qt.UserRole, "connection")
        self.addTopLevelItem(conn_item)

        db_names = []
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
            db_names = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"[ERROR] Could not fetch DB names: {e}")

        if not db_names:
            no_db_item = QTreeWidgetItem(["<No databases found>"])
            conn_item.addChild(no_db_item)
            return

        for dbn in db_names:
            db_item = QTreeWidgetItem([dbn])
            db_item.setData(0, Qt.UserRole, "database")
            # Mark as not loaded
            db_item.setData(0, Qt.UserRole + 1, False)
            # Dummy child so it can be expanded
            dummy = QTreeWidgetItem(["Loading..."])
            db_item.addChild(dummy)
            conn_item.addChild(db_item)

        self.expandItem(conn_item)

    def on_item_expanded(self, item):
        data_type = item.data(0, Qt.UserRole)
        loaded = item.data(0, Qt.UserRole + 1)
        if data_type == "database" and not loaded:
            item.takeChildren()
            db_name = item.text(0)
            worker = LazySchemaLoaderWorker(self.connection, db_name)
            worker.signals.finished.connect(lambda tbls, it=item: self.populate_db_node(it, tbls))
            worker.signals.error.connect(self.handle_error)
            self.threadpool.start(worker)

        elif data_type == "table" and not loaded:
            item.takeChildren()
            db_name = item.parent().text(0)
            table_name = item.text(0)
            cols = self.load_columns(db_name, table_name)
            if cols:
                for c in cols:
                    col_item = QTreeWidgetItem([c])
                    col_item.setData(0, Qt.UserRole, "column")
                    col_item.setFlags(col_item.flags() | Qt.ItemIsUserCheckable)
                    col_item.setCheckState(0, Qt.Unchecked)
                    item.addChild(col_item)
            else:
                item.addChild(QTreeWidgetItem(["<No columns>"]))
            item.setData(0, Qt.UserRole + 1, True)

    def populate_db_node(self, db_item, tables):
        if not tables:
            db_item.addChild(QTreeWidgetItem(["<No tables found>"]))
        else:
            for t in tables:
                titem = QTreeWidgetItem([t])
                titem.setData(0, Qt.UserRole, "table")
                titem.setData(0, Qt.UserRole + 1, False)  # not loaded columns
                dummy = QTreeWidgetItem(["Loading columns..."])
                titem.addChild(dummy)
                db_item.addChild(titem)
        db_item.setData(0, Qt.UserRole + 1, True)

    def load_columns(self, db_name, table_name):
        """
        Return a list of column names for the given table.
        """
        cols = []
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
            cols = [row[0] for row in results]
        except Exception as e:
            print(f"[ERROR] load_columns failed for {db_name}.{table_name}: {e}")
        return cols

    def handle_error(self, msg):
        QMessageBox.critical(self, "Schema Load Error", msg)

    def startDrag(self, actions):
        """
        When user drags a table node, we set its text in the mimedata.
        """
        item = self.currentItem()
        if item and item.parent() and item.data(0, Qt.UserRole) == "table":
            drag = QDrag(self)
            mime = QtCore.QMimeData()
            mime.setText(item.text(0))
            drag.setMimeData(mime)
            drag.exec_(actions)
# PART 3 of 4

###############################################################################
# Canvas Items: Table, Column, and JoinEdge for "Tableau-like" UI
###############################################################################

class CanvasColumnItem(QGraphicsRectItem):
    """
    Represents a single column on the canvas, displayed as a small rectangle
    with text. Enables user to drag from one column to another to form a join.
    """
    def __init__(self, table_name, column_name, parent_table_item, x, y, width=100, height=20):
        super().__init__(0, 0, width, height, parent_table_item)
        self.table_name = table_name
        self.column_name = column_name
        self.setPos(x, y)
        self.setBrush(QBrush(QColor("#EEEEEE")))
        self.setPen(QPen(Qt.darkGray, 1))
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setAcceptHoverEvents(True)

        # A text label
        self.text_item = QGraphicsTextItem(f"{column_name}", self)
        self.text_item.setDefaultTextColor(Qt.black)
        self.text_item.setPos(5, 2)
        font = QFont("Arial", 8)
        self.text_item.setFont(font)

    def hoverEnterEvent(self, event):
        """
        Highlight column rect on hover to indicate it's clickable/draggable.
        """
        self.setBrush(QBrush(QColor("#DDDDFF")))
        super().hoverEnterEvent(event)

    def hoverLeaveEvent(self, event):
        """
        Restore original color when the mouse leaves.
        """
        self.setBrush(QBrush(QColor("#EEEEEE")))
        super().hoverLeaveEvent(event)

    def mousePressEvent(self, event):
        """
        On mouse press, store this column as the potential start of a drag.
        """
        if event.button() == Qt.LeftButton:
            # Access the scene's view to store the 'start column'
            view = self.scene().views()[0]  # Our EnhancedCanvasGraphicsView
            view.start_column_drag(self)
            event.accept()
        else:
            super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        """
        On mouse release, if there's a start column in the view, attempt to create a join.
        """
        if event.button() == Qt.LeftButton:
            view = self.scene().views()[0]
            view.end_column_drag(self)
            event.accept()
        else:
            super().mouseReleaseEvent(event)


class CanvasTableItem(QGraphicsItemGroup):
    """
    A QGraphicsItemGroup that displays a table rectangle + a list of CanvasColumnItem
    for each column. Emulates a "mini-table" on the canvas (like Tableau).
    """
    def __init__(self, table_name, columns, pos=QPointF(0, 0)):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.setFlags(QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemIsSelectable)

        # We'll create a background rect for the table "header"
        self.header_rect = QGraphicsRectItem()
        self.header_rect.setBrush(QBrush(QColor("#CFE2F3")))
        self.header_rect.setPen(QPen(Qt.darkBlue, 2))
        self.addToGroup(self.header_rect)

        # A text item for the table name
        self.title_text = QGraphicsTextItem(self.table_name)
        self.title_text.setDefaultTextColor(Qt.black)
        font = QFont("Arial", 10, QFont.Bold)
        self.title_text.setFont(font)
        self.addToGroup(self.title_text)

        # We'll lay out columns below the header
        self.column_items = []
        self._build_columns()

        # Position ourselves
        self.setPos(pos)
        # Perform an initial layout
        self._update_layout()

    def _build_columns(self):
        """
        Create a CanvasColumnItem for each column name.
        """
        y_offset = 0
        for col in self.columns:
            citem = CanvasColumnItem(
                table_name=self.table_name,
                column_name=col,
                parent_table_item=self,
                x=0,  # will be laid out later
                y=0   # will be laid out later
            )
            self.column_items.append(citem)
            self.addToGroup(citem)

    def _update_layout(self):
        """
        Position the header, the table name, and the column rectangles.
        """
        header_height = 30
        col_height = 20
        col_spacing = 2
        width = 120

        # Position the header rect
        self.header_rect.setRect(0, 0, width, header_height)

        # Center the title text
        title_brect = self.title_text.boundingRect()
        tx = (width - title_brect.width()) / 2
        ty = (header_height - title_brect.height()) / 2
        self.title_text.setPos(tx, ty)

        # Lay out columns
        current_y = header_height
        for citem in self.column_items:
            citem.setRect(0, 0, width, col_height)
            citem.setPos(0, current_y)
            current_y += col_height + col_spacing

    def boundingRect(self):
        """
        Required for a QGraphicsItemGroup, but we can just combine the items.
        """
        rect = self.childrenBoundingRect()
        return rect

    def update_positions(self):
        """
        Called if the item moves; we notify any join lines that might be connected.
        """
        for col in self.column_items:
            for edge in getattr(col, '_join_edges', []):
                edge.update_position()


class JoinEdgeItem(QGraphicsLineItem):
    """
    A line that sticks to two columns: a start ColumnItem and an end ColumnItem.
    The line remains connected as tables move.
    """
    def __init__(self, start_col_item, end_col_item):
        super().__init__()
        self.start_col_item = start_col_item
        self.end_col_item = end_col_item
        pen = QPen(Qt.red, 2)
        self.setPen(pen)
        self.setZValue(-1)

        # We store the reference in each column so they can update us on movement
        if not hasattr(self.start_col_item, '_join_edges'):
            self.start_col_item._join_edges = []
        if not hasattr(self.end_col_item, '_join_edges'):
            self.end_col_item._join_edges = []

        self.start_col_item._join_edges.append(self)
        self.end_col_item._join_edges.append(self)

        self.update_position()

        # We can optionally add a label on the line or not
        self.label_item = QGraphicsTextItem(f"{start_col_item.column_name} = {end_col_item.column_name}", self)
        self.label_item.setDefaultTextColor(Qt.darkRed)
        font = QFont("Arial", 8)
        self.label_item.setFont(font)
        self.label_item.setZValue(1)

    def update_position(self):
        """
        Recalculate the line endpoints based on column items' scene positions.
        """
        # Start
        start_pos = self.start_col_item.mapToScene(self.start_col_item.boundingRect().center())
        # End
        end_pos = self.end_col_item.mapToScene(self.end_col_item.boundingRect().center())

        self.setLine(QtCore.QLineF(start_pos, end_pos))

        # Move label to midpoint
        mid_x = (start_pos.x() + end_pos.x()) / 2
        mid_y = (start_pos.y() + end_pos.y()) / 2
        self.label_item.setPos(mid_x, mid_y)
# PART 4 of 4

###############################################################################
# Enhanced Canvas + Visual Query Builder
###############################################################################

class EnhancedCanvasGraphicsView(QGraphicsView):
    """
    A QGraphicsView that hosts table items (CanvasTableItem).
    Allows drag from one column to another to create sticky join lines.
    """
    def __init__(self, builder, parent=None):
        super().__init__(parent)
        self.builder = builder
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QGraphicsView.RubberBandDrag)

        self.drag_start_column = None  # Track the start of a column drag
        self.join_lines = []          # List[JoinEdgeItem]
        self.table_items = {}         # Dict[str, CanvasTableItem]

        # Timer to validate SQL after changes
        self.validation_timer = QTimer()
        self.validation_timer.setInterval(600)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.builder.validate_sql)

    def drawBackground(self, painter, rect):
        """
        Draw a grid background, like many visual query builder tools.
        """
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

    def start_column_drag(self, col_item):
        """
        Called when the user presses on a column (start of a join).
        """
        self.drag_start_column = col_item

    def end_column_drag(self, col_item):
        """
        Called when the user releases on a column (end of a join).
        """
        if not self.drag_start_column:
            return
        # If it's the same column or same table, ignore
        if col_item is self.drag_start_column:
            self.drag_start_column = None
            return
        if col_item.table_name == self.drag_start_column.table_name:
            QMessageBox.warning(self, "Invalid Join", "Cannot join a table to itself here.")
            self.drag_start_column = None
            return

        # Otherwise, create a join edge
        join_item = JoinEdgeItem(self.drag_start_column, col_item)
        self.scene.addItem(join_item)
        self.join_lines.append(join_item)

        # Also inform the builder that we have a new join
        self.builder.add_join(
            self.drag_start_column.table_name,
            self.drag_start_column.column_name,
            col_item.table_name,
            col_item.column_name
        )

        # Trigger a SQL generation and validation
        self.builder.generate_sql()
        self.validation_timer.start()

        # Clear the drag start reference
        self.drag_start_column = None

    def add_table_item(self, table_name, columns, pos):
        """
        Creates a CanvasTableItem on the scene for the given table/columns.
        """
        if table_name in self.table_items:
            QMessageBox.warning(self, "Already Placed", f"'{table_name}' is already on canvas.")
            return

        item = CanvasTableItem(table_name, columns, pos)
        self.scene.addItem(item)
        self.table_items[table_name] = item

        # We want to detect movement so we can update lines
        item.installSceneEventFilter(self)

        self.builder.generate_sql()
        self.validation_timer.start()

    def sceneEventFilter(self, watched, event):
        """
        Listen for item movement in the scene so we can update join lines.
        (Alternatively, we could override itemChange in CanvasTableItem.)
        """
        if event.type() == QtCore.QEvent.GraphicsSceneMove:
            if isinstance(watched, CanvasTableItem):
                watched.update_positions()
        return False

    def dropEvent(self, event):
        """
        If user drags a table from the schema tree, place it on the canvas
        by retrieving its columns from the DB. (For simplicity, we rely on the tree's text.)
        """
        pos = self.mapToScene(event.pos())
        table_name = event.mimeData().text().strip()
        # Ask the builder to get columns from the tree or from a direct DB lookup
        columns = self.builder.get_columns_for_table(table_name)

        # Place the table item
        self.add_table_item(table_name, columns, pos)
        event.acceptProposedAction()

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        event.acceptProposedAction()


class SQLImportTab(QWidget):
    """
    Minimal stub tab to avoid code-breaking.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("SQL Import Tab (Stub)"))
        self.setLayout(layout)


###############################################################################
# The Main Visual Query Builder Tab
###############################################################################
class VisualQueryBuilderTab(QWidget):
    """
    Main widget that includes:
      - Connection row
      - Schema tree / Canvas split
      - Query config / SQL preview
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.connections = {}
        self.joins = []  # We'll store join info as dict entries {table1, col1, table2, col2}
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
        connect_btn = QPushButton("Connect")
        connect_btn.clicked.connect(self.open_connect_dialog)

        conn_layout.addWidget(self.status_light)
        conn_layout.addWidget(self.server_label)
        conn_layout.addWidget(connect_btn)
        conn_layout.addStretch()
        main_layout.addLayout(conn_layout)

        # Tabs
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # 1) Schema & Canvas Tab
        self.schema_canvas_tab = QWidget()
        self.tabs.addTab(self.schema_canvas_tab, "Schema & Canvas")

        # 2) Query Configuration Tab (Stub)
        self.query_config_tab = QWidget()
        self.tabs.addTab(self.query_config_tab, "Query Configuration")

        # 3) SQL Preview
        self.sql_preview_tab = QWidget()
        self.tabs.addTab(self.sql_preview_tab, "SQL Preview")

        # 4) SQL Import Tab
        self.sql_import_tab = SQLImportTab(self)
        self.tabs.addTab(self.sql_import_tab, "SQL Import")

        # Status bar
        self.status_bar = QStatusBar()
        main_layout.addWidget(self.status_bar)

        self.setLayout(main_layout)

        self.setup_schema_canvas_tab()
        self.setup_query_config_tab()
        self.setup_sql_preview_tab()

    def setup_schema_canvas_tab(self):
        layout = QVBoxLayout(self.schema_canvas_tab)

        # Search bar
        self.search_bar = QLineEdit()
        self.search_bar.setPlaceholderText("Search tables/columns...")
        self.search_bar.textChanged.connect(self.filter_schema_tree)
        layout.addWidget(self.search_bar)

        # Splitter
        splitter = QSplitter(Qt.Horizontal)

        # We have no connection initially, so pass None
        self.schema_tree = LazySchemaTreeWidget(None)
        # Let's allow check states for columns
        self.schema_tree.itemChanged.connect(self.handle_item_changed)
        splitter.addWidget(self.schema_tree)

        # Canvas
        self.canvas = EnhancedCanvasGraphicsView(builder=self)
        splitter.addWidget(self.canvas)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)
        layout.addWidget(splitter)

        # Optional: progress bar for background loads
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

    def setup_query_config_tab(self):
        # Just a placeholder
        layout = QVBoxLayout(self.query_config_tab)
        layout.addWidget(QLabel("Query Config Panel (Stub)"))
        self.query_config_tab.setLayout(layout)

    def setup_sql_preview_tab(self):
        layout = QVBoxLayout(self.sql_preview_tab)

        # Header
        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("Generated SQL:"))
        run_btn = QPushButton("Run SQL")
        run_btn.clicked.connect(self.run_sql_query)
        top_bar.addWidget(run_btn, alignment=Qt.AlignRight)
        layout.addLayout(top_bar)

        # SQL display
        self.sql_display = QTextEdit()
        self.sql_display.setReadOnly(True)
        self.sql_highlighter = SQLHighlighter(self.sql_display.document())
        layout.addWidget(self.sql_display)

        # Validation label
        self.validation_label = QLabel("SQL Status: Unknown")
        layout.addWidget(self.validation_label)

        self.sql_preview_tab.setLayout(layout)

    ###########################################################################
    # Connection
    ###########################################################################
    def open_connect_dialog(self):
        dlg = ODBCConnectDialog(self)
        if dlg.exec_() == QDialog.Accepted:
            conn = dlg.get_connection()
            db_type = dlg.get_db_type()
            if conn and db_type and db_type.upper() == "TERADATA":
                # We'll store just one connection for simplicity
                self.connections["Teradata"] = {
                    "type": "Teradata",
                    "connection": conn
                }
                self.update_connection_status(True, "Teradata DSN")
                self.schema_tree.connection = conn
                self.schema_tree.populate_top_level()
                self.status_bar.showMessage("Connected!", 3000)
            else:
                QMessageBox.warning(self, "Only Teradata Allowed", "Restricted to Teradata DSNs.")

    def update_connection_status(self, connected, text=""):
        if connected:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green; }")
            self.server_label.setText(text)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red; }")
            self.server_label.setText("Not Connected")

    ###########################################################################
    # Schema Filtering
    ###########################################################################
    def filter_schema_tree(self, text):
        """
        Hide or show items based on the search text.
        """
        for i in range(self.schema_tree.topLevelItemCount()):
            item = self.schema_tree.topLevelItem(i)
            self._filter_item_recursive(item, text.lower())

    def _filter_item_recursive(self, item, pattern):
        txt = item.text(0).lower()
        match = pattern in txt
        child_match = False
        for i in range(item.childCount()):
            cchild = item.child(i)
            child_match = self._filter_item_recursive(cchild, pattern) or child_match
        item.setHidden(not (match or child_match))
        return match or child_match

    ###########################################################################
    # Tree Check State => Generate SQL
    ###########################################################################
    def handle_item_changed(self, item, col):
        """
        If user checks/unchecks columns, we can regenerate the SQL if needed.
        """
        # Simple logic: if a parent table is (un)checked, do likewise to columns
        if item.childCount() > 0:
            st = item.checkState(0)
            for i in range(item.childCount()):
                item.child(i).setCheckState(0, st)
        else:
            # might be a column => partial check if not all columns are checked
            parent = item.parent()
            checked_count = sum(
                parent.child(i).checkState(0) == Qt.Checked
                for i in range(parent.childCount())
            )
            if checked_count == parent.childCount():
                parent.setCheckState(0, Qt.Checked)
            elif checked_count == 0:
                parent.setCheckState(0, Qt.Unchecked)
            else:
                parent.setCheckState(0, Qt.PartiallyChecked)

        # We can generate SQL or do nothing special
        self.generate_sql()

    ###########################################################################
    # Canvas + Joins
    ###########################################################################
    def add_join(self, table1, col1, table2, col2):
        """
        Called by the canvas when a new join line is created.
        We store this in `self.joins` as a dict so we can build queries.
        """
        join_info = {
            "table1": table1,
            "col1": col1,
            "table2": table2,
            "col2": col2,
            "join_type": "INNER JOIN"  # Hard-coded for demo
        }
        self.joins.append(join_info)

    def get_columns_for_table(self, table_name):
        """
        Helper: fetch columns for a table from the schema tree (or from DB if needed).
        Here, we attempt to locate the table node in the tree and read child columns.
        """
        # A simple approach is to search the schema tree for the node matching table_name
        # and gather child columns. If not found, we could do a direct DB fetch.
        if "Teradata" not in self.connections:
            return []
        conn = self.connections["Teradata"]["connection"]
        # direct approach to get columns from DBC
        cols = []
        try:
            cursor = conn.cursor()
            # We'll guess the DatabaseName is the default user?
            # Or we parse table_name if it has db.table format.
            # For now, we skip that detail and let user just do "mydb.mytable".
            parts = table_name.split(".")
            if len(parts) == 2:
                db, tbl = parts
            else:
                # fallback to DBC approach
                db = conn.getinfo(pyodbc.SQL_USER_NAME)
                tbl = table_name
            query = f"""
                SELECT ColumnName
                FROM DBC.ColumnsV
                WHERE DatabaseName='{db}' AND TableName='{tbl}'
                ORDER BY ColumnId
            """
            cursor.execute(query)
            results = cursor.fetchall()
            cols = [row[0] for row in results]
        except Exception as e:
            print(f"[ERROR] get_columns_for_table({table_name}): {e}")
        return cols

    ###########################################################################
    # Generate & Validate SQL
    ###########################################################################
    def generate_sql(self):
        """
        Very simplistic generation of a SELECT with JOIN lines from self.joins.
        """
        # 1) Collect checked columns from the tree
        selected_columns = []
        for i in range(self.schema_tree.topLevelItemCount()):
            item = self.schema_tree.topLevelItem(i)
            self._collect_checked_columns_recursive(item, selected_columns)

        if not selected_columns:
            self.sql_display.setPlainText("-- No columns selected.\n")
            self.validation_label.setText("SQL Status: Incomplete")
            self.validation_label.setStyleSheet("color: orange;")
            return

        # 2) Figure out the FROM table(s).
        #    For now, let's guess the first part of each "table.col" is the table.
        #    We'll do a naive approach: pick the first table from the columns as "main FROM".
        #    Then if there's a join with other tables, we generate join statements.
        tables_seen = [c.split(".")[0] for c in selected_columns]
        main_table = tables_seen[0] if tables_seen else None
        lines = []
        lines.append(f"SELECT {', '.join(selected_columns)}")
        if main_table:
            lines.append(f"FROM {main_table}")

        # 3) Add JOIN lines from self.joins
        used_tables = set([main_table])
        for jdict in self.joins:
            t1 = jdict["table1"]
            c1 = jdict["col1"]
            t2 = jdict["table2"]
            c2 = jdict["col2"]
            jt = jdict["join_type"]
            if t1 == main_table:
                # e.g. "INNER JOIN T2 ON T1.c1 = T2.c2"
                lines.append(f"{jt} {t2} ON {t1}.{c1} = {t2}.{c2}")
                used_tables.add(t2)
            elif t2 == main_table:
                lines.append(f"{jt} {t1} ON {t2}.{c2} = {t1}.{c1}")
                used_tables.add(t1)
            else:
                # If neither table is the main table, we just do naive approach:
                # Join t2 onto t1
                if t1 not in used_tables:
                    lines.append(f"{jt} {t1} ON ???")
                    used_tables.add(t1)
                if t2 not in used_tables:
                    lines.append(f"{jt} {t2} ON {t1}.{c1} = {t2}.{c2}")
                    used_tables.add(t2)

        final_sql = "\n".join(lines)
        self.sql_display.setPlainText(final_sql)

        # Validate
        self.validation_timer = QTimer()
        self.validation_timer.setInterval(500)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.validate_sql)
        self.validation_timer.start()

    def _collect_checked_columns_recursive(self, item, col_list):
        """
        Recursively gather fully qualified columns from checked items in the tree.
        """
        if item.data(0, Qt.UserRole) == "column":
            if item.checkState(0) == Qt.Checked:
                # parent is the table
                tbl_item = item.parent()
                if tbl_item:
                    table_name = tbl_item.text(0)
                    col_list.append(f"{table_name}.{item.text(0)}")
        for i in range(item.childCount()):
            self._collect_checked_columns_recursive(item.child(i), col_list)

    def validate_sql(self):
        """
        Use the simple SQLParser to check if the generated SQL is "valid enough."
        """
        sql_text = self.sql_display.toPlainText().strip()
        if not sql_text:
            self.validation_label.setText("SQL Status: No SQL to validate.")
            self.validation_label.setStyleSheet("color: orange;")
            return
        try:
            parser = SQLParser(sql_text)
            parser.parse()
            self.validation_label.setText("SQL Status: Valid.")
            self.validation_label.setStyleSheet("color: green;")
        except Exception as e:
            self.validation_label.setText(f"SQL Status: Invalid - {e}")
            self.validation_label.setStyleSheet("color: red;")

    ###########################################################################
    # Query Execution Stub
    ###########################################################################
    def run_sql_query(self):
        sql = self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self, "Empty SQL", "No SQL to run.")
            return
        QMessageBox.information(self, "SQL Execution", f"Executing:\n\n{sql}")


###############################################################################
# Main for standalone run
###############################################################################
if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_window = QMainWindow()
    builder_tab = VisualQueryBuilderTab(parent=main_window)
    main_window.setCentralWidget(builder_tab)
    main_window.setWindowTitle("Visual Query Builder - 'Tableau-like' Drag & Drop")
    main_window.resize(1200, 800)
    main_window.show()
    sys.exit(app.exec_())
