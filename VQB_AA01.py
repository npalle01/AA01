#!/usr/bin/env python3
# truly_production_ready_vqb_enhanced.py
#
# A single-file Python/PyQt advanced Visual Query Builder for Teradata, featuring:
#  - ODBC connection
#  - Lazy schema with DB->Tables->Columns in a QTree
#  - Drag tables to a canvas (auto-FK detection & BFS multi-join)
#  - DML mapping lines (INSERT/UPDATE/DELETE) with a red partition line
#  - Pivot wizard, window function wizard
#  - Advanced expression builder (now with a "SubQuery" token)
#  - **Nested subquery item** that uses a **full VQB** instead of manual text
#  - SubVQBDialog for combining queries (UNION, etc.)
#  - Toggle auto-generate vs. manual SQL
#  - Single-file demonstration

import sys
import traceback
import logging
import pyodbc
import sqlparse

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import (
    Qt, QPointF, QTimer, QThreadPool, QRunnable, pyqtSignal, QObject,
    QRegularExpression
)
from PyQt5.QtGui import (
    QPalette, QColor, QPen, QBrush, QFont, QSyntaxHighlighter, QTextCharFormat
)
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QPushButton, QSplitter,
    QLineEdit, QLabel, QDialog, QFormLayout, QComboBox, QTableWidget,
    QTableWidgetItem, QTabWidget, QMessageBox, QGraphicsView,
    QGraphicsScene, QGraphicsRectItem, QGraphicsTextItem, QGraphicsItem,
    QGraphicsLineItem, QProgressBar, QDialogButtonBox, QStatusBar,
    QGroupBox, QAbstractItemView, QSpinBox, QMenu, QFrame, QAction,
    QDockWidget, QListWidget, QCheckBox
)

###############################################################################
# Logging & PyODBC Pooling
###############################################################################
logging.basicConfig(
    filename="vqb.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG
)
pyodbc.pooling = True
GRID_SIZE = 20

###############################################################################
# 1) Apply Fusion Style (narrower checkboxes/radio)
###############################################################################
def apply_fusion_style():
    """Applies a 'Fusion' style and sets narrower checkbox/radio indicators."""
    QApplication.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(240,240,240))
    palette.setColor(QPalette.WindowText, Qt.black)
    palette.setColor(QPalette.Base, QColor(255,255,255))
    palette.setColor(QPalette.AlternateBase, QColor(225,225,225))
    palette.setColor(QPalette.ToolTipBase, Qt.yellow)
    palette.setColor(QPalette.ToolTipText, Qt.black)
    palette.setColor(QPalette.Button, QColor(230,230,230))
    palette.setColor(QPalette.ButtonText, Qt.black)
    palette.setColor(QPalette.Highlight, QColor(76,163,224))
    palette.setColor(QPalette.HighlightedText, Qt.white)
    QApplication.setPalette(palette)

    style_sheet = """
        QCheckBox::indicator, QRadioButton::indicator {
            width: 12px;
            height: 12px;
            spacing: 2px;
        }
    """
    QApplication.instance().setStyleSheet(style_sheet)

###############################################################################
# 2) ODBCConnectDialog
###############################################################################
class ODBCConnectDialog(QDialog):
    """
    Dialog to connect to Teradata via an ODBC DSN with optional user/pass.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._conn = None
        self._db_type = None
        self.setWindowTitle("Connect to Teradata (ODBC)")
        self.resize(400,230)

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Database Type (Fixed to Teradata):"))
        self.type_label = QLabel("Teradata")
        lay.addWidget(self.type_label)

        lay.addWidget(QLabel("ODBC DSN (Teradata Only):"))
        self.dsn_combo = QComboBox()
        try:
            dsn_map = pyodbc.dataSources()
            for dsn in sorted(dsn_map.keys()):
                self.dsn_combo.addItem(dsn)
        except:
            pass
        lay.addWidget(self.dsn_combo)

        lay.addWidget(QLabel("Username (optional):"))
        self.user_edit = QLineEdit()
        lay.addWidget(self.user_edit)

        lay.addWidget(QLabel("Password (optional):"))
        self.pass_edit = QLineEdit()
        self.pass_edit.setEchoMode(QLineEdit.Password)
        lay.addWidget(self.pass_edit)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)
        self.setLayout(lay)

    def on_ok(self):
        dsn = self.dsn_combo.currentText().strip()
        if not dsn:
            QMessageBox.warning(self, "Missing DSN", "Please pick a DSN.")
            return
        db_type = "Teradata"
        user = self.user_edit.text().strip()
        pwd = self.pass_edit.text().strip()

        conn_str = f"DSN={dsn};"
        if user:
            conn_str+=f"UID={user};"
        if pwd:
            conn_str+=f"PWD={pwd};"

        try:
            cn = pyodbc.connect(conn_str, autocommit=True)
            self._conn = cn
            self._db_type = db_type
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Connect Error", f"Failed to connect: {e}")

    def get_connection(self):
        return self._conn

    def get_db_type(self):
        return self._db_type

###############################################################################
# 3) Lazy schema loading
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
            cur = self.connection.cursor()
            q = f"""
                SELECT TableName
                FROM DBC.TablesV
                WHERE DatabaseName='{self.database_name}' AND TableKind='T'
                ORDER BY TableName
            """
            cur.execute(q)
            rows = cur.fetchall()
            tables = [r[0] for r in rows]
            self.signals.finished.emit(tables)
        except Exception as ex:
            msg = f"Error loading tables for {self.database_name}: {ex}\n{traceback.format_exc()}"
            self.signals.error.emit(msg)

###############################################################################
# 4) load_foreign_keys
###############################################################################
def load_foreign_keys(connection):
    """
    Returns a dict child_key -> parent_key, e.g. "db.table.col" : "db2.table2.col2"
    """
    fk_map = {}
    try:
        cur = connection.cursor()
        q = """
        SELECT
            ChildDatabaseName, ChildTableName, ChildKeyColumnName,
            ParentDatabaseName, ParentTableName, ParentKeyColumnName
        FROM DBC.All_RI_Children
        ORDER BY ChildDatabaseName, ChildTableName
        """
        cur.execute(q)
        rows = cur.fetchall()
        for row in rows:
            cd = row.ChildDatabaseName.strip()
            ct = row.ChildTableName.strip()
            cc = row.ChildKeyColumnName.strip()
            pd = row.ParentDatabaseName.strip()
            pt = row.ParentTableName.strip()
            pc = row.ParentKeyColumnName.strip()
            child_key = f"{cd}.{ct}.{cc}"
            parent_key= f"{pd}.{pt}.{pc}"
            fk_map[child_key] = parent_key
    except Exception as ex:
        logging.warning(f"FK load failed or none found: {ex}")
    return fk_map

###############################################################################
# 5) LazySchemaTreeWidget
###############################################################################
class LazySchemaTreeWidget(QTreeWidget):
    """
    Displays DB->Tables->Columns in a lazy expansion tree. Also supports drag
    of table name onto the canvas.
    """
    def __init__(self, connection, parent_builder=None, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.parent_builder = parent_builder
        self.setHeaderHidden(True)
        self.setDragEnabled(True)
        self.threadpool = QThreadPool.globalInstance()
        self.setSelectionMode(QAbstractItemView.SingleSelection)
        self.populate_top_level()

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

        citem = QTreeWidgetItem([conn_name])
        citem.setData(0, Qt.UserRole, "connection")
        self.addTopLevelItem(citem)

        if not self.connection:
            return

        db_names=[]
        try:
            cur=self.connection.cursor()
            cur.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
            db_names=[r[0] for r in cur.fetchall()]
        except Exception as ex:
            QMessageBox.warning(self,"Error",f"Failed to fetch DB list: {ex}")

        if not db_names:
            citem.addChild(QTreeWidgetItem(["<No databases found>"]))
            return

        for dbn in db_names:
            db_item = QTreeWidgetItem([dbn])
            db_item.setData(0, Qt.UserRole, "database")
            db_item.setData(0, Qt.UserRole+1, False) # not loaded
            dummy = QTreeWidgetItem(["Loading..."])
            db_item.addChild(dummy)
            citem.addChild(db_item)

        self.expandItem(citem)

    def mouseDoubleClickEvent(self, event):
        it = self.itemAt(event.pos())
        if it:
            self.try_expand_item(it)
        super().mouseDoubleClickEvent(event)

    def try_expand_item(self, it):
        dt=it.data(0, Qt.UserRole)
        loaded=it.data(0,Qt.UserRole+1)
        if dt=="database" and not loaded:
            it.takeChildren()
            dbn=it.text(0)
            worker=LazySchemaLoaderWorker(self.connection,dbn)
            def on_finished(tbls):
                self.populate_db_node(it,tbls)
            def on_error(msg):
                QMessageBox.critical(self,"Schema Error",msg)
            worker.signals.finished.connect(on_finished)
            worker.signals.error.connect(on_error)
            self.threadpool.start(worker)
        elif dt=="table" and not loaded:
            it.takeChildren()
            dbn=it.parent().text(0)
            tn=it.text(0)
            cols=self.load_columns(dbn,tn)
            if cols:
                for c in cols:
                    ci=QTreeWidgetItem([c])
                    ci.setData(0,Qt.UserRole,"column")
                    ci.setFlags(ci.flags()|Qt.ItemIsUserCheckable)
                    ci.setCheckState(0,Qt.Unchecked)
                    it.addChild(ci)
            else:
                it.addChild(QTreeWidgetItem(["<No columns>"]))
            it.setData(0,Qt.UserRole+1,True)

    def populate_db_node(self, db_item, tables):
        if not tables:
            db_item.addChild(QTreeWidgetItem(["<No tables>"]))
            db_item.setData(0, Qt.UserRole+1, True)
            return
        db_item.takeChildren()
        for t in tables:
            titem=QTreeWidgetItem([t])
            titem.setData(0,Qt.UserRole,"table")
            titem.setData(0,Qt.UserRole+1,False)
            dummy=QTreeWidgetItem(["Loading..."])
            titem.addChild(dummy)
            db_item.addChild(titem)
        db_item.setData(0,Qt.UserRole+1,True)

    def load_columns(self, dbn, tn):
        cols=[]
        try:
            cur=self.connection.cursor()
            cur.execute(f"""
                SELECT ColumnName
                FROM DBC.ColumnsV
                WHERE DatabaseName='{dbn}' AND TableName='{tn}'
                ORDER BY ColumnId
            """)
            rows=cur.fetchall()
            cols=[r[0] for r in rows]
        except Exception as ex:
            QMessageBox.warning(self,"Error",f"Failed to fetch columns for {dbn}.{tn}: {ex}")
        return cols

    def startDrag(self, actions):
        it=self.currentItem()
        if it and it.parent() and it.data(0,Qt.UserRole)=="table":
            dbn=it.parent().text(0)
            tn=it.text(0)
            full=f"{dbn}.{tn}"
            drag=QtGui.QDrag(self)
            mime=QtCore.QMimeData()
            mime.setText(full)
            drag.setMimeData(mime)
            drag.exec_(actions)

###############################################################################
# 6) SQL Parsing & Highlighting
###############################################################################
class FullSQLParser:
    """Uses sqlparse to validate there's at least one statement."""
    def __init__(self, sql):
        self.sql=sql
    def parse(self):
        st=sqlparse.parse(self.sql)
        if not st:
            raise ValueError("No valid SQL found.")

class SQLHighlighter(QSyntaxHighlighter):
    """Simple SQL syntax highlighter."""
    def __init__(self, doc):
        super().__init__(doc)
        self.rules=[]
        kwfmt=QTextCharFormat()
        kwfmt.setForeground(Qt.darkBlue)
        kwfmt.setFontWeight(QFont.Bold)

        keywords=[
            "SELECT","FROM","WHERE","JOIN","INNER","LEFT","RIGHT","FULL","OUTER",
            "GROUP","BY","HAVING","ORDER","LIMIT","OFFSET","UNION","ALL","INTERSECT",
            "EXCEPT","AS","ON","AND","OR","NOT","IN","IS","NULL","EXISTS","COUNT",
            "SUM","AVG","MIN","MAX","INSERT","UPDATE","DELETE","VALUES","OVER",
            "PARTITION","ROWS","RANGE","CURRENT ROW","ROW_NUMBER","RANK","DENSE_RANK",
            "NTILE","LAG","LEAD","CASE","COALESCE","TRIM"
        ]
        for w in keywords:
            pattern=QRegularExpression(r'\b'+w+r'\b', QRegularExpression.CaseInsensitiveOption)
            self.rules.append((pattern,kwfmt))

        strfmt=QTextCharFormat()
        strfmt.setForeground(Qt.darkRed)
        self.rules.append((QRegularExpression(r"'[^']*'"), strfmt))
        self.rules.append((QRegularExpression(r'"[^"]*"'), strfmt))

        cfmt=QTextCharFormat()
        cfmt.setForeground(Qt.green)
        self.rules.append((QRegularExpression(r'--[^\n]*'), cfmt))
        self.rules.append((QRegularExpression(r'/\*.*\*/', QRegularExpression.DotMatchesEverythingOption), cfmt))

    def highlightBlock(self, text):
        for pattern, fmt in self.rules:
            matches=pattern.globalMatch(text)
            while matches.hasNext():
                m=matches.next()
                st=m.capturedStart()
                ln=m.capturedLength()
                self.setFormat(st, ln, fmt)
        self.setCurrentBlockState(0)

###############################################################################
# 7) MappingLine, JoinLine
###############################################################################
class MappingLine(QGraphicsLineItem):
    """Used to map source columns to target columns for DML. Painted in red."""
    def __init__(self, source_text_item, target_text_item,
                 source_type=None, target_type=None):
        super().__init__()
        self.source_text_item=source_text_item
        self.target_text_item=target_text_item
        self.source_col=source_text_item.toPlainText()
        self.target_col=target_text_item.toPlainText()
        self.source_type=source_type
        self.target_type=target_type
        self.setPen(QPen(Qt.darkRed,2,Qt.SolidLine))
        self.setZValue(5)
        self.update_pos()

    def update_pos(self):
        s=self.source_text_item.mapToScene(self.source_text_item.boundingRect().center())
        t=self.target_text_item.mapToScene(self.target_text_item.boundingRect().center())
        self.setLine(QtCore.QLineF(s,t))

    def paint(self, painter, option, widget):
        self.update_pos()
        super().paint(painter,option,widget)

class JoinLine(QGraphicsLineItem):
    def __init__(self, start_item, end_item,
                 join_type="INNER", condition=""):
        super().__init__()
        self.start_item=start_item
        self.end_item=end_item
        self.join_type=join_type
        self.condition=condition
        self.setZValue(-1)
        self.setAcceptHoverEvents(True)

        self.pen_map={
            "INNER":(Qt.darkBlue, Qt.SolidLine),
            "LEFT": (Qt.darkGreen, Qt.SolidLine),
            "RIGHT":(Qt.magenta, Qt.DotLine),
            "FULL": (Qt.red, Qt.DashLine),
        }
        self.label=QGraphicsTextItem(self.join_type, self)
        self.label.setDefaultTextColor(Qt.blue)
        self.update_line()

    def update_line(self):
        s=self.start_item.scenePos()+QPointF(100,30)
        e=self.end_item.scenePos()+QPointF(100,30)
        self.setLine(QtCore.QLineF(s,e))
        mx=(s.x()+e.x())/2
        my=(s.y()+e.y())/2
        self.label.setPos(mx,my)
        color,style=self.pen_map.get(self.join_type,(Qt.gray,Qt.SolidLine))
        self.setPen(QPen(color,2,style))

    def hoverEnterEvent(self,e):
        p=self.pen()
        p.setColor(Qt.yellow)
        p.setWidth(3)
        self.setPen(p)
        super().hoverEnterEvent(e)

    def hoverLeaveEvent(self,e):
        self.update_line()
        super().hoverLeaveEvent(e)

###############################################################################
# 8) NestedSubqueryItem that uses a Full VQB
###############################################################################
class NestedVQBDialog(QDialog):
    """
    A full VQB in a dialog, used specifically for Nested SubQuery items.
    """
    def __init__(self, existing_sql="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Nested SubVQB")
        self.resize(900,600)
        self.vqb_tab = None
        self.existing_sql = existing_sql

        # Build the layout
        layout = QVBoxLayout(self)
        self.vqb_tab = VisualQueryBuilderTab()
        layout.addWidget(self.vqb_tab)

        btns = QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self.setLayout(layout)

        # Optionally, parse existing_sql if you want.  For now, we just show an empty builder.

    def get_built_sql(self):
        return self.vqb_tab.sql_display.toPlainText().strip()

class NestedSubqueryItem(QGraphicsRectItem):
    """Rect representing a nested subquery on the canvas, double-click to open a full sub-VQB."""
    def __init__(self, x=0,y=0):
        super().__init__(0,0,220,80)
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(200,255,200)))
        self.setPen(QPen(Qt.darkGreen,2))
        self.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
        self.query_text="-- Subquery"
        self.label=QGraphicsTextItem("Nested SubQuery\n(double-click)", self)
        self.label.setPos(5,5)
        f=QFont("Arial",9,QFont.Bold)
        self.label.setFont(f)

    def mouseDoubleClickEvent(self, event):
        # open a full nested VQB dialog
        dlg = NestedVQBDialog(existing_sql=self.query_text)
        if dlg.exec_() == QDialog.Accepted:
            new_sql = dlg.get_built_sql()
            if new_sql:
                self.query_text = new_sql
                self.label.setPlainText("Nested SubQuery\n(VQB built)")
        event.accept()

    def get_sql(self):
        return self.query_text

###############################################################################
# 9) WindowFunctionDialog & ExpressionBuilder with a "SubQuery" button
###############################################################################
class WindowFunctionDialog(QDialog):
    """Minimal wizard for window function."""
    def __init__(self, available_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Window Function Wizard")
        self.resize(500,400)
        self.available_cols=available_cols
        self.function=""
        self.partition_cols=[]
        self.order_cols=[]
        self.frame_clause=""
        self.alias=""

        layout=QVBoxLayout(self)
        form=QFormLayout()

        self.func_combo=QComboBox()
        self.func_combo.addItems(["ROW_NUMBER","RANK","DENSE_RANK","NTILE","LAG","LEAD"])
        form.addRow("Function:",self.func_combo)

        self.part_list=QListWidget()
        self.part_list.addItems(self.available_cols)
        self.part_list.setSelectionMode(QAbstractItemView.MultiSelection)
        form.addRow("Partition By:", self.part_list)

        self.order_list=QListWidget()
        self.order_list.addItems(self.available_cols)
        self.order_list.setSelectionMode(QAbstractItemView.MultiSelection)
        form.addRow("Order By:", self.order_list)

        self.frame_edit=QLineEdit()
        self.frame_edit.setPlaceholderText("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
        form.addRow("Frame Clause:",self.frame_edit)

        self.alias_edit=QLineEdit("win_alias")
        form.addRow("Alias:",self.alias_edit)

        layout.addLayout(form)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)

        self.setLayout(layout)

    def on_ok(self):
        fn=self.func_combo.currentText()
        p=[it.text() for it in self.part_list.selectedItems()]
        o=[it.text() for it in self.order_list.selectedItems()]
        fr=self.frame_edit.text().strip()
        al=self.alias_edit.text().strip()
        if not al:
            QMessageBox.warning(self,"No alias","Alias needed.")
            return
        self.function=fn
        self.partition_cols=p
        self.order_cols=o
        self.frame_clause=fr
        self.alias=al
        self.accept()

    def get_expression(self):
        parts=[]
        if self.partition_cols:
            parts.append("PARTITION BY "+", ".join(self.partition_cols))
        if self.order_cols:
            parts.append("ORDER BY "+", ".join(self.order_cols))
        if self.frame_clause:
            parts.append(self.frame_clause)
        if parts:
            inside=" ".join(parts)
            over=f"({inside})"
        else:
            over="()"
        return f"{self.function}(){over} AS {self.alias}"

class AdvancedExpressionBuilderDialog(QDialog):
    """Token-based expression builder with minimal freehand + subquery token."""
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Advanced Expression Builder")
        self.available_columns=available_columns or []
        self.expression_tokens=[]
        self.alias=""

        layout=QVBoxLayout(self)
        form=QFormLayout()

        self.preview_edit=QLineEdit()
        self.preview_edit.setReadOnly(True)
        form.addRow("Expression Preview:", self.preview_edit)
        layout.addLayout(form)

        token_h=QHBoxLayout()

        self.col_combo=QComboBox()
        self.col_combo.addItems(["(Pick Col)"]+self.available_columns)
        col_btn=QPushButton("Col")
        col_btn.clicked.connect(self.add_col_token)
        token_h.addWidget(self.col_combo)
        token_h.addWidget(col_btn)

        self.op_combo=QComboBox()
        self.op_combo.addItems(["+","-","*","/","=","<",">","<=",">=","<>","AND","OR","LIKE"])
        op_btn=QPushButton("Op")
        op_btn.clicked.connect(self.add_op_token)
        token_h.addWidget(self.op_combo)
        token_h.addWidget(op_btn)

        self.func_combo=QComboBox()
        self.func_combo.addItems(["UPPER","LOWER","ABS","COALESCE","SUBSTR","TRIM","CASE("])
        func_btn=QPushButton("Func")
        func_btn.clicked.connect(self.add_func_token)
        token_h.addWidget(self.func_combo)
        token_h.addWidget(func_btn)

        paren_l=QPushButton("(")
        paren_l.clicked.connect(lambda: self.add_token("("))
        paren_r=QPushButton(")")
        paren_r.clicked.connect(lambda: self.add_token(")"))
        token_h.addWidget(paren_l)
        token_h.addWidget(paren_r)

        # Add a "SubQuery" token
        subq_btn=QPushButton("SubQuery")
        subq_btn.clicked.connect(self.add_subquery_token)
        token_h.addWidget(subq_btn)

        undo_btn=QPushButton("Undo")
        undo_btn.clicked.connect(self.remove_last_token)
        token_h.addWidget(undo_btn)

        layout.addLayout(token_h)

        alias_form=QFormLayout()
        self.alias_edit=QLineEdit("ExprAlias")
        alias_form.addRow("Alias:", self.alias_edit)
        layout.addLayout(alias_form)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)

        self.setLayout(layout)

    def add_col_token(self):
        c=self.col_combo.currentText()
        if c and c!="(Pick Col)":
            self.add_token(c)

    def add_op_token(self):
        op=self.op_combo.currentText()
        self.add_token(op)

    def add_func_token(self):
        fn=self.func_combo.currentText()
        if fn:
            self.add_token(fn)

    def add_subquery_token(self):
        """Open a full sub-VQB and insert it as (subquery)."""
        dlg = SubVQBDialog()  # or we can create a specialized NestedVQBDialog
        if dlg.exec_() == QDialog.Accepted:
            op, second_sql = dlg.getResult()
            # SubVQBDialog always returns an operator and the built SQL. We can ignore operator here.
            if second_sql:
                token_str = f"({second_sql})"
                self.add_token(token_str)

    def add_token(self, token):
        self.expression_tokens.append(token)
        self.refresh_preview()

    def remove_last_token(self):
        if self.expression_tokens:
            self.expression_tokens.pop()
        self.refresh_preview()

    def refresh_preview(self):
        self.preview_edit.setText(" ".join(self.expression_tokens))

    def on_ok(self):
        if not self.expression_tokens:
            QMessageBox.warning(self,"No Expression","No tokens in expression.")
            return
        a=self.alias_edit.text().strip()
        if not a:
            QMessageBox.warning(self,"No Alias","Alias is required.")
            return
        self.alias=a
        self.accept()

    def get_expression_data(self):
        expr="".join(self.expression_tokens)
        return (self.alias,expr)

###############################################################################
# 10) FilterPanel, PivotDialog, GroupByPanel, SortLimitPanel
###############################################################################
class AddFilterDialog(QDialog):
    """Dialog to add a single filter condition."""
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Filter")
        self.selected_col=None
        self.selected_op=None
        self.selected_val=None

        layout=QFormLayout(self)
        self.col_combo=QComboBox()
        self.col_combo.addItems(available_columns)
        layout.addRow("Column:",self.col_combo)
        self.op_combo=QComboBox()
        self.op_combo.addItems(["=","<>","<",">","<=",">=","IS NULL","IS NOT NULL"])
        layout.addRow("Operator:",self.op_combo)
        self.val_edit=QLineEdit("'ABC'")
        layout.addRow("Value:",self.val_edit)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(layout)

    def on_ok(self):
        c=self.col_combo.currentText()
        if not c:
            QMessageBox.warning(self,"No Column","Must pick a column.")
            return
        self.selected_col=c
        self.selected_op=self.op_combo.currentText()
        self.selected_val=self.val_edit.text().strip()
        self.accept()

    def get_filter(self):
        return (self.selected_col,self.selected_op,self.selected_val)

class FilterPanel(QGroupBox):
    """WHERE / HAVING filter management."""
    def __init__(self,builder,parent=None):
        super().__init__("Filters",parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        self.setLayout(layout)
        self.tabs=QTabWidget()
        layout.addWidget(self.tabs)

        self.where_tab=QWidget()
        self.having_tab=QWidget()
        self.tabs.addTab(self.where_tab,"WHERE")
        self.tabs.addTab(self.having_tab,"HAVING")

        # WHERE
        self.where_layout=QVBoxLayout(self.where_tab)
        self.where_table=QTableWidget(0,3)
        self.where_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.where_table.horizontalHeader().setStretchLastSection(True)
        self.where_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.where_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.where_layout.addWidget(self.where_table)

        wh_btn=QHBoxLayout()
        add_w=QPushButton("Add WHERE")
        add_w.clicked.connect(lambda: self.add_filter("WHERE"))
        rm_w=QPushButton("Remove WHERE")
        rm_w.clicked.connect(lambda: self.remove_filter("WHERE"))
        wh_btn.addWidget(add_w)
        wh_btn.addWidget(rm_w)
        self.where_layout.addLayout(wh_btn)

        # HAVING
        self.having_layout=QVBoxLayout(self.having_tab)
        self.having_table=QTableWidget(0,3)
        self.having_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.having_table.horizontalHeader().setStretchLastSection(True)
        self.having_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.having_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.having_layout.addWidget(self.having_table)

        hv_btn=QHBoxLayout()
        add_h=QPushButton("Add HAVING")
        add_h.clicked.connect(lambda: self.add_filter("HAVING"))
        rm_h=QPushButton("Remove HAVING")
        rm_h.clicked.connect(lambda: self.remove_filter("HAVING"))
        hv_btn.addWidget(add_h)
        hv_btn.addWidget(rm_h)
        self.having_layout.addLayout(hv_btn)

    def add_filter(self, clause):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No Columns","No columns available.")
            return
        dlg=AddFilterDialog(cols,self)
        if dlg.exec_()==QDialog.Accepted:
            c,o,v=dlg.get_filter()
            table=self.where_table if clause=="WHERE" else self.having_table
            r=table.rowCount()
            table.insertRow(r)
            table.setItem(r,0,QTableWidgetItem(c))
            table.setItem(r,1,QTableWidgetItem(o))
            table.setItem(r,2,QTableWidgetItem(v))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_filter(self, clause):
        table=self.where_table if clause=="WHERE" else self.having_table
        rows=sorted([x.row() for x in table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_filters(self, clause):
        table=self.where_table if clause=="WHERE" else self.having_table
        arr=[]
        for r in range(table.rowCount()):
            col=table.item(r,0).text()
            op =table.item(r,1).text()
            val=table.item(r,2).text()
            arr.append((col,op,val))
        return arr

class PivotDialog(QDialog):
    """Minimal pivot wizard. Real usage may query distinct categories from DB."""
    def __init__(self, available_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Pivot Wizard")
        self.category_col=None
        self.value_col=None
        self.distinct_vals=[]
        layout=QVBoxLayout(self)

        form=QFormLayout()
        self.cat_combo=QComboBox()
        self.cat_combo.addItems(available_cols)
        form.addRow("Category Column:", self.cat_combo)

        self.val_combo=QComboBox()
        self.val_combo.addItems(available_cols)
        form.addRow("Value Column:", self.val_combo)

        layout.addLayout(form)

        self.val_list=QListWidget()
        self.val_list.setSelectionMode(QAbstractItemView.MultiSelection)
        layout.addWidget(QLabel("Pick categories (demo):"))
        layout.addWidget(self.val_list)

        load_btn=QPushButton("Load Distinct (Demo)")
        load_btn.clicked.connect(self.on_load_demo)
        layout.addWidget(load_btn)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(layout)

    def on_load_demo(self):
        self.val_list.clear()
        for v in ["Manager","Clerk","Sales","IT","HR"]:
            self.val_list.addItem(v)

    def on_ok(self):
        cat=self.cat_combo.currentText()
        val=self.val_combo.currentText()
        if not cat or not val:
            QMessageBox.warning(self,"PivotWizard","Must pick cat & value.")
            return
        self.category_col=cat
        self.value_col=val
        self.distinct_vals=[it.text() for it in self.val_list.selectedItems()]
        self.accept()

    def build_expressions(self):
        arr=[]
        for dv in self.distinct_vals:
            alias=dv.lower().replace(" ","_")+"_val"
            expr=f"SUM(CASE WHEN {self.category_col}='{dv}' THEN {self.value_col} END) AS {alias}"
            arr.append(expr)
        return arr

class GroupByPanel(QGroupBox):
    """Lets user select group-by columns and define aggregates. Also pivot wizard."""
    def __init__(self,builder,parent=None):
        super().__init__("Group By & Aggregates (+Pivot)",parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        self.setLayout(layout)

        self.gb_table=QTableWidget(0,1)
        self.gb_table.setHorizontalHeaderLabels(["Group By Columns"])
        self.gb_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.gb_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.gb_table)

        gb_btn=QHBoxLayout()
        add_gb=QPushButton("Add GroupBy")
        add_gb.clicked.connect(self.add_group_by)
        rm_gb=QPushButton("Remove GroupBy")
        rm_gb.clicked.connect(self.remove_group_by)
        gb_btn.addWidget(add_gb)
        gb_btn.addWidget(rm_gb)
        layout.addLayout(gb_btn)

        self.agg_table=QTableWidget(0,3)
        self.agg_table.setHorizontalHeaderLabels(["Function","Column","Alias"])
        self.agg_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.agg_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.agg_table)

        agg_btn=QHBoxLayout()
        add_agg=QPushButton("Add Agg")
        add_agg.clicked.connect(self.add_agg)
        rm_agg=QPushButton("Remove Agg")
        rm_agg.clicked.connect(self.remove_agg)
        agg_btn.addWidget(add_agg)
        agg_btn.addWidget(rm_agg)
        layout.addLayout(agg_btn)

        self.pivot_btn=QPushButton("Pivot Wizard")
        self.pivot_btn.clicked.connect(self.launch_pivot)
        layout.addWidget(self.pivot_btn)

    def add_group_by(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No Columns","No columns available.")
            return
        (c,ok)=QtWidgets.QInputDialog.getItem(self,"Add GroupBy","Pick column:",cols,0,False)
        if ok and c:
            r=self.gb_table.rowCount()
            self.gb_table.insertRow(r)
            self.gb_table.setItem(r,0,QTableWidgetItem(c))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_group_by(self):
        rows=sorted([x.row() for x in self.gb_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            self.gb_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def add_agg(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No cols","No columns available.")
            return
        d=QDialog(self)
        d.setWindowTitle("Add Aggregate")
        fl=QFormLayout(d)
        func_cb=QComboBox()
        func_cb.addItems(["COUNT","SUM","AVG","MIN","MAX"])
        col_cb=QComboBox()
        col_cb.addItems(cols)
        alias_ed=QLineEdit("AggVal")
        fl.addRow("Function:",func_cb)
        fl.addRow("Column:",col_cb)
        fl.addRow("Alias:",alias_ed)
        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        fl.addWidget(btns)
        def on_ok():
            if not col_cb.currentText():
                QMessageBox.warning(d,"Error","Pick a column.")
                return
            d.accept()
        btns.accepted.connect(on_ok)
        btns.rejected.connect(d.reject)
        d.setLayout(fl)
        if d.exec_()==QDialog.Accepted:
            f=func_cb.currentText()
            c=col_cb.currentText()
            a=alias_ed.text().strip()
            r=self.agg_table.rowCount()
            self.agg_table.insertRow(r)
            self.agg_table.setItem(r,0,QTableWidgetItem(f))
            self.agg_table.setItem(r,1,QTableWidgetItem(c))
            self.agg_table.setItem(r,2,QTableWidgetItem(a))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_agg(self):
        rows=sorted([x.row() for x in self.agg_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            self.agg_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def launch_pivot(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns available.")
            return
        dlg=PivotDialog(cols,self)
        if dlg.exec_()==QDialog.Accepted:
            exs=dlg.build_expressions()
            for ex in exs:
                r=self.agg_table.rowCount()
                self.agg_table.insertRow(r)
                self.agg_table.setItem(r,0,QTableWidgetItem("CUSTOM"))
                self.agg_table.setItem(r,1,QTableWidgetItem(ex))
                self.agg_table.setItem(r,2,QTableWidgetItem("PivotVal"))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def get_group_by(self):
        arr=[]
        for r in range(self.gb_table.rowCount()):
            it=self.gb_table.item(r,0)
            arr.append(it.text())
        return arr

    def get_aggregates(self):
        ags=[]
        for r in range(self.agg_table.rowCount()):
            f=self.agg_table.item(r,0).text()
            c=self.agg_table.item(r,1).text()
            a=self.agg_table.item(r,2).text()
            ags.append((f,c,a))
        return ags

class SortLimitPanel(QGroupBox):
    """Sort + Limit/Offset management."""
    def __init__(self,builder,parent=None):
        super().__init__("Sort and Limit",parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        self.setLayout(layout)

        self.sort_table=QTableWidget(0,2)
        self.sort_table.setHorizontalHeaderLabels(["Column","Direction"])
        self.sort_table.horizontalHeader().setStretchLastSection(True)
        self.sort_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sort_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.sort_table)

        btn_h=QHBoxLayout()
        add_s=QPushButton("Add Sort")
        add_s.clicked.connect(self.add_sort_dialog)
        rm_s=QPushButton("Remove Sort")
        rm_s.clicked.connect(self.remove_sort)
        btn_h.addWidget(add_s)
        btn_h.addWidget(rm_s)
        layout.addLayout(btn_h)

        lo_h=QHBoxLayout()
        self.limit_spin=QSpinBox()
        self.limit_spin.setRange(0,9999999)
        self.limit_spin.setValue(0)
        self.limit_spin.setSuffix(" (Limit)")
        self.limit_spin.setSpecialValueText("No Limit")
        self.limit_spin.valueChanged.connect(self._maybe_regen)
        lo_h.addWidget(self.limit_spin)

        self.offset_spin=QSpinBox()
        self.offset_spin.setRange(0,9999999)
        self.offset_spin.setValue(0)
        self.offset_spin.setSuffix(" (Offset)")
        self.offset_spin.setSpecialValueText("No Offset")
        self.offset_spin.valueChanged.connect(self._maybe_regen)
        lo_h.addWidget(self.offset_spin)
        layout.addLayout(lo_h)

    def _maybe_regen(self):
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def add_sort_dialog(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns available.")
            return
        d=QDialog(self)
        d.setWindowTitle("Add Sort")
        fl=QFormLayout(d)
        col_cb=QComboBox()
        col_cb.addItems(cols)
        dir_cb=QComboBox()
        dir_cb.addItems(["ASC","DESC"])
        fl.addRow("Column:",col_cb)
        fl.addRow("Direction:",dir_cb)
        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        fl.addWidget(btns)
        def on_ok():
            if not col_cb.currentText():
                QMessageBox.warning(d,"No col","Pick a column.")
                return
            d.accept()
        btns.accepted.connect(on_ok)
        btns.rejected.connect(d.reject)
        d.setLayout(fl)
        if d.exec_()==QDialog.Accepted:
            c=col_cb.currentText()
            dd=dir_cb.currentText()
            row=self.sort_table.rowCount()
            self.sort_table.insertRow(row)
            self.sort_table.setItem(row,0,QTableWidgetItem(c))
            self.sort_table.setItem(row,1,QTableWidgetItem(dd))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_sort(self):
        rows=sorted([x.row() for x in self.sort_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            self.sort_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_order_bys(self):
        arr=[]
        for r in range(self.sort_table.rowCount()):
            col=self.sort_table.item(r,0).text()
            dr =self.sort_table.item(r,1).text()
            arr.append(f"{col} {dr}")
        return arr

    def get_limit(self):
        val=self.limit_spin.value()
        return val if val>0 else None

    def get_offset(self):
        val=self.offset_spin.value()
        return val if val>0 else None

###############################################################################
# 11) SQLImportTab
###############################################################################
class SQLImportTab(QWidget):
    """Allows user to paste SQL and run a parse check."""
    def __init__(self, builder=None, parent=None):
        super().__init__(parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        instruct=QLabel("Paste or type your SQL below, then click 'Import'. We'll parse it.")
        layout.addWidget(instruct)

        self.sql_edit=QTextEdit()
        layout.addWidget(self.sql_edit)

        import_btn=QPushButton("Import SQL")
        import_btn.clicked.connect(self.on_import)
        layout.addWidget(import_btn)

        self.setLayout(layout)

    def on_import(self):
        raw=self.sql_edit.toPlainText().strip()
        if not raw:
            QMessageBox.information(self,"Empty SQL","No SQL to parse.")
            return
        try:
            parser=FullSQLParser(raw)
            parser.parse()
            QMessageBox.information(self,"Import OK","SQL parse succeeded.")
        except Exception as e:
            QMessageBox.warning(self,"Import Error",f"Parsing error: {e}")

###############################################################################
# 12) EnhancedCanvas
###############################################################################
class EnhancedCanvasGraphicsView(QGraphicsView):
    """Main canvas for table items, subqueries, join lines, DML mapping lines, etc."""
    def __init__(self,builder,parent=None):
        super().__init__(parent)
        self.builder=builder
        self.scene_=QGraphicsScene(self)
        self.setScene(self.scene_)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QGraphicsView.RubberBandDrag)

        self.table_items={}   # e.g. { "db.tbl": QGraphicsRectItem or NestedSubqueryItem }
        self.join_lines=[]
        self.mapping_lines=[]

        # For DML partition line + placeholders
        self.operation_red_line=None
        self.complete_query_item=None
        self.target_table_item=None

        self.zoom_factor=1.25
        self.min_scale=0.1
        self.max_scale=8.0

        self.validation_timer=QTimer()
        self.validation_timer.setInterval(800)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.builder.validate_sql)

    def wheelEvent(self,event):
        if event.angleDelta().y()>0:
            z=self.zoom_factor
        else:
            z=1/self.zoom_factor
        c=self.transform().m11()
        n=c*z
        if self.min_scale<n<self.max_scale:
            self.scale(z,z)

    def dragEnterEvent(self,e):
        if e.mimeData().hasText():
            e.acceptProposedAction()

    def dragMoveEvent(self,e):
        e.acceptProposedAction()

    def dropEvent(self,e):
        txt=e.mimeData().text()
        pos=self.mapToScene(e.pos())
        self.builder.handle_drop(txt,pos)
        e.acceptProposedAction()

    def add_table_item(self, table_name, columns, x, y):
        rect=QGraphicsRectItem(0,0,200,60)
        rect.setBrush(QBrush(QColor(220,220,255)))
        rect.setPen(QPen(Qt.darkGray,2))
        rect.setPos(x,y)
        rect.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
        label=QGraphicsTextItem(f"{table_name}\n({len(columns)} cols)", rect)
        label.setPos(5,5)
        self.scene_.addItem(rect)
        self.table_items[table_name]=rect
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def add_subquery_item(self, x, y):
        sq=NestedSubqueryItem(x,y)
        self.scene_.addItem(sq)
        key=f"SubQueryItem_{id(sq)}"
        self.table_items[key]=sq
        self.validation_timer.start()

    def add_vertical_red_line(self,x=450):
        if self.operation_red_line:
            self.scene_.removeItem(self.operation_red_line)
            self.operation_red_line=None
        ln=QGraphicsLineItem(x,0,x,3000)
        ln.setPen(QPen(Qt.red,2,Qt.DashDotLine))
        ln.setZValue(-10)
        self.scene_.addItem(ln)
        self.operation_red_line=ln

    def create_mapping_line(self, source_text_item, target_text_item,
                            src_type=None, tgt_type=None):
        ml=MappingLine(source_text_item, target_text_item, src_type, tgt_type)
        self.scene_.addItem(ml)
        self.mapping_lines.append(ml)
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def remove_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene_.removeItem(ml)
        self.mapping_lines.clear()

    def mouseReleaseEvent(self,event):
        super().mouseReleaseEvent(event)
        for jl in self.join_lines:
            jl.update_line()

###############################################################################
# 13) SubVQBDialog (reuse for combining queries or expression builder subquery)
###############################################################################
class SubVQBDialog(QDialog):
    """
    A second full VQB as a subdialog, with an operator (UNION, etc.).
    """
    def __init__(self, parent_vqb=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Build Second Query (Full VQB)")
        self.resize(900,600)
        self.operator="UNION"
        self.second_sql=""
        self.parent_vqb=parent_vqb

        layout=QVBoxLayout(self)

        op_layout=QHBoxLayout()
        op_layout.addWidget(QLabel("Combine Operator:"))
        self.op_combo=QComboBox()
        self.op_combo.addItems(["UNION","UNION ALL","INTERSECT","EXCEPT"])
        op_layout.addWidget(self.op_combo)
        op_layout.addStretch()
        layout.addLayout(op_layout)

        # Use a second VisualQueryBuilderTab
        from_tab=VisualQueryBuilderTab()
        self.sub_vqb=from_tab
        layout.addWidget(from_tab)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        self.setLayout(layout)

    def on_ok(self):
        self.operator=self.op_combo.currentText()
        self.second_sql=self.sub_vqb.sql_display.toPlainText().strip()
        if not self.second_sql:
            QMessageBox.warning(self,"No SQL","Second query is empty.")
            return
        self.accept()

    def getResult(self):
        return (self.operator,self.second_sql)

###############################################################################
# 14) The main VisualQueryBuilderTab
###############################################################################
class VisualQueryBuilderTab(QWidget):
    """Manages the schema/canvas, filter, group, sort, plus final SQL generation."""
    def __init__(self,parent=None):
        super().__init__(parent)
        self.connections={}
        self.table_columns_map={}
        self.fk_map={}
        self.auto_generate=True
        self.operation_mode="SELECT"
        self.threadpool=QThreadPool.globalInstance()

        self.init_ui()

    def init_ui(self):
        main=QVBoxLayout(self)

        # Connection row
        conn_h=QHBoxLayout()
        self.status_light=QFrame()
        self.status_light.setFixedSize(15,15)
        self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red;}")
        self.server_label=QLabel("Not Connected")
        conn_btn=QPushButton("Connect")
        conn_btn.clicked.connect(self.open_connect_dialog)
        conn_h.addWidget(self.status_light)
        conn_h.addWidget(self.server_label)
        conn_h.addWidget(conn_btn)

        # auto-generate
        self.auto_gen_chk=QCheckBox("Auto-Generate")
        self.auto_gen_chk.setChecked(True)
        self.auto_gen_chk.stateChanged.connect(self.on_auto_gen_changed)
        conn_h.addWidget(self.auto_gen_chk)

        conn_h.addStretch()
        main.addLayout(conn_h)

        # Toolbar
        tb_h=QHBoxLayout()
        ref_btn=QPushButton("Refresh Schema")
        ref_btn.clicked.connect(self.refresh_schema)
        tb_h.addWidget(ref_btn)

        subq_btn=QPushButton("Add SubQuery to Canvas")
        subq_btn.clicked.connect(self.add_subquery_to_canvas)
        tb_h.addWidget(subq_btn)

        expr_btn=QPushButton("Expression Builder")
        expr_btn.clicked.connect(self.launch_expr_builder)
        tb_h.addWidget(expr_btn)

        win_btn=QPushButton("Window Function Wizard")
        win_btn.clicked.connect(self.launch_window_func)
        tb_h.addWidget(win_btn)

        sub_vqb_btn=QPushButton("Combine Query (Full Sub VQB)")
        sub_vqb_btn.clicked.connect(self.combine_with_subvqb)
        tb_h.addWidget(sub_vqb_btn)

        self.op_combo=QComboBox()
        self.op_combo.addItems(["SELECT","INSERT","UPDATE","DELETE"])
        self.op_combo.currentIndexChanged.connect(self.on_op_mode_changed)
        tb_h.addWidget(self.op_combo)

        tb_h.addStretch()
        main.addLayout(tb_h)

        self.tabs=QTabWidget()
        main.addWidget(self.tabs)

        self.schema_tab=QWidget()
        self.tabs.addTab(self.schema_tab,"Schema & Canvas")

        self.config_tab=QWidget()
        self.tabs.addTab(self.config_tab,"Query Config")

        self.sql_tab=QWidget()
        self.tabs.addTab(self.sql_tab,"SQL Preview")

        self.import_tab=SQLImportTab(builder=self)
        self.tabs.addTab(self.import_tab,"SQL Import")

        self.status_bar=QStatusBar()
        main.addWidget(self.status_bar)
        self.setLayout(main)

        self.setup_schema_tab()
        self.setup_config_tab()
        self.setup_sql_tab()

    def setup_schema_tab(self):
        lay=QVBoxLayout(self.schema_tab)
        self.search_edit=QLineEdit()
        self.search_edit.setPlaceholderText("Search tables/columns...")
        self.search_edit.textChanged.connect(self.on_schema_filter)
        lay.addWidget(self.search_edit)

        splitter=QSplitter(Qt.Horizontal)
        self.schema_tree=LazySchemaTreeWidget(None,parent_builder=self)
        self.schema_tree.itemChanged.connect(self.on_item_changed)

        leftp=QWidget()
        lp=QVBoxLayout(leftp)
        lp.addWidget(self.schema_tree)
        splitter.addWidget(leftp)

        self.canvas=EnhancedCanvasGraphicsView(self)
        splitter.addWidget(self.canvas)
        splitter.setStretchFactor(0,1)
        splitter.setStretchFactor(1,3)
        lay.addWidget(splitter)

        self.progress=QProgressBar()
        self.progress.setVisible(False)
        lay.addWidget(self.progress)

    def setup_config_tab(self):
        h=QHBoxLayout(self.config_tab)
        self.filter_panel=FilterPanel(self)
        h.addWidget(self.filter_panel,2)
        self.group_by_panel=GroupByPanel(self)
        h.addWidget(self.group_by_panel,3)
        self.sort_limit_panel=SortLimitPanel(self)
        h.addWidget(self.sort_limit_panel,2)
        self.config_tab.setLayout(h)

    def setup_sql_tab(self):
        lay=QVBoxLayout(self.sql_tab)
        top=QHBoxLayout()
        top.addWidget(QLabel("Generated SQL:"))
        run_btn=QPushButton("Run SQL (Stub)")
        run_btn.clicked.connect(self.run_sql_query)
        top.addWidget(run_btn,alignment=Qt.AlignRight)
        lay.addLayout(top)

        self.sql_display=QTextEdit()
        self.sql_display.setReadOnly(False)  # allow manual edits, too
        self.sql_highlighter=SQLHighlighter(self.sql_display.document())
        lay.addWidget(self.sql_display)

        self.validation_lbl=QLabel("SQL Status: Unknown")
        lay.addWidget(self.validation_lbl)

        self.sql_tab.setLayout(lay)

    def open_connect_dialog(self):
        dlg=ODBCConnectDialog(self)
        if dlg.exec_()==QDialog.Accepted:
            c=dlg.get_connection()
            db_type=dlg.get_db_type()
            if c and db_type and db_type.upper()=="TERADATA":
                alias=f"{db_type}_{len(self.connections)+1}"
                self.connections[alias]={"connection":c}
                self.update_connection_status(True,f"{db_type} ({alias})")
                self.load_schema(alias)
                self.fk_map=load_foreign_keys(c)
            else:
                QMessageBox.warning(self,"Only Teradata","DSN restricted to Teradata")

    def load_schema(self,alias):
        if alias not in self.connections:
            return
        conn=self.connections[alias]["connection"]
        self.schema_tree.connection=conn
        self.schema_tree.populate_top_level()
        self.status_bar.showMessage(f"Schema loaded => {alias}",3000)

    def refresh_schema(self):
        if not self.connections:
            QMessageBox.information(self,"No Connection","Please connect first.")
            return
        first_key=list(self.connections.keys())[0]
        self.load_schema(first_key)

    def update_connection_status(self, st, info=""):
        if st:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green;}")
            self.server_label.setText(info)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red;}")
            self.server_label.setText("Not Connected")

    def run_sql_query(self):
        sql=self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self,"Empty SQL","No SQL to run.")
            return
        QMessageBox.information(self,"SQL Execution",f"Executing:\n\n{sql}")

    def on_schema_filter(self,text):
        for i in range(self.schema_tree.topLevelItemCount()):
            it=self.schema_tree.topLevelItem(i)
            self._filter_tree_item(it,text)

    def _filter_tree_item(self, it, txt):
        low=txt.lower()
        match=low in it.text(0).lower()
        child_match=False
        for i in range(it.childCount()):
            child_match=self._filter_tree_item(it.child(i),txt) or child_match
        it.setHidden(not (match or child_match))
        return (match or child_match)

    def on_item_changed(self, it,col):
        if it.childCount()>0:
            st=it.checkState(0)
            for i in range(it.childCount()):
                it.child(i).setCheckState(0,st)
        else:
            p=it.parent()
            if p:
                c=sum(p.child(i).checkState(0)==Qt.Checked for i in range(p.childCount()))
                if c==p.childCount():
                    p.setCheckState(0,Qt.Checked)
                elif c>0:
                    p.setCheckState(0,Qt.PartiallyChecked)
                else:
                    p.setCheckState(0,Qt.Unchecked)
        if self.auto_generate:
            self.generate_sql()

    def on_auto_gen_changed(self, st):
        self.auto_generate=(st==Qt.Checked)

    def on_op_mode_changed(self, idx):
        modes=["SELECT","INSERT","UPDATE","DELETE"]
        self.operation_mode=modes[idx]
        # Toggle the DML placeholders on the canvas if needed
        self.toggle_dml_canvas()
        if self.auto_generate:
            self.generate_sql()

    def add_subquery_to_canvas(self):
        self.canvas.add_subquery_item(200,200)
        if self.auto_generate:
            self.generate_sql()

    def combine_with_subvqb(self):
        """Combine query button that opens SubVQBDialog to build second query."""
        subdlg=SubVQBDialog(parent_vqb=self, parent=self)
        if subdlg.exec_()==QDialog.Accepted:
            op, second_sql=subdlg.getResult()
            old=self.sql_display.toPlainText().strip()
            if old:
                new_sql=old+f"\n{op}\n(\n{second_sql}\n)"
            else:
                new_sql=f"{op}\n(\n{second_sql}\n)"
            self.sql_display.setPlainText(new_sql)
            self.validate_sql()

    def launch_expr_builder(self):
        cols=self.get_all_possible_columns_for_dialog()
        dlg=AdvancedExpressionBuilderDialog(cols,self)
        if dlg.exec_()==QDialog.Accepted:
            alias,expr=dlg.get_expression_data()
            old=self.sql_display.toPlainText()
            self.sql_display.setPlainText(old+f"\n-- Derived: {alias}={expr}")
            self.validate_sql()

    def launch_window_func(self):
        cols=self.get_all_possible_columns_for_dialog()
        dlg=WindowFunctionDialog(cols,self)
        if dlg.exec_()==QDialog.Accepted:
            wexpr=dlg.get_expression()
            old=self.sql_display.toPlainText()
            self.sql_display.setPlainText(old+f"\n-- Window Function: {wexpr}")
            self.validate_sql()

    def handle_drop(self, full_name, pos):
        """When a user drags a table from schema to canvas."""
        if full_name not in self.table_columns_map:
            self.table_columns_map[full_name]=["id","col1","col2"]
        cols=self.table_columns_map[full_name]
        self.canvas.add_table_item(full_name,cols,pos.x(),pos.y())
        self.check_auto_fk(full_name)

    def check_auto_fk(self, table_key):
        """If any child->parent references exist, auto-join to existing tables on the canvas."""
        if not self.fk_map:
            return
        item=self.canvas.table_items.get(table_key,None)
        if not item:
            return

        col_list=self.table_columns_map[table_key]
        for c in col_list:
            child_key=f"{table_key}.{c}"
            if child_key in self.fk_map:
                pk=self.fk_map[child_key]
                parent_tab=".".join(pk.split(".")[:2])
                pitem=self.canvas.table_items.get(parent_tab,None)
                if pitem:
                    jl=JoinLine(item,pitem,"LEFT",f"{child_key}={pk}")
                    self.canvas.scene_.addItem(jl)
                    self.canvas.join_lines.append(jl)
                    jl.update_line()

        # Also check if table_key is parent
        for ck,pk in self.fk_map.items():
            if pk.startswith(table_key+"."):
                child_tab=".".join(ck.split(".")[:2])
                citm=self.canvas.table_items.get(child_tab,None)
                if citm:
                    jl=JoinLine(citm,item,"LEFT",f"{ck}={pk}")
                    self.canvas.scene_.addItem(jl)
                    self.canvas.join_lines.append(jl)
                    jl.update_line()

    def get_selected_columns(self):
        """Return a list of db.table.col that are checked."""
        arr=[]
        for i in range(self.schema_tree.topLevelItemCount()):
            conn_it=self.schema_tree.topLevelItem(i)
            for j in range(conn_it.childCount()):
                db_it=conn_it.child(j)
                if db_it.data(0,Qt.UserRole)=="database":
                    dbn=db_it.text(0)
                    for k in range(db_it.childCount()):
                        tbl_it=db_it.child(k)
                        if tbl_it.data(0,Qt.UserRole)=="table":
                            tn=tbl_it.text(0)
                            for l in range(tbl_it.childCount()):
                                col_it=tbl_it.child(l)
                                if (col_it.data(0,Qt.UserRole)=="column" and
                                    col_it.checkState(0)==Qt.Checked):
                                    arr.append(f"{dbn}.{tn}.{col_it.text(0)}")
        return arr

    def get_all_possible_columns_for_dialog(self):
        return self.get_selected_columns()

    def toggle_dml_canvas(self):
        """If user picks INSERT/UPDATE/DELETE, show a red partition line & placeholders."""
        if self.operation_mode=="SELECT":
            # remove placeholders
            if self.canvas.operation_red_line:
                self.canvas.scene_.removeItem(self.canvas.operation_red_line)
                self.canvas.operation_red_line=None
            if self.canvas.complete_query_item:
                self.canvas.scene_.removeItem(self.canvas.complete_query_item)
                self.canvas.complete_query_item=None
            if self.canvas.target_table_item:
                self.canvas.scene_.removeItem(self.canvas.target_table_item)
                self.canvas.target_table_item=None
            self.canvas.remove_mapping_lines()
            return

        # Otherwise, add a vertical red line + placeholders
        self.canvas.add_vertical_red_line(450)

        # Create "complete query item" on the left if missing
        if not self.canvas.complete_query_item:
            rect=QGraphicsRectItem(0,0,200,80)
            rect.setBrush(QBrush(QColor(250,250,180)))
            rect.setPen(QPen(Qt.red,2))
            rect.setPos(100,200)
            rect.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
            colNames=["srcCol1","srcCol2","srcKey"]
            yOff=5
            for c in colNames:
                t=QGraphicsTextItem(c, rect)
                t.setPos(5,yOff)
                yOff+=15
            self.canvas.scene_.addItem(rect)
            self.canvas.complete_query_item=rect

        # Create "target table item" on the right if missing
        if not self.canvas.target_table_item:
            rect2=QGraphicsRectItem(0,0,200,100)
            rect2.setBrush(QBrush(QColor(220,220,255)))
            rect2.setPen(QPen(Qt.darkGray,2))
            rect2.setPos(500,200)
            rect2.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
            t=QGraphicsTextItem("Target: myDB.myTarget", rect2)
            t.setPos(5,5)
            targCols=["colA","colB","key"]
            y2=25
            for c in targCols:
                tx=QGraphicsTextItem(c, rect2)
                tx.setPos(5,y2)
                y2+=15
            self.canvas.scene_.addItem(rect2)
            self.canvas.target_table_item=rect2

    def generate_sql(self):
        """Gather columns, BFS join logic, filters, group-by, etc., and build final SQL."""
        if not self.auto_generate:
            return

        if self.operation_mode=="INSERT":
            sql=self._generate_insert()
        elif self.operation_mode=="UPDATE":
            sql=self._generate_update()
        elif self.operation_mode=="DELETE":
            sql=self._generate_delete()
        else:
            sql=self._generate_select()

        self.sql_display.setPlainText(sql)
        self.validate_sql()

    def validate_sql(self):
        txt=self.sql_display.toPlainText().strip()
        if not txt:
            self.validation_lbl.setText("SQL Status: No SQL.")
            self.validation_lbl.setStyleSheet("color: orange;")
            return
        try:
            p=FullSQLParser(txt)
            p.parse()
            self.validation_lbl.setText("SQL Status: Valid.")
            self.validation_lbl.setStyleSheet("color: green;")
        except Exception as ex:
            self.validation_lbl.setText(f"SQL Status: Invalid - {ex}")
            self.validation_lbl.setStyleSheet("color: red;")

    def _build_bfs_from(self):
        """BFS subgraph approach: handle multiple join lines, building FROM + conditions."""
        invert={v:k for k,v in self.canvas.table_items.items()}
        adj={}
        for k in self.canvas.table_items.keys():
            adj[k]=[]
        for jl in self.canvas.join_lines:
            s=invert.get(jl.start_item,None)
            e=invert.get(jl.end_item,None)
            if s and e:
                adj[s].append((e,jl))
                adj[e].append((s,jl))

        visited=set()
        blocks=[]
        for root in adj:
            if root not in visited:
                queue=[root]
                visited.add(root)
                seg=[root]
                while queue:
                    node=queue.pop(0)
                    for (nbr,ln) in adj[node]:
                        if nbr not in visited:
                            visited.add(nbr)
                            queue.append(nbr)
                            seg.append(f"{ln.join_type} {nbr} ON {ln.condition}")
                block="\n  ".join(seg)
                if not blocks:
                    blocks.append("FROM "+block)
                else:
                    # if disconnected subgraph => multiple FROM segments
                    blocks.append("-- Another subgraph:\nFROM "+block)
        return "\n".join(blocks) if blocks else "-- no tables on canvas"

    def _generate_select(self):
        scols=self.get_selected_columns()
        if not scols:
            scols=["*"]
        # group-by logic
        ags=self.group_by_panel.get_aggregates()
        final_cols=list(scols)
        for (f,c,a) in ags:
            if f.upper()=="CUSTOM":
                final_cols.append(c)
            else:
                final_cols.append(f"{f}({c}) AS {a}")

        lines=[]
        lines.append("SELECT "+", ".join(final_cols))
        lines.append(self._build_bfs_from())

        # WHERE
        wfs=self.filter_panel.get_filters("WHERE")
        if wfs:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in wfs]
            lines.append("WHERE "+ " AND ".join(conds))

        gb=self.group_by_panel.get_group_by()
        if gb:
            lines.append("GROUP BY "+", ".join(gb))

        # HAVING
        hv=self.filter_panel.get_filters("HAVING")
        if hv:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in hv]
            lines.append("HAVING "+ " AND ".join(conds))

        # ORDER BY
        ob=self.sort_limit_panel.get_order_bys()
        if ob:
            lines.append("ORDER BY "+ ", ".join(ob))

        # LIMIT & OFFSET
        lm=self.sort_limit_panel.get_limit()
        if lm is not None:
            lines.append(f"LIMIT {lm}")
        off=self.sort_limit_panel.get_offset()
        if off is not None:
            lines.append(f"OFFSET {off}")

        return "\n".join(lines)

    def _parse_target_info(self):
        """Parse 'Target: db.tbl' from the target_table_item if present."""
        if not self.canvas.target_table_item:
            return (None, None)
        for ch in self.canvas.target_table_item.childItems():
            if isinstance(ch, QGraphicsTextItem):
                txt=ch.toPlainText().strip()
                if txt.startswith("Target:"):
                    raw=txt.replace("Target:","").strip()
                    if "." in raw:
                        parts=raw.split(".",1)
                        return (parts[0].strip(), parts[1].strip())
        return (None,None)

    def _parse_mapped_columns(self):
        """Return a list of (srcCol, tgtCol) from mapping lines."""
        arr=[]
        for ml in self.canvas.mapping_lines:
            arr.append((ml.source_col, ml.target_col))
        return arr

    def _generate_insert(self):
        dbName,tName=self._parse_target_info()
        if not dbName or not tName:
            return "-- No target => no INSERT"

        mapped=self._parse_mapped_columns()
        if not mapped:
            return "-- No column mapping => no INSERT"

        subSelect=self._generate_select_sql_only()

        target_cols=[]
        source_cols=[]
        for (src,tgt) in mapped:
            target_cols.append(tgt)
            source_cols.append(src)

        lines=[]
        lines.append(f"INSERT INTO {dbName}.{tName} ({', '.join(target_cols)})")
        lines.append(subSelect)
        return "\n".join(lines)

    def _generate_update(self):
        dbName,tName=self._parse_target_info()
        if not dbName or not tName:
            return "-- No target => no UPDATE"

        mapped=self._parse_mapped_columns()
        if not mapped:
            return "-- No column mapping => no UPDATE"

        subSelect=self._generate_select_sql_only()
        # assume last col is key
        key_col="key"
        sets=[]
        for (src,tgt) in mapped:
            if tgt.lower()!=key_col:
                sets.append(f"{tgt}=src.{src}")

        lines=[]
        lines.append(f"UPDATE {dbName}.{tName}")
        lines.append(f"SET {', '.join(sets)}")
        lines.append("FROM (")
        lines.append(subSelect)
        lines.append(") AS src")
        lines.append(f"WHERE {dbName}.{tName}.{key_col} = src.{key_col}")
        return "\n".join(lines)

    def _generate_delete(self):
        dbName,tName=self._parse_target_info()
        if not dbName or not tName:
            return "-- No target => no DELETE"

        subSelect=self._generate_select_sql_only()
        key_col="key"
        lines=[]
        lines.append(f"DELETE FROM {dbName}.{tName}")
        lines.append("WHERE "+f"{key_col} IN (")
        lines.append(subSelect)
        lines.append(")")
        return "\n".join(lines)

    def _generate_select_sql_only(self):
        """Generate a plain SELECT for subSelect usage in DML."""
        scols=self.get_selected_columns()
        if not scols:
            scols=["*"]
        lines=[]
        lines.append("SELECT "+", ".join(scols))
        lines.append(self._build_bfs_from())
        wfs=self.filter_panel.get_filters("WHERE")
        if wfs:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in wfs]
            lines.append("WHERE "+ " AND ".join(conds))
        # Not applying group/having in the subSelect for brevity
        return "\n".join(lines)

###############################################################################
# 15) MainVQBWindow
###############################################################################
class MainVQBWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Enhanced Visual Query Builder - Single File")
        self.resize(1200,800)

        self.builder_tab=VisualQueryBuilderTab()
        self.setCentralWidget(self.builder_tab)

        self.init_toolbar()

    def init_toolbar(self):
        tb=self.addToolBar("Main Toolbar")
        fit_act=QAction("Fit to View",self)
        fit_act.triggered.connect(self.on_fit_view)
        tb.addAction(fit_act)

        layout_act=QAction("Auto-Layout (Stub)",self)
        layout_act.triggered.connect(self.on_auto_layout)
        tb.addAction(layout_act)

        # Example: create a quick mapping line
        map_act=QAction("Demo Map (srcCol1->colA)", self)
        map_act.triggered.connect(self.demo_map)
        tb.addAction(map_act)

    def on_fit_view(self):
        sc=self.builder_tab.canvas.scene_
        self.builder_tab.canvas.fitInView(sc.itemsBoundingRect(),Qt.KeepAspectRatio)

    def on_auto_layout(self):
        # Example: position tables in a grid
        items=list(self.builder_tab.canvas.table_items.values())
        col_count=3
        xsp=250
        ysp=180
        for i,itm in enumerate(items):
            row=i//col_count
            col=i%col_count
            itm.setPos(col*xsp,row*ysp)
        for jl in self.builder_tab.canvas.join_lines:
            jl.update_line()

    def demo_map(self):
        """Connect 'srcCol1' in complete_query_item to 'colA' in target_table_item."""
        cv=self.builder_tab.canvas
        if (not cv.complete_query_item) or (not cv.target_table_item):
            QMessageBox.information(self,"No DML placeholders","Switch to INSERT/UPDATE/DELETE mode first.")
            return

        left_txt=None
        for ch in cv.complete_query_item.childItems():
            if isinstance(ch, QGraphicsTextItem):
                if ch.toPlainText().strip().lower()=="srccol1":
                    left_txt=ch
                    break
        right_txt=None
        for ch in cv.target_table_item.childItems():
            if isinstance(ch, QGraphicsTextItem):
                if ch.toPlainText().strip().lower()=="cola":
                    right_txt=ch
                    break
        if not left_txt or not right_txt:
            QMessageBox.information(self,"Not Found","srcCol1 or colA not found in placeholders.")
            return
        cv.create_mapping_line(left_txt, right_txt)

###############################################################################
# main()
###############################################################################
def main():
    app=QApplication(sys.argv)
    apply_fusion_style()
    w=MainVQBWindow()
    w.show()
    sys.exit(app.exec_())

if __name__=="__main__":
    main()
