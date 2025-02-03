#!/usr/bin/env python3
# vqb_full.py
#
# Single-file PyQt5 Visual Query Builder ("full VQB") featuring:
#  - Multiple ODBC connections (alias = DSN name)
#  - BFS (canvas) with multi-join lines
#  - Mark any BFS item as DML Target for INSERT/UPDATE/DELETE mappings
#  - Filter/Group/Sort/CTE panels + Pivot Wizard + Window Function Wizard
#  - Token-based Advanced Expression Builder (CASE wizard, subquery insertion)
#  - Data Profiler (counts, distinct, min, max, avg, boxplot outliers)
#  - SQL Import with sqlglot => partial BFS rebuild
#  - Cross-DB rewriting via Linked Server config
#  - No dummy placeholders for BFS source/target
#
# Requirements:
#   pip install pyqt5 pyodbc sqlparse sqlglot matplotlib
#

import sys
import traceback
import logging
import pyodbc
import sqlparse
import sqlglot
from sqlglot import exp

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import (
    Qt, QTimer, QThreadPool, QRunnable, pyqtSignal, QObject,
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
    QListWidget, QCheckBox, QHeaderView
)

import matplotlib
matplotlib.use("Agg")  # Avoid needing a display environment
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure

###############################################################################
# Logging + Fusion Style
###############################################################################
logging.basicConfig(
    filename="vqb.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG
)
pyodbc.pooling = True

def apply_fusion_style():
    QApplication.setStyle("Fusion")
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(240,240,240))
    palette.setColor(QPalette.WindowText, Qt.black)
    palette.setColor(QPalette.Base, QColor(255,255,255))
    palette.setColor(QPalette.AlternateBase, QColor(225,225,225))
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
# ODBC + Connections
###############################################################################
class ODBCConnectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connect to ODBC")
        self.resize(400,230)
        self._conn = None
        self._db_type = None
        self._dsn_name= None

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Pick an ODBC DSN:"))
        self.dsn_combo = QComboBox()
        try:
            dsns = pyodbc.dataSources()
            for dsn in sorted(dsns.keys()):
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
        lay.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(lay)

    def on_ok(self):
        dsn = self.dsn_combo.currentText().strip()
        if not dsn:
            QMessageBox.warning(self, "No DSN", "Must pick a DSN.")
            return
        user = self.user_edit.text().strip()
        pwd  = self.pass_edit.text().strip()

        conn_str = f"DSN={dsn};"
        if user:
            conn_str += f"UID={user};"
        if pwd:
            conn_str += f"PWD={pwd};"

        try:
            cn = pyodbc.connect(conn_str, autocommit=True)
            self._conn = cn
            self._dsn_name = dsn
            try:
                dbms = cn.getinfo(pyodbc.SQL_DBMS_NAME) or ""
                if "TERADATA" in dbms.upper():
                    self._db_type = "Teradata"
                elif "SQL SERVER" in dbms.upper():
                    self._db_type = "SQLServer"
                else:
                    self._db_type = dbms.strip()
            except:
                self._db_type = "Unknown"
            self.accept()
        except Exception as ex:
            QMessageBox.critical(self,"Connect Error",f"Failed:\n{ex}")

    def get_connection(self):
        return self._conn

    def get_db_type(self):
        return self._db_type

    def get_dsn_name(self):
        return self._dsn_name

class MultiODBCConnectDialog(QDialog):
    """
    We store connections in { alias: { "connection":..., "db_type":... }, ... }
    alias = DSN or DSN_2, DSN_3 if duplicates
    """
    def __init__(self, existing_conns=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage DB Connections")
        self.resize(500,300)
        self._connections = existing_conns if existing_conns else {}

        layout = QVBoxLayout(self)
        instruct = QLabel("Add or Remove ODBC connections.\nAlias = DSN name.")
        layout.addWidget(instruct)

        self.conn_table = QTableWidget(0,3)
        self.conn_table.setHorizontalHeaderLabels(["Alias(DSN)","DB Type","Status"])
        self.conn_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.conn_table)

        for alias, info in self._connections.items():
            r = self.conn_table.rowCount()
            self.conn_table.insertRow(r)
            self.conn_table.setItem(r,0,QTableWidgetItem(alias))
            self.conn_table.setItem(r,1,QTableWidgetItem(info.get("db_type","Unknown")))
            st="OK" if info.get("connection") else "NoConn"
            self.conn_table.setItem(r,2,QTableWidgetItem(st))

        btn_h = QHBoxLayout()
        add_b = QPushButton("Add Connection")
        rm_b  = QPushButton("Remove Connection")
        cls_b = QPushButton("Close")
        btn_h.addWidget(add_b)
        btn_h.addWidget(rm_b)
        btn_h.addStretch()
        btn_h.addWidget(cls_b)
        layout.addLayout(btn_h)

        add_b.clicked.connect(self.on_add)
        rm_b.clicked.connect(self.on_rm)
        cls_b.clicked.connect(self.accept)
        self.setLayout(layout)

    def on_add(self):
        d = ODBCConnectDialog(self)
        if d.exec_() == QDialog.Accepted:
            c   = d.get_connection()
            dbt = d.get_db_type()
            dsn = d.get_dsn_name()
            if c and dsn:
                alias = dsn
                count_ = 2
                while alias in self._connections:
                    alias = f"{dsn}_{count_}"
                    count_ += 1
                self._connections[alias] = {"connection":c,"db_type":dbt}
                r=self.conn_table.rowCount()
                self.conn_table.insertRow(r)
                self.conn_table.setItem(r,0,QTableWidgetItem(alias))
                self.conn_table.setItem(r,1,QTableWidgetItem(dbt))
                self.conn_table.setItem(r,2,QTableWidgetItem("OK"))

    def on_rm(self):
        rows=self.conn_table.selectionModel().selectedRows()
        if not rows:
            return
        for rr in sorted([x.row() for x in rows],reverse=True):
            alias_item=self.conn_table.item(rr,0)
            if alias_item:
                alias=alias_item.text()
                if alias in self._connections:
                    del self._connections[alias]
            self.conn_table.removeRow(rr)

    def get_connections(self):
        return self._connections

###############################################################################
# LinkedServer => cross DB rewriting
###############################################################################
class LinkedServerConfigDialog(QDialog):
    def __init__(self, existing_map=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Linked Server / Federation Config")
        self.resize(500,300)
        self._map = existing_map.copy() if existing_map else {}

        ly= QVBoxLayout(self)
        instr= QLabel("Map each DSN alias to a 'linked server' name for cross-DB.\n"
                      "'MyDSN.myDb.myTable' => '[LinkedSrv].[myDb].dbo.[myTable]'.")
        ly.addWidget(instr)

        self.tbl= QTableWidget(0,2)
        self.tbl.setHorizontalHeaderLabels(["Alias(DSN)","LinkedServerName"])
        self.tbl.horizontalHeader().setStretchLastSection(True)
        ly.addWidget(self.tbl)

        for alias,lsn in self._map.items():
            r=self.tbl.rowCount()
            self.tbl.insertRow(r)
            self.tbl.setItem(r,0,QTableWidgetItem(alias))
            self.tbl.setItem(r,1,QTableWidgetItem(lsn))

        bh= QHBoxLayout()
        addb= QPushButton("Add")
        rmb= QPushButton("Remove")
        cls= QPushButton("Close")
        bh.addWidget(addb)
        bh.addWidget(rmb)
        bh.addStretch()
        bh.addWidget(cls)
        ly.addLayout(bh)

        addb.clicked.connect(self.on_add)
        rmb.clicked.connect(self.on_rm)
        cls.clicked.connect(self.accept)
        self.setLayout(ly)

    def on_add(self):
        r=self.tbl.rowCount()
        self.tbl.insertRow(r)
        self.tbl.setItem(r,0,QTableWidgetItem("MyDSN"))
        self.tbl.setItem(r,1,QTableWidgetItem("LinkedSrvName"))

    def on_rm(self):
        rows=self.tbl.selectionModel().selectedRows()
        if not rows: return
        for rr in sorted([r.row() for r in rows],reverse=True):
            self.tbl.removeRow(rr)

    def accept(self):
        newmap={}
        for r in range(self.tbl.rowCount()):
            aitem=self.tbl.item(r,0)
            litem=self.tbl.item(r,1)
            if aitem and litem:
                alias=aitem.text().strip()
                lsn=litem.text().strip()
                if alias:
                    newmap[alias]=lsn
        self._map=newmap
        super().accept()

    def get_map(self):
        return self._map

###############################################################################
# load_tables/load_columns => multi DB
###############################################################################
def load_tables(connection, db_type, db_name):
    out=[]
    if not connection:
        return out
    try:
        cur= connection.cursor()
        if "TERADATA" in db_type.upper():
            q=f"SELECT TableName FROM DBC.TablesV WHERE DatabaseName='{db_name}' AND TableKind='T' ORDER BY TableName"
            cur.execute(q)
            rows= cur.fetchall()
            out=[row[0].strip() for row in rows]
        elif "SQLSERVER" in db_type.upper():
            q= f"SELECT TABLE_NAME FROM {db_name}.INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME"
            cur.execute(q)
            rows=cur.fetchall()
            out=[row[0].strip() for row in rows]
        else:
            pass
    except Exception as ex:
        logging.warning(f"Failed to load tables for {db_name}: {ex}")
    return out

def load_columns(connection, db_type, db_name, tbl_name):
    out=[]
    if not connection:
        return out
    try:
        cur= connection.cursor()
        if "TERADATA" in db_type.upper():
            q=f"SELECT ColumnName FROM DBC.ColumnsV WHERE DatabaseName='{db_name}' AND TableName='{tbl_name}' ORDER BY ColumnId"
            cur.execute(q)
            rows=cur.fetchall()
            out=[row[0].strip() for row in rows]
        elif "SQLSERVER" in db_type.upper():
            q=f"SELECT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{tbl_name}' ORDER BY ORDINAL_POSITION"
            cur.execute(q)
            rows=cur.fetchall()
            out=[row[0].strip() for row in rows]
        else:
            pass
    except Exception as ex:
        logging.warning(f"Failed to load columns for {db_name}.{tbl_name}: {ex}")
    return out

###############################################################################
# Lazy schema tree
###############################################################################
class SchemaLoaderSignals(QtCore.QObject):
    finished=pyqtSignal(str,list)
    error=pyqtSignal(str,str)

class SchemaLoader(QtCore.QRunnable):
    def __init__(self, connection, db_type, db_name):
        super().__init__()
        self.connection= connection
        self.db_type= db_type
        self.db_name= db_name
        self.signals= SchemaLoaderSignals()

    @QtCore.pyqtSlot()
    def run(self):
        try:
            tabs= load_tables(self.connection, self.db_type, self.db_name)
            self.signals.finished.emit(self.db_name, tabs)
        except Exception as ex:
            err = f"{ex}\n{traceback.format_exc()}"
            self.signals.error.emit(self.db_name, err)

class MultiDBLazySchemaTreeWidget(QTreeWidget):
    """
    DSN alias => databases => tables => columns
    """
    def __init__(self, connections, parent_builder=None, parent=None):
        super().__init__(parent)
        self.connections=connections
        self.parent_builder= parent_builder
        self.setHeaderHidden(False)
        self.setColumnCount(1)
        self.setHeaderLabel("Databases / Tables")
        self.setDragEnabled(True)
        self.threadpool= QThreadPool.globalInstance()
        self.populate_roots()

    def populate_roots(self):
        self.clear()
        if not self.connections:
            self.addTopLevelItem(QTreeWidgetItem(["No Connections"]))
            return
        for alias,info in self.connections.items():
            top= QTreeWidgetItem([f"{alias} ({info.get('db_type','Unknown')})"])
            top.setData(0, Qt.UserRole, ("connAlias", alias))
            self.addTopLevelItem(top)
            conn=info.get("connection")
            dbt=info.get("db_type","")
            if not conn:
                top.addChild(QTreeWidgetItem(["(No connection)"]))
                continue
            try:
                c=conn.cursor()
                if "TERADATA" in dbt.upper():
                    c.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
                    rows=c.fetchall()
                    for row in rows:
                        dbn=row[0].strip()
                        dbi= QTreeWidgetItem([dbn])
                        dbi.setData(0, Qt.UserRole, ("db",alias,dbn))
                        dbi.setData(0, Qt.UserRole+1,False)
                        dbi.addChild(QTreeWidgetItem(["Loading..."]))
                        top.addChild(dbi)
                elif "SQLSERVER" in dbt.upper():
                    c.execute("SELECT name FROM sys.databases ORDER BY name")
                    rows=c.fetchall()
                    for row in rows:
                        dbn=row[0].strip()
                        dbi=QTreeWidgetItem([dbn])
                        dbi.setData(0, Qt.UserRole, ("db",alias,dbn))
                        dbi.setData(0, Qt.UserRole+1,False)
                        dbi.addChild(QTreeWidgetItem(["Loading..."]))
                        top.addChild(dbi)
                else:
                    top.addChild(QTreeWidgetItem(["(Unknown DB type)"]))
            except Exception as ex:
                top.addChild(QTreeWidgetItem([f"(Error: {ex})"]))
        self.expandAll()

    def mouseDoubleClickEvent(self,e):
        it= self.itemAt(e.pos())
        if it:
            d= it.data(0, Qt.UserRole)
            if d and d[0]=="db":
                loaded= it.data(0, Qt.UserRole+1)
                if not loaded:
                    it.takeChildren()
                    alias, dbn= d[1], d[2]
                    info= self.connections.get(alias)
                    if info and info.get("connection"):
                        c= info["connection"]
                        dbt= info["db_type"]
                        worker= SchemaLoader(c,dbt,dbn)
                        def fin(dbase, tables):
                            self.populate_tables(it, dbase, tables)
                        def err(dbase, msg):
                            QMessageBox.critical(self,"Schema Error",f"{dbase} => {msg}")
                        worker.signals.finished.connect(fin)
                        worker.signals.error.connect(err)
                        self.threadpool.start(worker)
        super().mouseDoubleClickEvent(e)

    def populate_tables(self, parent_item, dbname, tables):
        if not tables:
            parent_item.addChild(QTreeWidgetItem(["<No Tables>"]))
            parent_item.setData(0, Qt.UserRole+1, True)
            return
        parent_item.takeChildren()
        d= parent_item.data(0, Qt.UserRole)
        alias= d[1]
        for t in tables:
            t_item=QTreeWidgetItem([t])
            t_item.setData(0, Qt.UserRole, ("table",alias,dbname,t))
            t_item.setData(0, Qt.UserRole+1, False)
            t_item.addChild(QTreeWidgetItem(["Loading..."]))
            parent_item.addChild(t_item)
        parent_item.setData(0, Qt.UserRole+1, True)

    def expand_table(self, table_item):
        loaded= table_item.data(0, Qt.UserRole+1)
        if not loaded:
            table_item.takeChildren()
            d= table_item.data(0, Qt.UserRole)
            alias, dbn, tbn= d[1], d[2], d[3]
            info= self.connections.get(alias)
            if info:
                c=info["connection"]
                dbt= info["db_type"]
                cols= load_columns(c,dbt,dbn,tbn)
                if cols:
                    for cc in cols:
                        child=QTreeWidgetItem([cc])
                        child.setData(0, Qt.UserRole, ("column",alias,dbn,tbn,cc))
                        table_item.addChild(child)
                else:
                    table_item.addChild(QTreeWidgetItem(["<No columns>"]))
            table_item.setData(0, Qt.UserRole+1, True)

    def mousePressEvent(self,e):
        it=self.itemAt(e.pos())
        if it:
            d= it.data(0, Qt.UserRole)
            if d and d[0]=="table":
                self.expand_table(it)
        super().mousePressEvent(e)

    def startDrag(self,actions):
        it= self.currentItem()
        if it:
            d= it.data(0, Qt.UserRole)
            if d and d[0]=="table":
                alias,dbn,tbl= d[1], d[2], d[3]
                full_key= f"{alias}.{dbn}.{tbl}"
                drag= QtGui.QDrag(self)
                mime= QtCore.QMimeData()
                mime.setText(full_key)
                drag.setMimeData(mime)
                drag.exec_(actions)

###############################################################################
# Basic SQL Parser + Syntax Highlighter
###############################################################################
class FullSQLParser:
    def __init__(self, sql):
        self.sql=sql
    def parse(self):
        parsed=sqlparse.parse(self.sql)
        if not parsed:
            raise ValueError("No valid SQL found.")

class SQLHighlighter(QSyntaxHighlighter):
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
            "NTILE","LAG","LEAD","CASE","COALESCE","TRIM","FIRST_VALUE","LAST_VALUE",
            "WITH"
        ]
        for w in keywords:
            pat= QRegularExpression(r'\b'+w+r'\b', QRegularExpression.CaseInsensitiveOption)
            self.rules.append((pat, kwfmt))

        strfmt=QTextCharFormat()
        strfmt.setForeground(Qt.darkRed)
        self.rules.append((QRegularExpression(r"'[^']*'"),strfmt))
        self.rules.append((QRegularExpression(r'"[^"]*"'),strfmt))

        comfmt=QTextCharFormat()
        comfmt.setForeground(Qt.green)
        self.rules.append((QRegularExpression(r'--[^\n]*'), comfmt))
        self.rules.append((QRegularExpression(r'/\*.*\*/',QRegularExpression.DotMatchesEverythingOption), comfmt))

    def highlightBlock(self, text):
        for pat,fmt in self.rules:
            matches= pat.globalMatch(text)
            while matches.hasNext():
                m= matches.next()
                st= m.capturedStart()
                ln= m.capturedLength()
                self.setFormat(st,ln,fmt)
        self.setCurrentBlockState(0)

###############################################################################
# BFS Items (CollapsibleTable, BFS, lines, join wizard, etc.)
###############################################################################
class ColumnJoinWizardDialog(QDialog):
    def __init__(self, source_full, source_type, target_full, target_type, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Column Join Wizard")
        self.resize(400,200)
        self.source_col= source_full
        self.source_type= source_type
        self.target_col= target_full
        self.target_type= target_type
        self.join_type="INNER"
        self.condition= f"{self.source_col} = {self.target_col}"

        layout= QVBoxLayout(self)
        info_label= QLabel(f"Source: {self.source_col} (type={self.source_type})\n"
                           f"Target: {self.target_col} (type={self.target_type})")
        layout.addWidget(info_label)
        if self.source_type.lower() != self.target_type.lower():
            w= QLabel("<b>Warning:</b> Different data types (may require cast).")
            w.setStyleSheet("color:red;")
            layout.addWidget(w)

        form= QFormLayout()
        self.join_combo= QComboBox()
        self.join_combo.addItems(["INNER","LEFT","RIGHT","FULL"])
        form.addRow("Join Type:", self.join_combo)

        self.cond_edit= QLineEdit(self.condition)
        form.addRow("Condition:", self.cond_edit)
        layout.addLayout(form)

        btns= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        layout.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(layout)

    def on_ok(self):
        jt= self.join_combo.currentText()
        c= self.cond_edit.text().strip()
        if not c:
            QMessageBox.warning(self,"No condition","Condition is empty.")
            return
        self.join_type= jt
        self.condition= c
        self.accept()

    def get_join_data(self):
        return (self.join_type, self.condition)


class MappingLine(QGraphicsLineItem):
    """
    For DML mappings (source col => target col).
    """
    def __init__(self, source_text_item, target_text_item, canvas, src_type=None, tgt_type=None):
        super().__init__()
        self.canvas= canvas
        self.source_text_item= source_text_item
        self.target_text_item= target_text_item
        self.source_col= source_text_item.toPlainText()
        self.target_col= target_text_item.toPlainText()
        self.src_type= src_type
        self.tgt_type= tgt_type

        self.setPen(QPen(Qt.darkRed,2,Qt.SolidLine))
        self.setZValue(5)
        self.setFlags(QGraphicsItem.ItemIsSelectable|QGraphicsItem.ItemIsFocusable)
        self.setAcceptHoverEvents(True)
        self.update_pos()

    def update_pos(self):
        s= self.source_text_item.mapToScene(self.source_text_item.boundingRect().center())
        t= self.target_text_item.mapToScene(self.target_text_item.boundingRect().center())
        self.setLine(QtCore.QLineF(s,t))

    def paint(self, painter, option, widget):
        self.update_pos()
        super().paint(painter, option, widget)

    def contextMenuEvent(self,event):
        menu= QMenu()
        rm= menu.addAction("Remove Column Mapping")
        chosen= menu.exec_(event.screenPos())
        if chosen== rm:
            if self in self.canvas.mapping_lines:
                self.canvas.mapping_lines.remove(self)
            sc= self.scene()
            if sc:
                sc.removeItem(self)


class JoinLine(QGraphicsLineItem):
    """
    BFS join line => table => table
    """
    def __init__(self, start_item, end_item, jtype="INNER", condition="", start_col_item=None, end_col_item=None):
        super().__init__()
        self.start_item= start_item
        self.end_item= end_item
        self.join_type= jtype
        self.condition= condition
        self.start_col_item= start_col_item
        self.end_col_item= end_col_item
        self.setZValue(-1)
        self.setAcceptHoverEvents(True)

        self.pen_map= {
            "INNER": (Qt.darkBlue, Qt.SolidLine),
            "LEFT":  (Qt.darkGreen, Qt.SolidLine),
            "RIGHT": (Qt.magenta,  Qt.DotLine),
            "FULL":  (Qt.red,      Qt.DashLine),
        }
        self.label= QGraphicsTextItem(f"{self.join_type} JOIN", self)
        self.label.setDefaultTextColor(Qt.blue)
        self.update_line()

    def update_line(self):
        if self.start_col_item:
            sr= self.start_col_item.boundingRect()
            scn= self.start_col_item.mapToScene(sr.center())
        else:
            sr= self.start_item.boundingRect()
            scn= self.start_item.mapToScene(sr.center())

        if self.end_col_item:
            er= self.end_col_item.boundingRect()
            ecn= self.end_col_item.mapToScene(er.center())
        else:
            er= self.end_item.boundingRect()
            ecn= self.end_item.mapToScene(er.center())

        self.setLine(QtCore.QLineF(scn, ecn))
        mx= (scn.x()+ ecn.x())/2
        my= (scn.y()+ ecn.y())/2
        self.label.setPos(mx,my)
        color, style= self.pen_map.get(self.join_type,(Qt.gray, Qt.SolidLine))
        self.setPen(QPen(color,2,style))

    def hoverEnterEvent(self,e):
        p= self.pen()
        p.setColor(Qt.yellow)
        p.setWidth(3)
        self.setPen(p)
        super().hoverEnterEvent(e)

    def hoverLeaveEvent(self,e):
        self.update_line()
        super().hoverLeaveEvent(e)


class DraggableColumnTextItem(QGraphicsTextItem):
    def __init__(self, parent_table_item, col_name, col_type):
        super().__init__(col_name, parent_table_item)
        self.parent_table_item= parent_table_item
        self.col_name= col_name
        self.col_type= col_type
        self.setFlags(QGraphicsItem.ItemIsSelectable| QGraphicsItem.ItemIsFocusable)
        self.setAcceptDrops(True)

    def mousePressEvent(self, e):
        if e.button()== Qt.LeftButton:
            drag= QtGui.QDrag(e.widget())
            mime= QtCore.QMimeData()
            full_col= f"{self.parent_table_item.table_fullname}.{self.col_name}"
            mime.setText(f"{full_col}||{self.col_type}")
            drag.setMimeData(mime)
            drag.exec_(Qt.MoveAction)
        else:
            super().mousePressEvent(e)

    def dragEnterEvent(self, e):
        if e.mimeData().hasText() and "||" in e.mimeData().text():
            e.acceptProposedAction()
        else:
            e.ignore()

    def dragMoveEvent(self, e):
        e.acceptProposedAction()

    def dropEvent(self,e):
        txt= e.mimeData().text()
        if "||" not in txt:
            e.ignore()
            return
        source_full, source_type= txt.split("||",1)
        target_full= f"{self.parent_table_item.table_fullname}.{self.col_name}"
        target_type= self.col_type

        if source_full == target_full:
            QMessageBox.information(None,"Same Column","Cannot join a column to itself.")
            e.ignore()
            return

        # If the BFS item for both is the same, it's the same table => not valid for BFS multi-join
        # but might be valid for a self-join in advanced scenarios. We'll disallow by default:
        sf_tab= ".".join(source_full.split(".")[:3])
        tf_tab= ".".join(target_full.split(".")[:3])
        if sf_tab == tf_tab:
            # For DML? If this BFS item is the target item, we might do a mapping. We'll see below.
            pass

        # We'll figure out if this is a BFS table-to-table join or a DML mapping (source => target).
        builder= self.parent_table_item.parent_builder
        canvas= builder.canvas

        # Check if either BFS item is "marked as DML target":
        is_source_target= self.parent_table_item.is_dml_target
        # We'll also look up the BFS item on the other side:
        source_item= canvas.table_items.get(sf_tab)
        target_item= canvas.table_items.get(tf_tab)

        if (not source_item) or (not target_item):
            QMessageBox.warning(None,"Join Error","Could not find BFS items for source/target.")
            e.ignore()
            return

        # If one BFS item is DML target, we do a "mapping line."
        # Otherwise, we do a "join line."
        if source_item.is_dml_target and (not target_item.is_dml_target):
            # So the user dragged from a "non-target" onto the "target" BFS item
            # => DML mapping line
            self.create_mapping_line(source_full, source_type, target_full, target_type, source_item, target_item, e)
        elif target_item.is_dml_target and (not source_item.is_dml_target):
            # The user dragged from "non-target" BFS item onto a "target" BFS item
            self.create_mapping_line(source_full, source_type, target_full, target_type, source_item, target_item, e)
        else:
            # We'll do a BFS join line wizard
            self.create_join_line(source_full, source_type, target_full, target_type, source_item, target_item, e)

    def create_join_line(self, source_full, source_type, target_full, target_type, source_item, target_item, drop_event):
        dlg= ColumnJoinWizardDialog(source_full, source_type, target_full, target_type)
        if dlg.exec_()== QDialog.Accepted:
            jtype, cond= dlg.get_join_data()
            s_col_name= source_full.split(".")[-1]
            t_col_name= target_full.split(".")[-1]
            src_col_item= source_item.column_text_items.get(s_col_name)
            tgt_col_item= target_item.column_text_items.get(t_col_name)

            jl= JoinLine(source_item, target_item, jtype, cond, src_col_item, tgt_col_item)
            self.parent_table_item.parent_builder.canvas.scene_.addItem(jl)
            self.parent_table_item.parent_builder.canvas.join_lines.append(jl)
            jl.update_line()
            QMessageBox.information(None,"Join Created",f"{jtype} JOIN:\n{cond}")
            drop_event.acceptProposedAction()
        else:
            drop_event.ignore()

    def create_mapping_line(self, source_full, source_type, target_full, target_type, source_item, target_item, drop_event):
        # DML mapping line
        # We'll skip a wizard and just create the mapping line.
        s_col_name= source_full.split(".")[-1]
        t_col_name= target_full.split(".")[-1]

        src_txt= source_item.column_text_items.get(s_col_name)
        tgt_txt= target_item.column_text_items.get(t_col_name)

        ml= MappingLine(src_txt, tgt_txt, self.parent_table_item.parent_builder.canvas, source_type, target_type)
        self.parent_table_item.parent_builder.canvas.scene_.addItem(ml)
        self.parent_table_item.parent_builder.canvas.mapping_lines.append(ml)
        drop_event.acceptProposedAction()


class CollapsibleTableGraphicsItem(QGraphicsRectItem):
    """
    BFS item => real DB table => user can check columns for SELECT usage
    Also can mark as "is_dml_target" => used for DML mapping lines
    """
    def __init__(self, table_fullname, columns, parent_builder, x=0, y=0):
        super().__init__(0,0,220,40)
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(220,220,255)))
        self.setPen(QPen(Qt.darkGray,2))
        self.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
        self.table_fullname= table_fullname
        self.columns= columns
        self.parent_builder= parent_builder
        self.is_collapsed= True
        self.is_dml_target= False
        self.title_height= 20
        self.column_items=[]
        self.column_text_items={}

        # Close + toggle
        self.close_btn= QGraphicsTextItem("[X]", self)
        self.close_btn.setPos(190,2)
        self.close_btn.setDefaultTextColor(Qt.red)

        self.toggle_btn= QGraphicsTextItem("[+]", self)
        self.toggle_btn.setPos(170,2)
        self.toggle_btn.setDefaultTextColor(Qt.blue)

        f= QFont("Arial",9,QFont.Bold)
        self.title_text= QGraphicsTextItem(table_fullname, self)
        self.title_text.setFont(f)
        self.title_text.setPos(5,2)

        # We'll guess column types
        self.mock_types={}
        for c in columns:
            if c.lower().startswith("id") or c.lower().endswith("id"):
                self.mock_types[c] = "INT"
            else:
                self.mock_types[c] = "VARCHAR"

        yOff= self.title_height
        for c in columns:
            cRect= QGraphicsRectItem(5,yOff+4,10,10,self)
            cRect.setBrush(QBrush(Qt.white))
            cRect.setPen(QPen(Qt.black,1))
            cTxt= DraggableColumnTextItem(self, c, self.mock_types[c])
            cTxt.setPos(20,yOff)
            self.column_items.append([cRect, cTxt, False])
            self.column_text_items[c]= cTxt
            yOff+= 20

        self.update_layout()

    def update_layout(self):
        if self.is_collapsed:
            self.setRect(0,0,220,self.title_height)
            for (r,t,_) in self.column_items:
                r.setVisible(False)
                t.setVisible(False)
            self.toggle_btn.setPlainText("[+]")
        else:
            expanded = self.title_height + len(self.column_items)*20
            self.setRect(0,0,220,expanded)
            for (r,t,_) in self.column_items:
                r.setVisible(True)
                t.setVisible(True)
            self.toggle_btn.setPlainText("[-]")
        # reposition the close & toggle
        self.close_btn.setPos(190,2)
        self.toggle_btn.setPos(170,2)

        # If is_dml_target => highlight
        if self.is_dml_target:
            self.setBrush(QBrush(QColor(255,240,200)))
        else:
            self.setBrush(QBrush(QColor(220,220,255)))

    def mousePressEvent(self, ev):
        pos= ev.pos()
        cR= self.close_btn.mapToParent(self.close_btn.boundingRect()).boundingRect()
        if cR.contains(pos):
            # remove BFS item
            self.parent_builder.handle_remove_table(self)
            ev.accept()
            return
        tR= self.toggle_btn.mapToParent(self.toggle_btn.boundingRect()).boundingRect()
        if tR.contains(pos):
            self.is_collapsed= not self.is_collapsed
            self.update_layout()
            ev.accept()
            return

        # check for column "checkbox"
        for i,(rc,tc,checked) in enumerate(self.column_items):
            rr= rc.mapToParent(rc.boundingRect()).boundingRect()
            if rr.contains(pos):
                self.column_items[i][2]= not checked
                rc.setBrush(QBrush(Qt.blue if self.column_items[i][2] else Qt.white))
                if self.parent_builder.auto_generate:
                    self.parent_builder.generate_sql()
                ev.accept()
                return
        super().mousePressEvent(ev)

    def contextMenuEvent(self, ev):
        menu= QMenu()
        mark_tgt= menu.addAction("Mark as DML Target" if not self.is_dml_target else "Unmark as DML Target")
        rm= menu.addAction("Remove Table/CTE")
        chosen= menu.exec_(ev.screenPos())
        if chosen== mark_tgt:
            self.is_dml_target= not self.is_dml_target
            self.update_layout()
            if self.parent_builder.auto_generate:
                self.parent_builder.generate_sql()
        elif chosen== rm:
            self.parent_builder.handle_remove_table(self)

    def get_selected_columns(self):
        arr=[]
        for (r,t,ck) in self.column_items:
            if ck:
                arr.append(f"{self.table_fullname}.{t.col_name}")
        return arr

###############################################################################
# BFS Canvas
###############################################################################
class EnhancedCanvasGraphicsView(QGraphicsView):
    def __init__(self, builder, parent=None):
        super().__init__(parent)
        self.builder= builder
        self.setAcceptDrops(True)

        self.scene_= QGraphicsScene(self)
        self.setScene(self.scene_)

        self.table_items={}
        self.join_lines=[]
        self.mapping_lines=[]
        self.operation_red_line= None

        self.validation_timer= QTimer()
        self.validation_timer.setInterval(400)
        self.validation_timer.setSingleShot(True)
        self.validation_timer.timeout.connect(self.builder.validate_sql)

    def dragEnterEvent(self,e):
        if e.mimeData().hasText():
            e.acceptProposedAction()

    def dragMoveEvent(self,e):
        e.acceptProposedAction()

    def dropEvent(self,e):
        txt= e.mimeData().text()
        pos= self.mapToScene(e.pos())
        self.builder.handle_drop(txt, pos)
        e.acceptProposedAction()

    def add_table_item(self, table_key, columns, x, y):
        item= CollapsibleTableGraphicsItem(table_key, columns, self.builder, x, y)
        self.scene_.addItem(item)
        self.table_items[table_key]= item
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def remove_table_item(self, table_key):
        if table_key in self.table_items:
            it= self.table_items[table_key]
            # remove lines referencing it
            to_rm=[]
            for jl in self.join_lines:
                if jl.start_item== it or jl.end_item== it:
                    to_rm.append(jl)
            for ml in self.mapping_lines:
                # no BFS item, but check if it belongs
                pass
                # We'll also check if ml belongs. We'll do a check if ml's source_text_item or target_text_item is in "it"
            # Actually easier approach: check the BFS lines on scene
            lines_to_remove=[]
            for ln in self.scene_.items():
                if isinstance(ln, JoinLine):
                    if ln.start_item== it or ln.end_item== it:
                        lines_to_remove.append(ln)
                if isinstance(ln, MappingLine):
                    # see if the text item belongs to it
                    pass
                    # We'll do a bounding approach or cross reference
            for ln in lines_to_remove:
                self.scene_.removeItem(ln)
                if ln in self.join_lines:
                    self.join_lines.remove(ln)

            self.scene_.removeItem(it)
            del self.table_items[table_key]
            if self.builder.auto_generate:
                self.builder.generate_sql()
            self.validation_timer.start()

    def remove_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene_.removeItem(ml)
        self.mapping_lines.clear()

    def add_vertical_red_line(self,x=450):
        if self.operation_red_line:
            self.scene_.removeItem(self.operation_red_line)
            self.operation_red_line=None
        line= QGraphicsLineItem(x,0,x,9999)
        line.setPen(QPen(Qt.red,2,Qt.DashDotLine))
        line.setZValue(-10)
        self.scene_.addItem(line)
        self.operation_red_line= line

    def create_mapping_line(self, source_txt_item, target_txt_item, stype=None, ttype=None):
        ml= MappingLine(source_txt_item, target_txt_item, self, stype, ttype)
        self.scene_.addItem(ml)
        self.mapping_lines.append(ml)
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def mouseReleaseEvent(self,e):
        super().mouseReleaseEvent(e)
        for j in self.join_lines:
            j.update_line()
        for ml in self.mapping_lines:
            ml.update_pos()

###############################################################################
# FilterPanel, GroupByPanel, Aggregates, PivotWizard, SortLimit, WindowFunction
###############################################################################
class PivotWizardDialog(QDialog):
    """
    Minimal pivot wizard => sums with CASE WHEN category= ...
    """
    def __init__(self, available_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Pivot Wizard")
        self.resize(500,400)
        self.available_cols= available_cols
        self.category_col= None
        self.value_col= None
        self.distinct_vals=[]

        main= QVBoxLayout(self)
        form= QFormLayout()
        self.cat_combo= QComboBox()
        self.cat_combo.addItems(available_cols)
        form.addRow("Category Column:", self.cat_combo)
        self.val_combo= QComboBox()
        self.val_combo.addItems(available_cols)
        form.addRow("Value Column:", self.val_combo)
        main.addLayout(form)

        main.addWidget(QLabel("Choose categories (for demonstration)."))
        self.dist_list= QListWidget()
        self.dist_list.setSelectionMode(QAbstractItemView.MultiSelection)
        main.addWidget(self.dist_list)

        load_btn= QPushButton("Load Distinct (Demo Only)")
        def do_load():
            self.dist_list.clear()
            for v in ["Manager","Clerk","Sales","IT","HR"]:
                self.dist_list.addItem(v)
        load_btn.clicked.connect(do_load)
        main.addWidget(load_btn)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        main.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(main)

    def on_ok(self):
        cat= self.cat_combo.currentText()
        val= self.val_combo.currentText()
        if not cat or not val:
            QMessageBox.warning(self,"PivotWizard","Pick category & value col.")
            return
        self.category_col= cat
        self.value_col= val
        self.distinct_vals= [it.text() for it in self.dist_list.selectedItems()]
        self.accept()

    def build_expressions(self):
        arr=[]
        for dv in self.distinct_vals:
            alias= dv.lower().replace(" ","_")+"_val"
            expr= f"SUM(CASE WHEN {self.category_col}='{dv}' THEN {self.value_col} END) AS {alias}"
            arr.append(expr)
        return arr


class AddFilterDialog(QDialog):
    def __init__(self, avail_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Filter")
        self.selected_col=None
        self.selected_op=None
        self.selected_val=None
        lay= QFormLayout(self)
        self.col_combo= QComboBox()
        self.col_combo.addItems(avail_cols)
        lay.addRow("Column:", self.col_combo)

        self.op_combo= QComboBox()
        self.op_combo.addItems(["=","<>","<",">","<=",">=","IS NULL","IS NOT NULL"])
        lay.addRow("Operator:", self.op_combo)

        self.val_edit= QLineEdit("'ABC'")
        lay.addRow("Value:", self.val_edit)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        lay.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(lay)

    def on_ok(self):
        c= self.col_combo.currentText()
        if not c:
            QMessageBox.warning(self,"No col","Need a column.")
            return
        self.selected_col= c
        self.selected_op= self.op_combo.currentText()
        self.selected_val= self.val_edit.text().strip()
        self.accept()

    def get_filter(self):
        return (self.selected_col, self.selected_op, self.selected_val)

class FilterPanel(QGroupBox):
    def __init__(self,builder,parent=None):
        super().__init__("Filters", parent)
        self.builder= builder
        main= QVBoxLayout(self)
        self.setLayout(main)

        self.tabs= QTabWidget()
        main.addWidget(self.tabs)

        self.where_tab= QWidget()
        self.having_tab= QWidget()
        self.tabs.addTab(self.where_tab,"WHERE")
        self.tabs.addTab(self.having_tab,"HAVING")

        # WHERE
        wLay= QVBoxLayout(self.where_tab)
        self.where_table= QTableWidget(0,3)
        self.where_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.where_table.horizontalHeader().setStretchLastSection(True)
        self.where_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.where_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        wLay.addWidget(self.where_table)

        wh_b= QHBoxLayout()
        add_w= QPushButton("Add WHERE")
        rm_w= QPushButton("Remove WHERE")
        wh_b.addWidget(add_w)
        wh_b.addWidget(rm_w)
        wLay.addLayout(wh_b)

        add_w.clicked.connect(lambda: self.add_filter("WHERE"))
        rm_w.clicked.connect(lambda: self.remove_filter("WHERE"))

        # HAVING
        hLay= QVBoxLayout(self.having_tab)
        self.having_table= QTableWidget(0,3)
        self.having_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.having_table.horizontalHeader().setStretchLastSection(True)
        self.having_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.having_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        hLay.addWidget(self.having_table)

        hv_b= QHBoxLayout()
        add_h= QPushButton("Add HAVING")
        rm_h= QPushButton("Remove HAVING")
        hv_b.addWidget(add_h)
        hv_b.addWidget(rm_h)
        hLay.addLayout(hv_b)

        add_h.clicked.connect(lambda: self.add_filter("HAVING"))
        rm_h.clicked.connect(lambda: self.remove_filter("HAVING"))

    def add_filter(self, clause):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns to filter.")
            return
        dlg= AddFilterDialog(cols,self)
        if dlg.exec_()== QDialog.Accepted:
            c,o,v= dlg.get_filter()
            if clause=="WHERE":
                tb= self.where_table
            else:
                tb= self.having_table
            r= tb.rowCount()
            tb.insertRow(r)
            tb.setItem(r,0,QTableWidgetItem(c))
            tb.setItem(r,1,QTableWidgetItem(o))
            tb.setItem(r,2,QTableWidgetItem(v))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_filter(self, clause):
        if clause=="WHERE":
            tb= self.where_table
        else:
            tb= self.having_table
        rows= sorted([x.row() for x in tb.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            tb.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_filters(self,clause):
        if clause=="WHERE":
            tb= self.where_table
        else:
            tb= self.having_table
        arr=[]
        for r in range(tb.rowCount()):
            c= tb.item(r,0).text()
            o= tb.item(r,1).text()
            v= tb.item(r,2).text()
            arr.append((c,o,v))
        return arr

class GroupByPanel(QGroupBox):
    def __init__(self,builder,parent=None):
        super().__init__("Group By", parent)
        self.builder= builder
        main= QVBoxLayout(self)
        self.setLayout(main)

        self.gb_table= QTableWidget(0,1)
        self.gb_table.setHorizontalHeaderLabels(["GroupBy Column"])
        self.gb_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.gb_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        main.addWidget(self.gb_table)

        row= QHBoxLayout()
        add_gb= QPushButton("Add GroupBy")
        rm_gb= QPushButton("Remove GroupBy")
        row.addWidget(add_gb)
        row.addWidget(rm_gb)
        main.addLayout(row)

        add_gb.clicked.connect(self.add_group)
        rm_gb.clicked.connect(self.remove_group)

    def add_group(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns available.")
            return
        (col,ok)= QtWidgets.QInputDialog.getItem(self,"Add GroupBy","Column:", cols, 0, False)
        if ok and col:
            r= self.gb_table.rowCount()
            self.gb_table.insertRow(r)
            self.gb_table.setItem(r,0,QTableWidgetItem(col))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_group(self):
        rows= sorted([x.row() for x in self.gb_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            self.gb_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_group_by(self):
        arr=[]
        for r in range(self.gb_table.rowCount()):
            it= self.gb_table.item(r,0)
            arr.append(it.text())
        return arr

class GroupAggPanel(QGroupBox):
    def __init__(self,builder,parent=None):
        super().__init__("Aggregates", parent)
        self.builder= builder
        main= QVBoxLayout(self)
        self.setLayout(main)

        self.agg_table= QTableWidget(0,3)
        self.agg_table.setHorizontalHeaderLabels(["Function","Column","Alias"])
        self.agg_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.agg_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        main.addWidget(self.agg_table)

        row= QHBoxLayout()
        add_a= QPushButton("Add Agg")
        rm_a= QPushButton("Remove Agg")
        row.addWidget(add_a)
        row.addWidget(rm_a)
        main.addLayout(row)

        add_a.clicked.connect(self.add_agg)
        rm_a.clicked.connect(self.remove_agg)

    def add_agg(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns available.")
            return
        d= QDialog(self)
        d.setWindowTitle("Add Aggregate")
        fl= QFormLayout(d)
        func_cb= QComboBox()
        func_cb.addItems(["COUNT","SUM","AVG","MIN","MAX","CUSTOM"])
        col_cb= QComboBox()
        col_cb.addItems(cols)
        alias_ed= QLineEdit("AggVal")
        fl.addRow("Function:", func_cb)
        fl.addRow("Column:", col_cb)
        fl.addRow("Alias:", alias_ed)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        fl.addWidget(dbb)
        def okclick():
            if not col_cb.currentText() and func_cb.currentText()!="CUSTOM":
                QMessageBox.warning(d,"Error","Pick a column or use CUSTOM.")
                return
            d.accept()
        dbb.accepted.connect(okclick)
        dbb.rejected.connect(d.reject)
        d.setLayout(fl)
        if d.exec_()== QDialog.Accepted:
            f= func_cb.currentText()
            c= col_cb.currentText()
            a= alias_ed.text().strip()
            r= self.agg_table.rowCount()
            self.agg_table.insertRow(r)
            self.agg_table.setItem(r,0,QTableWidgetItem(f))
            self.agg_table.setItem(r,1,QTableWidgetItem(c))
            self.agg_table.setItem(r,2,QTableWidgetItem(a))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_agg(self):
        rows= sorted([x.row() for x in self.agg_table.selectionModel().selectedRows()],reverse=True)
        for rr in rows:
            self.agg_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_aggregates(self):
        out=[]
        for r in range(self.agg_table.rowCount()):
            f=self.agg_table.item(r,0).text()
            c=self.agg_table.item(r,1).text()
            a=self.agg_table.item(r,2).text()
            out.append((f,c,a))
        return out

class SortLimitPanel(QGroupBox):
    def __init__(self,builder,parent=None):
        super().__init__("Sort & Limit", parent)
        self.builder= builder
        main= QVBoxLayout(self)
        self.setLayout(main)

        self.sort_table= QTableWidget(0,2)
        self.sort_table.setHorizontalHeaderLabels(["Column","Direction"])
        self.sort_table.horizontalHeader().setStretchLastSection(True)
        self.sort_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sort_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        main.addWidget(self.sort_table)

        row= QHBoxLayout()
        add_s= QPushButton("Add Sort")
        rm_s= QPushButton("Remove Sort")
        row.addWidget(add_s)
        row.addWidget(rm_s)
        main.addLayout(row)

        add_s.clicked.connect(self.add_sort)
        rm_s.clicked.connect(self.remove_sort)

        hr= QHBoxLayout()
        self.limit_spin= QSpinBox()
        self.limit_spin.setRange(0,9999999)
        self.limit_spin.setValue(0)
        self.limit_spin.setSuffix(" (Limit)")
        self.limit_spin.setSpecialValueText("No Limit")
        self.limit_spin.valueChanged.connect(self.maybe_regen)
        hr.addWidget(self.limit_spin)

        self.offset_spin= QSpinBox()
        self.offset_spin.setRange(0,9999999)
        self.offset_spin.setValue(0)
        self.offset_spin.setSuffix(" (Offset)")
        self.offset_spin.setSpecialValueText("No Offset")
        self.offset_spin.valueChanged.connect(self.maybe_regen)
        hr.addWidget(self.offset_spin)

        main.addLayout(hr)

    def maybe_regen(self):
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def add_sort(self):
        cols= self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No columns","No columns available.")
            return
        d= QDialog(self)
        d.setWindowTitle("Add Sort")
        fl= QFormLayout(d)
        col_cb= QComboBox()
        col_cb.addItems(cols)
        dir_cb= QComboBox()
        dir_cb.addItems(["ASC","DESC"])
        fl.addRow("Column:", col_cb)
        fl.addRow("Direction:", dir_cb)
        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        fl.addWidget(dbb)
        def okcl():
            if not col_cb.currentText():
                QMessageBox.warning(d,"No column","Must pick column.")
                return
            d.accept()
        dbb.accepted.connect(okcl)
        dbb.rejected.connect(d.reject)
        d.setLayout(fl)
        if d.exec_()== QDialog.Accepted:
            c= col_cb.currentText()
            dd= dir_cb.currentText()
            r= self.sort_table.rowCount()
            self.sort_table.insertRow(r)
            self.sort_table.setItem(r,0,QTableWidgetItem(c))
            self.sort_table.setItem(r,1,QTableWidgetItem(dd))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_sort(self):
        rows= sorted([x.row() for x in self.sort_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            self.sort_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_order_bys(self):
        arr=[]
        for r in range(self.sort_table.rowCount()):
            c= self.sort_table.item(r,0).text()
            dr= self.sort_table.item(r,1).text()
            arr.append(f"{c} {dr}")
        return arr

    def get_limit(self):
        val= self.limit_spin.value()
        return val if val>0 else None

    def get_offset(self):
        val= self.offset_spin.value()
        return val if val>0 else None

###############################################################################
# Advanced Expression Builder => token-based
###############################################################################
class CaseWizardDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CASE Wizard")
        self.resize(500,400)
        self.available_columns= available_columns
        self.when_clauses=[]
        self.else_expr=""

        main= QVBoxLayout(self)
        self.preview= QTextEdit()
        self.preview.setReadOnly(True)
        main.addWidget(self.preview)

        row= QHBoxLayout()
        add_w= QPushButton("Add WHEN")
        rm_w= QPushButton("Remove WHEN")
        row.addWidget(add_w)
        row.addWidget(rm_w)
        main.addLayout(row)

        def add_when():
            cd,ok= QtWidgets.QInputDialog.getText(self,"CASE: WHEN cond","Enter condition (col=val):")
            if not ok or not cd.strip():
                return
            rs,ok2= QtWidgets.QInputDialog.getText(self,"CASE: THEN result","Result (literal or col):")
            if not ok2:
                return
            self.when_clauses.append((cd.strip(), rs.strip()))
            self.update_preview()

        def rm_when():
            if self.when_clauses:
                self.when_clauses.pop()
            self.update_preview()

        add_w.clicked.connect(add_when)
        rm_w.clicked.connect(rm_when)

        row2= QHBoxLayout()
        row2.addWidget(QLabel("ELSE:"))
        self.else_edit= QLineEdit()
        row2.addWidget(self.else_edit)
        main.addLayout(row2)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        main.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(main)

    def update_preview(self):
        lines=["CASE"]
        for (c,r) in self.when_clauses:
            lines.append(f"  WHEN {c} THEN {r}")
        if self.else_expr.strip():
            lines.append(f"  ELSE {self.else_expr}")
        lines.append("END")
        self.preview.setPlainText("\n".join(lines))

    def on_ok(self):
        self.else_expr= self.else_edit.text().strip()
        self.update_preview()
        self.accept()

    def get_case_expression(self):
        lines=["CASE"]
        for (c,r) in self.when_clauses:
            lines.append(f"  WHEN {c} THEN {r}")
        if self.else_expr.strip():
            lines.append(f"  ELSE {self.else_expr}")
        lines.append("END")
        return "\n".join(lines)


class SubqueryStubDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Subquery Editor")
        self.resize(600,400)
        self.result_sql=""

        main= QVBoxLayout(self)
        main.addWidget(QLabel("Type a single-column SELECT subquery:"))
        self.sub_edit= QTextEdit()
        main.addWidget(self.sub_edit)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        main.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(main)

    def on_ok(self):
        raw= self.sub_edit.toPlainText().strip()
        if not raw.lower().startswith("select"):
            QMessageBox.warning(self,"No SELECT","Must start with SELECT")
            return
        self.result_sql= f"({raw})"
        self.accept()

    def get_subquery(self):
        return self.result_sql


class ExprTokenWidget(QFrame):
    def __init__(self, token_text, parent=None):
        super().__init__(parent)
        self.token_text= token_text
        self.setFrameShape(QFrame.Box)
        self.setLineWidth(1)
        self.setStyleSheet("background-color: #f0f0f0;")
        lay= QHBoxLayout(self)
        lbl= QLabel(token_text)
        lbl.setStyleSheet("padding: 2px;")
        lay.addWidget(lbl)
        self.setLayout(lay)

    def contextMenuEvent(self, e):
        menu= QMenu()
        rm= menu.addAction("Remove Token")
        chosen= menu.exec_(e.globalPos())
        if chosen== rm:
            self.setParent(None)


class TokenFlowArea(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_layout= QHBoxLayout(self)
        self.main_layout.addStretch()
        self.setLayout(self.main_layout)

    def add_token(self, txt):
        w= ExprTokenWidget(txt, self)
        idx= self.main_layout.count() - 1
        self.main_layout.insertWidget(idx, w)

    def get_tokens(self):
        tokens=[]
        for i in range(self.main_layout.count()):
            ww= self.main_layout.itemAt(i).widget()
            if isinstance(ww, ExprTokenWidget):
                tokens.append(ww.token_text)
        return tokens


class AdvancedExpressionBuilderDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Advanced Expression Builder")
        self.resize(900,600)
        self.available_columns= available_columns or []

        main= QHBoxLayout(self)
        self.setLayout(main)

        leftp= QVBoxLayout()
        self.col_list= QListWidget()
        for c in self.available_columns:
            self.col_list.addItem(c)
        leftp.addWidget(QLabel("Columns (double-click to insert):"))
        leftp.addWidget(self.col_list,1)

        self.op_list= QListWidget()
        op_items= [
            "(", ")", "+","-","*","/","=","<",">","<=",">=","<>","AND","OR","NOT",
            "LIKE","IN","IS NULL","IS NOT NULL",
            "SUM(","AVG(","MIN(","MAX(","COUNT(","UPPER(","LOWER(","TRIM(","COALESCE(",
            "CASE (Wizard)", "SUBQUERY"
        ]
        for it in op_items:
            self.op_list.addItem(it)
        leftp.addWidget(QLabel("Operators / Functions:"))
        leftp.addWidget(self.op_list,1)

        l_container= QWidget()
        l_container.setLayout(leftp)
        main.addWidget(l_container,2)

        center_v= QVBoxLayout()
        self.token_flow= TokenFlowArea()
        center_v.addWidget(QLabel("Expression Tokens:"))
        center_v.addWidget(self.token_flow,1)

        self.syntax_label= QLabel("Syntax OK or not?")
        center_v.addWidget(self.syntax_label)

        form= QFormLayout()
        self.alias_edit= QLineEdit()
        form.addRow("Alias (optional):", self.alias_edit)
        center_v.addLayout(form)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        center_v.addWidget(dbb)
        c_container= QWidget()
        c_container.setLayout(center_v)
        main.addWidget(c_container,3)

        self.col_list.itemDoubleClicked.connect(self.on_col_dbl)
        self.op_list.itemDoubleClicked.connect(self.on_op_dbl)

        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)

    def on_col_dbl(self, item):
        self.token_flow.add_token(item.text())
        self.check_syntax()

    def on_op_dbl(self, item):
        txt= item.text()
        if txt== "CASE (Wizard)":
            cw= CaseWizardDialog(self.available_columns, self)
            if cw.exec_()== QDialog.Accepted:
                cexpr= cw.get_case_expression()
                self.token_flow.add_token(cexpr)
        elif txt== "SUBQUERY":
            sq= SubqueryStubDialog(self)
            if sq.exec_()== QDialog.Accepted:
                self.token_flow.add_token(sq.get_subquery())
        else:
            self.token_flow.add_token(txt)
        self.check_syntax()

    def build_expression(self):
        tokens= self.token_flow.get_tokens()
        out=[]
        for t in tokens:
            if t.endswith("(") or t.startswith(")"):
                out.append(t)
            else:
                out.append(f" {t} ")
        return "".join(out).strip()

    def check_syntax(self):
        expr_str= self.build_expression()
        if not expr_str:
            self.syntax_label.setText("No expression.")
            self.syntax_label.setStyleSheet("color: black;")
            return
        try:
            st= sqlparse.parse(expr_str)
            if not st:
                raise ValueError("No parse result")
            self.syntax_label.setText("Expression Syntax: OK")
            self.syntax_label.setStyleSheet("color: green;")
        except Exception as ex:
            self.syntax_label.setText(f"Syntax ERROR => {ex}")
            self.syntax_label.setStyleSheet("color: red;")

    def on_ok(self):
        expr= self.build_expression()
        if not expr:
            QMessageBox.warning(self,"No Expression","Empty expression.")
            return
        self.accept()

    def get_expression_data(self):
        return (self.alias_edit.text().strip(), self.build_expression())

###############################################################################
# WindowFunctionWizard
###############################################################################
class AdvancedWindowFunctionDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Window Function Wizard")
        self.resize(500,500)
        self.available_columns= available_columns
        self.function=""
        self.main_col=""
        self.partition_cols=[]
        self.order_cols=[]
        self.frame_clause=""
        self.offset=1
        self.default_val="0"
        self.buckets=4
        self.alias="winfun"

        main= QVBoxLayout(self)
        frm= QFormLayout()

        self.func_cb= QComboBox()
        self.func_cb.addItems(["ROW_NUMBER","RANK","DENSE_RANK","NTILE","LAG","LEAD","FIRST_VALUE","LAST_VALUE","SUM","AVG","MIN","MAX"])
        frm.addRow("Function:", self.func_cb)

        self.col_cb= QComboBox()
        self.col_cb.addItems(["(No col)"]+self.available_columns)
        frm.addRow("Main Column:", self.col_cb)

        self.ntile_sb= QSpinBox()
        self.ntile_sb.setRange(2,999)
        self.ntile_sb.setValue(4)
        frm.addRow("NTILE Buckets:", self.ntile_sb)

        self.offset_sb= QSpinBox()
        self.offset_sb.setRange(1,999)
        self.offset_sb.setValue(1)
        frm.addRow("LAG/LEAD Offset:", self.offset_sb)

        self.default_ed= QLineEdit("0")
        frm.addRow("LAG/LEAD Default:", self.default_ed)

        self.alias_ed= QLineEdit("winfun")
        frm.addRow("Alias:", self.alias_ed)
        main.addLayout(frm)

        main.addWidget(QLabel("Partition By (multi-select):"))
        self.part_list= QListWidget()
        self.part_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.part_list.addItems(self.available_columns)
        main.addWidget(self.part_list)

        main.addWidget(QLabel("Order By (multi-select):"))
        self.order_list= QListWidget()
        self.order_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self.order_list.addItems(self.available_columns)
        main.addWidget(self.order_list)

        self.frame_ed= QLineEdit("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
        main.addWidget(QLabel("Frame Clause (optional):"))
        main.addWidget(self.frame_ed)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        main.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(main)

    def on_ok(self):
        fn= self.func_cb.currentText()
        mc= self.col_cb.currentText()
        parts= [it.text() for it in self.part_list.selectedItems()]
        orders=[it.text() for it in self.order_list.selectedItems()]
        fr= self.frame_ed.text().strip()
        a= self.alias_ed.text().strip()
        if not a:
            QMessageBox.warning(self,"No alias","Need alias.")
            return
        self.function= fn
        if mc!="(No col)":
            self.main_col= mc
        self.partition_cols= parts
        self.order_cols= orders
        self.frame_clause= fr
        self.alias= a
        self.offset= self.offset_sb.value()
        self.default_val= self.default_ed.text().strip()
        self.buckets= self.ntile_sb.value()
        self.accept()

    def get_expression(self):
        parts=[]
        if self.partition_cols:
            parts.append("PARTITION BY "+", ".join(self.partition_cols))
        if self.order_cols:
            parts.append("ORDER BY "+", ".join(self.order_cols))
        if self.frame_clause:
            parts.append(self.frame_clause)
        inside="()"
        if parts:
            inside="("+ " ".join(parts) +")"

        fn= self.function.upper()
        col= self.main_col if self.main_col else "0"
        if fn in ["ROW_NUMBER","RANK","DENSE_RANK"]:
            return f"{fn}() OVER {inside} AS {self.alias}"
        elif fn=="NTILE":
            return f"NTILE({self.buckets}) OVER {inside} AS {self.alias}"
        elif fn in ["LAG","LEAD"]:
            return f"{fn}({col}, {self.offset}, {self.default_val}) OVER {inside} AS {self.alias}"
        elif fn in ["FIRST_VALUE","LAST_VALUE","SUM","AVG","MIN","MAX"]:
            return f"{fn}({col}) OVER {inside} AS {self.alias}"
        else:
            return f"ROW_NUMBER() OVER {inside} AS {self.alias}"

###############################################################################
# DataProfiler
###############################################################################
class ProfilerChartCanvas(FigureCanvasQTAgg):
    def __init__(self, data_list, col_name="", parent=None):
        fig= Figure()
        super().__init__(fig)
        self.setParent(parent)
        self.axes= fig.add_subplot(111)
        self.data= data_list
        self.col_name= col_name
        self.plot_data()

    def plot_data(self):
        self.axes.clear()
        if not self.data:
            self.axes.text(0.5,0.5,"No numeric data",ha='center',va='center')
            self.draw()
            return
        self.axes.boxplot(self.data,labels=[self.col_name])
        self.axes.set_title(f"Outlier Chart: {self.col_name}")
        self.draw()

class DataProfilerDialog(QDialog):
    def __init__(self, table_key, columns, connection, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Data Profiler - {table_key}")
        self.resize(900,500)
        self.table_key= table_key
        self.columns= columns
        self.connection= connection

        main= QVBoxLayout(self)
        info= QLabel(
            f"Profiling {table_key} => COUNT(*), DISTINCT, MIN, MAX, AVG => 'ERR' if fails.\n"
            "Use 'Outlier Chart' for numeric data."
        )
        main.addWidget(info)

        self.prof_table= QTableWidget(0,7)
        self.prof_table.setHorizontalHeaderLabels(["Column","COUNT","DISTINCT","MIN","MAX","AVG","Error"])
        main.addWidget(self.prof_table)

        row= QHBoxLayout()
        chart_b= QPushButton("Outlier Chart")
        chart_b.clicked.connect(self.show_outlier)
        close_b= QPushButton("Close")
        close_b.clicked.connect(self.accept)
        row.addWidget(chart_b)
        row.addStretch()
        row.addWidget(close_b)
        main.addLayout(row)
        self.setLayout(main)

        self.run_profiler()

    def run_profiler(self):
        if not self.connection:
            QMessageBox.warning(self,"No Connection","No DB connection for profiling.")
            return
        c= self.connection.cursor()
        self.prof_table.setRowCount(0)

        for col in self.columns:
            short_col= col.split(".")[-1]
            r= self.prof_table.rowCount()
            self.prof_table.insertRow(r)
            self.prof_table.setItem(r,0,QTableWidgetItem(short_col))

            # COUNT(*)
            co_val=""
            try:
                c.execute(f"SELECT COUNT(*) FROM {self.table_key}")
                rr=c.fetchone()
                co_val=str(rr[0]) if rr else "0"
            except Exception as ex:
                co_val=f"ERR({ex})"
            self.prof_table.setItem(r,1,QTableWidgetItem(co_val))

            # DISTINCT
            dist_val=""
            try:
                c.execute(f"SELECT COUNT(DISTINCT {short_col}) FROM {self.table_key}")
                rr=c.fetchone()
                dist_val=str(rr[0]) if rr else "0"
            except Exception as ex:
                dist_val=f"ERR({ex})"
            self.prof_table.setItem(r,2,QTableWidgetItem(dist_val))

            minv,maxv,avgv="N/A","N/A","N/A"
            err_msg=""
            try:
                c.execute(f"SELECT MIN({short_col}) FROM {self.table_key}")
                rres= c.fetchone()
                if rres and rres[0]!=None:
                    minv= str(rres[0])
                else:
                    minv="NULL"
            except Exception as ex:
                minv="ERR"
                err_msg= str(ex)
            try:
                c.execute(f"SELECT MAX({short_col}) FROM {self.table_key}")
                rres= c.fetchone()
                if rres and rres[0]!=None:
                    maxv= str(rres[0])
                else:
                    maxv="NULL"
            except Exception as ex:
                maxv="ERR"
                if not err_msg:
                    err_msg=str(ex)
            try:
                c.execute(f"SELECT AVG({short_col}) FROM {self.table_key}")
                rres= c.fetchone()
                if rres and rres[0]!=None:
                    avgv= str(rres[0])
                else:
                    avgv="NULL"
            except Exception as ex:
                avgv="ERR"
                if not err_msg:
                    err_msg=str(ex)

            self.prof_table.setItem(r,3,QTableWidgetItem(minv))
            self.prof_table.setItem(r,4,QTableWidgetItem(maxv))
            self.prof_table.setItem(r,5,QTableWidgetItem(avgv))
            self.prof_table.setItem(r,6,QTableWidgetItem(err_msg))

    def show_outlier(self):
        rows= self.prof_table.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self,"No selection","Pick row first.")
            return
        row_idx= rows[0].row()
        cName= self.prof_table.item(row_idx,0).text()

        data_list=[]
        try:
            c= self.connection.cursor()
            sql= f"SELECT {cName} FROM {self.table_key} WHERE {cName} IS NOT NULL"
            c.execute(sql)
            rr= c.fetchall()
            for r_ in rr:
                val= r_[0]
                data_list.append(float(val))
        except Exception as ex:
            QMessageBox.warning(self,"Outlier Error",f"Cannot fetch numeric data:\n{ex}")
            return

        if not data_list:
            QMessageBox.information(self,"No data","No numeric data or table empty.")
            return

        d= QDialog(self)
        d.setWindowTitle(f"Outlier Chart: {cName}")
        d.resize(600,400)
        ly= QVBoxLayout(d)
        can= ProfilerChartCanvas(data_list,cName,d)
        ly.addWidget(can)
        dbb= QDialogButtonBox(QDialogButtonBox.Ok)
        ly.addWidget(dbb)
        dbb.accepted.connect(d.accept)
        d.setLayout(ly)
        d.exec_()

###############################################################################
# SQLImportTab => parse with sqlglot => partial BFS
###############################################################################
class SQLImportTab(QWidget):
    def __init__(self,builder=None, parent=None):
        super().__init__(parent)
        self.builder= builder
        main= QVBoxLayout(self)
        instruct= QLabel("Paste or type SQL, then 'Import & Rebuild' via sqlglot.\n"
                         "Complex queries may only partially import BFS.")
        main.addWidget(instruct)

        self.sql_edit= QTextEdit()
        main.addWidget(self.sql_edit)

        row= QHBoxLayout()
        self.import_btn= QPushButton("Import & Rebuild")
        row.addWidget(self.import_btn)
        main.addLayout(row)

        self.import_btn.clicked.connect(self.on_import)
        self.setLayout(main)

    def on_import(self):
        raw= self.sql_edit.toPlainText().strip()
        if not raw:
            QMessageBox.information(self,"Empty SQL","No SQL to parse.")
            return
        try:
            st= sqlparse.parse(raw)
            if not st:
                QMessageBox.warning(self,"No valid SQL","No statements found.")
                return
        except Exception as ex:
            QMessageBox.warning(self,"SyntaxError",f"sqlparse:\n{ex}")
            return

        try:
            expr= sqlglot.parse_one(raw)
        except Exception as ex:
            QMessageBox.warning(self,"sqlglot parse error", f"Could not parse:\n{ex}")
            return

        self.builder.import_and_rebuild_canvas(expr,raw)
        QMessageBox.information(self,"Import OK","Canvas has been rebuilt from the SQL.")


###############################################################################
# CTE => BFS
###############################################################################
class CTEDialog(QDialog):
    def __init__(self, builder_ref, existing_name="", existing_sql="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Define CTE")
        self.resize(900,600)
        self.builder= builder_ref
        self.cte_name= existing_name
        self.cte_sql= existing_sql

        main= QVBoxLayout(self)
        frm= QFormLayout()
        self.name_edit= QLineEdit(self.cte_name)
        frm.addRow("CTE Name:", self.name_edit)
        main.addLayout(frm)

        self.cte_edit= QTextEdit()
        self.cte_edit.setPlainText(self.cte_sql)
        main.addWidget(self.cte_edit)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        main.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(main)

    def on_ok(self):
        nm= self.name_edit.text().strip()
        if not nm:
            QMessageBox.warning(self,"No name","CTE name empty.")
            return
        sq= self.cte_edit.toPlainText().strip()
        if not sq:
            QMessageBox.warning(self,"No subquery","CTE SQL empty.")
            return
        self.cte_name= nm
        self.cte_sql= sq
        self.accept()

    def get_cte_data(self):
        return (self.cte_name, self.cte_sql)

class CTEPanel(QGroupBox):
    def __init__(self,builder, parent=None):
        super().__init__("CTEs", parent)
        self.builder= builder
        self.cte_data=[]
        main= QVBoxLayout(self)
        self.setLayout(main)

        self.cte_table= QTableWidget(0,3)
        self.cte_table.setHorizontalHeaderLabels(["CTE Name","(Placeholder)","Preview SQL"])
        self.cte_table.horizontalHeader().setSectionResizeMode(0,QHeaderView.ResizeToContents)
        self.cte_table.horizontalHeader().setSectionResizeMode(1,QHeaderView.ResizeToContents)
        self.cte_table.horizontalHeader().setSectionResizeMode(2,QHeaderView.Stretch)
        self.cte_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.cte_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        main.addWidget(self.cte_table)

        row= QHBoxLayout()
        add_b= QPushButton("Add CTE")
        edit_b= QPushButton("Edit CTE")
        rm_b= QPushButton("Remove CTE")
        row.addWidget(add_b)
        row.addWidget(edit_b)
        row.addWidget(rm_b)
        row.addStretch()
        main.addLayout(row)

        add_b.clicked.connect(self.on_add)
        edit_b.clicked.connect(self.on_edit)
        rm_b.clicked.connect(self.on_remove)

    def on_add(self):
        dlg= CTEDialog(self.builder,"","",self)
        if dlg.exec_()== QDialog.Accepted:
            n,sql= dlg.get_cte_data()
            self._add_cte_row(n,sql)
            # BFS => we show as virtual table
            self.builder.show_cte_as_virtual_table(n,["col1","col2"])
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def on_edit(self):
        rows= self.cte_table.selectionModel().selectedRows()
        if not rows:
            QMessageBox.information(self,"No row","Select a row.")
            return
        row_idx= rows[0].row()
        cName= self.cte_table.item(row_idx,0).text()
        cSQL= self.cte_table.item(row_idx,2).text()

        dlg= CTEDialog(self.builder,cName,cSQL,self)
        if dlg.exec_()== QDialog.Accepted:
            newName, newSQL= dlg.get_cte_data()
            self.cte_table.setItem(row_idx,0,QTableWidgetItem(newName))
            self.cte_table.setItem(row_idx,2,QTableWidgetItem(newSQL))
            self.cte_data[row_idx]['name']= newName
            self.cte_data[row_idx]['sql']= newSQL
            if newName!= cName:
                self.builder.remove_virtual_cte_table(cName)
            self.builder.show_cte_as_virtual_table(newName,["col1","col2"])
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def on_remove(self):
        rows= sorted([r.row() for r in self.cte_table.selectionModel().selectedRows()], reverse=True)
        for rr in rows:
            nm= self.cte_table.item(rr,0).text()
            self.cte_table.removeRow(rr)
            del self.cte_data[rr]
            self.builder.remove_virtual_cte_table(nm)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def _add_cte_row(self, n, sql):
        r= self.cte_table.rowCount()
        self.cte_table.insertRow(r)
        self.cte_table.setItem(r,0,QTableWidgetItem(n))
        self.cte_table.setItem(r,1,QTableWidgetItem("(Edit)"))
        self.cte_table.setItem(r,2,QTableWidgetItem(sql))
        self.cte_data.append({'name':n,'sql':sql})

    def get_ctes(self):
        out=[]
        for r in range(self.cte_table.rowCount()):
            nm= self.cte_table.item(r,0).text()
            s= self.cte_table.item(r,2).text()
            out.append((nm,s))
        return out

###############################################################################
# Simple result data dialog
###############################################################################
class ResultDataDialog(QDialog):
    def __init__(self, rows, columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("SQL Results")
        self.resize(800,400)
        main= QVBoxLayout(self)
        tbl= QTableWidget(len(rows), len(columns))
        tbl.setHorizontalHeaderLabels(columns)
        for rr, rowval in enumerate(rows):
            for cc, val in enumerate(rowval):
                it= QTableWidgetItem(str(val))
                tbl.setItem(rr,cc,it)
        main.addWidget(tbl)
        dbb= QDialogButtonBox(QDialogButtonBox.Ok)
        dbb.accepted.connect(self.accept)
        main.addWidget(dbb)
        self.setLayout(main)

###############################################################################
# Sub VQB => combine queries
###############################################################################
class SubVQBDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Combine Query (Sub VQB)")
        self.resize(900,600)
        self.operator="UNION"
        self.second_sql=""

        lay= QVBoxLayout(self)
        row= QHBoxLayout()
        row.addWidget(QLabel("Combine Operator:"))
        self.op_combo= QComboBox()
        self.op_combo.addItems(["UNION","UNION ALL","INTERSECT","EXCEPT"])
        row.addWidget(self.op_combo)
        row.addStretch()
        lay.addLayout(row)

        self.sub_edit= QTextEdit()
        lay.addWidget(self.sub_edit)

        dbb= QDialogButtonBox(QDialogButtonBox.Ok| QDialogButtonBox.Cancel)
        lay.addWidget(dbb)
        dbb.accepted.connect(self.on_ok)
        dbb.rejected.connect(self.reject)
        self.setLayout(lay)

    def on_ok(self):
        op= self.op_combo.currentText()
        sq= self.sub_edit.toPlainText().strip()
        if not sq:
            QMessageBox.warning(self,"No Query","No sub query typed.")
            return
        self.operator= op
        self.second_sql= sq
        self.accept()

    def getResult(self):
        return (self.operator, self.second_sql)

###############################################################################
# The main VQB Tab
###############################################################################
class VisualQueryBuilderTab(QWidget):
    def __init__(self, multi_connections=None, linked_map=None, parent=None):
        super().__init__(parent)
        self.connections= multi_connections if multi_connections else {}
        self.linked_server_map= linked_map if linked_map else {}
        self.auto_generate= True
        self.operation_mode= "SELECT"

        self.table_columns_map={}

        self.init_ui()
        self.threadpool= QThreadPool.globalInstance()

    def init_ui(self):
        main= QVBoxLayout(self)

        row1= QHBoxLayout()
        self.status_light= QFrame()
        self.status_light.setFixedSize(15,15)
        self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color:red;}")
        self.conn_label= QLabel("Not Connected")
        row1.addWidget(self.status_light)
        row1.addWidget(self.conn_label)

        self.auto_chk= QCheckBox("Auto-Generate")
        self.auto_chk.setChecked(True)
        self.auto_chk.stateChanged.connect(lambda s: setattr(self,"auto_generate",(s== Qt.Checked)))
        row1.addWidget(self.auto_chk)
        row1.addStretch()
        main.addLayout(row1)

        row2= QHBoxLayout()
        subq_b= QPushButton("Add SubQuery to Canvas")
        expr_b= QPushButton("Expression Builder")
        wfun_b= QPushButton("Window Function Wizard")
        comb_b= QPushButton("Combine Query (Sub VQB)")
        self.op_combo= QComboBox()
        self.op_combo.addItems(["SELECT","INSERT","UPDATE","DELETE"])
        row2.addWidget(subq_b)
        row2.addWidget(expr_b)
        row2.addWidget(wfun_b)
        row2.addWidget(comb_b)
        row2.addWidget(self.op_combo)
        row2.addStretch()
        main.addLayout(row2)

        subq_b.clicked.connect(self.add_subquery_item)
        expr_b.clicked.connect(self.launch_expr_builder)
        wfun_b.clicked.connect(self.launch_window_func)
        comb_b.clicked.connect(self.combine_with_subvqb)
        self.op_combo.currentIndexChanged.connect(self.on_op_changed)

        self.tabs= QTabWidget()
        main.addWidget(self.tabs)

        self.schema_tab= QWidget()
        self.config_tab= QWidget()
        self.sql_tab= QWidget()
        self.import_tab= SQLImportTab(builder=self)

        self.tabs.addTab(self.schema_tab,"Schema & Canvas")
        self.tabs.addTab(self.config_tab,"Query Config")
        self.tabs.addTab(self.sql_tab,"SQL Preview")
        self.tabs.addTab(self.import_tab,"SQL Import")

        self.status_bar= QStatusBar()
        main.addWidget(self.status_bar)
        self.setLayout(main)

        self.setup_schema_tab()
        self.setup_config_tab()
        self.setup_sql_tab()

    def setup_schema_tab(self):
        lay= QVBoxLayout(self.schema_tab)
        self.search_ed= QLineEdit()
        self.search_ed.setPlaceholderText("Filter schema tree...")
        self.search_ed.textChanged.connect(self.filter_schema)
        lay.addWidget(self.search_ed)

        splitter= QSplitter(Qt.Horizontal)
        self.schema_tree= MultiDBLazySchemaTreeWidget(self.connections, parent_builder=self)
        leftp= QWidget()
        lyp= QVBoxLayout(leftp)
        lyp.addWidget(self.schema_tree)
        splitter.addWidget(leftp)

        self.canvas= EnhancedCanvasGraphicsView(self)
        splitter.addWidget(self.canvas)
        splitter.setStretchFactor(0,1)
        splitter.setStretchFactor(1,3)
        lay.addWidget(splitter)

        self.progress= QProgressBar()
        self.progress.setVisible(False)
        lay.addWidget(self.progress)

    def setup_config_tab(self):
        h= QHBoxLayout(self.config_tab)
        self.cte_panel= CTEPanel(self)
        h.addWidget(self.cte_panel,2)

        self.filter_panel= FilterPanel(self)
        h.addWidget(self.filter_panel,2)

        col= QVBoxLayout()
        self.group_panel= GroupByPanel(self)
        self.agg_panel= GroupAggPanel(self)
        col.addWidget(self.group_panel)
        col.addWidget(self.agg_panel)
        midw= QWidget()
        midw.setLayout(col)
        h.addWidget(midw,3)

        self.sort_panel= SortLimitPanel(self)
        h.addWidget(self.sort_panel,2)
        self.config_tab.setLayout(h)

    def setup_sql_tab(self):
        lay= QVBoxLayout(self.sql_tab)
        top= QHBoxLayout()
        top.addWidget(QLabel("Generated SQL:"))
        run_b= QPushButton("Run SQL")
        run_b.clicked.connect(self.run_sql)
        top.addWidget(run_b, alignment=Qt.AlignRight)
        lay.addLayout(top)

        self.sql_edit= QTextEdit()
        self.sql_edit.setReadOnly(False)
        self.sql_highlighter= SQLHighlighter(self.sql_edit.document())
        lay.addWidget(self.sql_edit)

        self.validation_lbl= QLabel("SQL Status: Unknown")
        lay.addWidget(self.validation_lbl)

        prof_b= QPushButton("Data Profiler")
        prof_b.clicked.connect(self.launch_data_profiler)
        lay.addWidget(prof_b, alignment=Qt.AlignRight)

        self.sql_tab.setLayout(lay)

    def set_connections(self, conns):
        self.connections= conns
        if conns:
            self.update_conn_status(True, f"{len(conns)} DSNs connected.")
        else:
            self.update_conn_status(False,"No Connections")
        self.schema_tree.connections= conns
        self.schema_tree.populate_roots()

    def set_federation_map(self, newmap):
        self.linked_server_map= newmap

    def update_conn_status(self, st, txt):
        if st:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green;}")
            self.conn_label.setText(txt)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red;}")
            self.conn_label.setText("Not Connected")

    def filter_schema(self, val):
        # naive filter
        def do_f(i, text):
            show= False
            if text.lower() in i.text(0).lower():
                show= True
            for ch in range(i.childCount()):
                if do_f(i.child(ch), text):
                    show= True
            i.setHidden(not show)
            return show

        for r in range(self.schema_tree.topLevelItemCount()):
            topi= self.schema_tree.topLevelItem(r)
            do_f(topi,val)

    def add_subquery_item(self):
        # BFS => nested subquery item
        from_ = self.canvas.mapToScene(300,200)
        # We'll just place it at (300,200).
        # Or we can do something else
        x= from_.x()
        y= from_.y()
        it= NestedSubqueryItem(self, x,y)
        self.canvas.scene_.addItem(it)
        key= f"SubQueryItem_{id(it)}"
        self.canvas.table_items[key]= it
        if self.auto_generate:
            self.generate_sql()

    def launch_expr_builder(self):
        # advanced token-based expr builder
        cols= self.get_all_possible_columns_for_dialog()
        dlg= AdvancedExpressionBuilderDialog(cols,self)
        if dlg.exec_()== QDialog.Accepted:
            al, expr= dlg.get_expression_data()
            old= self.sql_edit.toPlainText()
            if al:
                newp= f"({expr}) AS {al}"
            else:
                newp= expr
            self.sql_edit.setPlainText(old+ "\n-- Derived expression:\n"+ newp)
            self.validate_sql()

    def launch_window_func(self):
        c= self.get_all_possible_columns_for_dialog()
        dlg= AdvancedWindowFunctionDialog(c,self)
        if dlg.exec_()== QDialog.Accepted:
            e= dlg.get_expression()
            old= self.sql_edit.toPlainText()
            self.sql_edit.setPlainText(old+ f"\n-- WindowFunc:\n{e}")
            self.validate_sql()

    def combine_with_subvqb(self):
        d= SubVQBDialog(self)
        if d.exec_()== QDialog.Accepted:
            op, ssql= d.getResult()
            old= self.sql_edit.toPlainText().strip()
            if old:
                self.sql_edit.setPlainText(old+f"\n{op}\n(\n{ssql}\n)")
            else:
                self.sql_edit.setPlainText(f"{op}\n(\n{ssql}\n)")
            self.validate_sql()

    def on_op_changed(self, idx):
        modes=["SELECT","INSERT","UPDATE","DELETE"]
        self.operation_mode= modes[idx]
        if self.auto_generate:
            self.generate_sql()

    def run_sql(self):
        raw= self.sql_edit.toPlainText().strip()
        if not raw:
            QMessageBox.information(self,"Empty SQL","No SQL to run.")
            return
        if not self.connections:
            QMessageBox.information(self,"No Connections","No DB connection.")
            return
        first_alias= list(self.connections.keys())[0]
        conn= self.connections[first_alias]["connection"]
        if not conn:
            QMessageBox.warning(self,"No conn","Invalid connection.")
            return
        try:
            c= conn.cursor()
            c.execute(raw)
            rows= c.fetchall()
            cols= [d[0] for d in c.description] if c.description else []
            rd= ResultDataDialog(rows, cols, self)
            rd.exec_()
        except Exception as ex:
            QMessageBox.warning(self,"SQL Error",f"{ex}")

    def launch_data_profiler(self):
        # pick BFS table item => real table => data profiler
        real_tables=[]
        for k,v in self.canvas.table_items.items():
            if hasattr(v,"columns") and not k.startswith("SubQueryItem_") and not k.startswith("CTE."):
                real_tables.append(k)
        if not real_tables:
            QMessageBox.information(self,"No Table","No real BFS tables.")
            return
        chosen,ok= QtWidgets.QInputDialog.getItem(self,"Pick Table","",real_tables,0,False)
        if not ok or not chosen:
            return
        item= self.canvas.table_items.get(chosen)
        if not hasattr(item,"columns"):
            QMessageBox.warning(self,"No columns?","BFS item has no columns.")
            return
        alias= chosen.split(".")[0]
        conn=None
        if alias in self.connections:
            conn= self.connections[alias]["connection"]
        if not conn:
            QMessageBox.warning(self,"No connection","No DB conn for that alias.")
            return
        colkeys=[]
        for c in item.columns:
            colkeys.append(f"{chosen}.{c}")
        d= DataProfilerDialog(chosen,colkeys,conn,self)
        d.exec_()

    def get_all_possible_columns_for_dialog(self):
        arr=[]
        for k,v in self.canvas.table_items.items():
            if hasattr(v,"columns"):
                for c in v.columns:
                    arr.append(f"{k}.{c}")
        return arr

    def handle_drop(self, full_name, pos):
        # parse => alias.db.table
        # load columns
        parts= full_name.split(".")
        if len(parts)<3:
            return
        alias, dbn, tbl= parts[0], parts[1], parts[2]
        info= self.connections.get(alias)
        if info:
            c= info["connection"]
            dbt= info["db_type"]
            cols= load_columns(c, dbt, dbn, tbl)
            if not cols:
                cols=["col1","col2","col3"]
            self.table_columns_map[full_name]= cols
        else:
            # fallback
            if full_name not in self.table_columns_map:
                self.table_columns_map[full_name]=["col1","col2"]

        col_list= self.table_columns_map[full_name]
        self.canvas.add_table_item(full_name, col_list, pos.x(), pos.y())

    def validate_sql(self):
        txt= self.sql_edit.toPlainText().strip()
        if not txt:
            self.validation_lbl.setText("SQL Status: No SQL.")
            self.validation_lbl.setStyleSheet("color:orange;")
            return
        try:
            FullSQLParser(txt).parse()
            self.validation_lbl.setText("SQL Status: Valid.")
            self.validation_lbl.setStyleSheet("color:green;")
        except Exception as ex:
            self.validation_lbl.setText(f"SQL Status: Invalid - {ex}")
            self.validation_lbl.setStyleSheet("color:red;")

    def import_and_rebuild_canvas(self, expr, full_sql):
        # Clear BFS
        for k in list(self.canvas.table_items.keys()):
            self.canvas.remove_table_item(k)
        self.canvas.remove_mapping_lines()

        # Clear filter
        while self.filter_panel.where_table.rowCount()>0:
            self.filter_panel.where_table.removeRow(0)
        while self.filter_panel.having_table.rowCount()>0:
            self.filter_panel.having_table.removeRow(0)

        # Clear group
        while self.group_panel.gb_table.rowCount()>0:
            self.group_panel.gb_table.removeRow(0)
        while self.agg_panel.agg_table.rowCount()>0:
            self.agg_panel.agg_table.removeRow(0)

        # Clear sort
        while self.sort_panel.sort_table.rowCount()>0:
            self.sort_panel.sort_table.removeRow(0)
        self.sort_panel.limit_spin.setValue(0)
        self.sort_panel.offset_spin.setValue(0)

        # Clear ctes
        while self.cte_panel.cte_table.rowCount()>0:
            self.cte_panel.cte_table.removeRow(0)
        self.cte_panel.cte_data.clear()

        # If expr key= WITH => parse ctes
        main_expr= expr
        if expr.key=="WITH":
            cte_exps= expr.args.get("expressions") or []
            for cexp in cte_exps:
                cname= cexp.alias
                csql= cexp.this.sql()
                self.cte_panel._add_cte_row(cname, csql)
            main_expr= expr.this

        # If main_expr not SELECT => just show
        if not isinstance(main_expr, exp.Select):
            self.sql_edit.setPlainText(full_sql)
            self.validate_sql()
            return

        # simple approach
        self.sql_edit.setPlainText(full_sql)
        self.validate_sql()

    def toggle_dml_canvas(self):
        # we no longer do placeholders. We'll just let user mark BFS items as DML target.
        pass

    def generate_sql(self):
        if not self.auto_generate:
            return
        if self.operation_mode=="INSERT":
            body= self._gen_insert()
        elif self.operation_mode=="UPDATE":
            body= self._gen_update()
        elif self.operation_mode=="DELETE":
            body= self._gen_delete()
        else:
            body= self._gen_select()

        ctes= self.cte_panel.get_ctes()
        if ctes:
            cparts=[]
            for n, s in ctes:
                cparts.append(f"{n} AS (\n{s}\n)")
            cblock= "WITH "+ ",\n  ".join(cparts)+ "\n"
            final_sql= cblock + body
        else:
            final_sql= body
        self.sql_edit.setPlainText(final_sql)
        self.validate_sql()

    def _transform_for_fed(self, table_key):
        # parse => alias.db.table
        # if alias in linked_server_map => rewrite
        parts= table_key.split(".")
        if len(parts)<3:
            return table_key
        alias= parts[0]
        ls= self.linked_server_map.get(alias)
        if ls:
            dbn= parts[1]
            tbl= parts[2]
            return f"[{ls}].[{dbn}].dbo.[{tbl}]"
        return table_key

    def _build_bfs_from(self):
        invert= {v:k for k,v in self.canvas.table_items.items()}
        adj={}
        for k in self.canvas.table_items.keys():
            adj[k]=[]
        # gather join lines
        for it in self.canvas.scene_.items():
            if isinstance(it,JoinLine):
                s= invert.get(it.start_item)
                e= invert.get(it.end_item)
                if s and e:
                    adj[s].append((e,it))
                    adj[e].append((s,it))

        visited= set()
        blocks=[]
        for root in adj:
            if root not in visited:
                queue=[root]
                visited.add(root)
                seg=[root]
                while queue:
                    nd= queue.pop(0)
                    for (nbr,ln) in adj[nd]:
                        if nbr not in visited:
                            visited.add(nbr)
                            queue.append(nbr)
                            seg.append(f"{ln.join_type} JOIN {nbr} ON {ln.condition}")
                block= "\n  ".join(seg)
                blocks.append("FROM "+block)

        if not blocks:
            return "-- no tables"
        # rewrite for cross-DB
        final=[]
        for blk in blocks:
            lines= blk.split("\n")
            outblk=[]
            for line in lines:
                tokens= line.split()
                row=[]
                for t in tokens:
                    if t.count(".")>=2 and t.upper() not in ["FROM","JOIN","ON","INNER","LEFT","RIGHT","FULL"]:
                        row.append(self._transform_for_fed(t))
                    else:
                        row.append(t)
                outblk.append(" ".join(row))
            final.append("\n".join(outblk))
        return "\n".join(final)

    def _gen_select(self):
        scols=[]
        for k,it in self.canvas.table_items.items():
            if hasattr(it,"get_selected_columns"):
                scols.extend(it.get_selected_columns())
        if not scols:
            scols=["*"]
        # also aggregator
        aex= self.agg_panel.get_aggregates()
        final_cols= list(scols)
        for (f,c,a) in aex:
            if f.upper()=="CUSTOM":
                final_cols.append(c)
            else:
                final_cols.append(f"{f}({c}) AS {a}")

        lines=[]
        lines.append("SELECT "+", ".join(final_cols))
        lines.append(self._build_bfs_from())

        wh= self.filter_panel.get_filters("WHERE")
        if wh:
            conds= [f"{a[0]} {a[1]} {a[2]}" for a in wh]
            lines.append("WHERE "+ " AND ".join(conds))

        gb= self.group_panel.get_group_by()
        if gb:
            lines.append("GROUP BY "+", ".join(gb))

        hv= self.filter_panel.get_filters("HAVING")
        if hv:
            conds= [f"{a[0]} {a[1]} {a[2]}" for a in hv]
            lines.append("HAVING "+ " AND ".join(conds))

        ob= self.sort_panel.get_order_bys()
        if ob:
            lines.append("ORDER BY "+", ".join(ob))

        lm= self.sort_panel.get_limit()
        if lm is not None:
            lines.append(f"LIMIT {lm}")
        off= self.sort_panel.get_offset()
        if off is not None:
            lines.append(f"OFFSET {off}")
        return "\n".join(lines)

    def _gen_select_noagg(self):
        scols=[]
        for k,it in self.canvas.table_items.items():
            if hasattr(it,"get_selected_columns"):
                scols.extend(it.get_selected_columns())
        if not scols:
            scols=["*"]
        lines= []
        lines.append("SELECT "+ ", ".join(scols))
        lines.append(self._build_bfs_from())
        wh= self.filter_panel.get_filters("WHERE")
        if wh:
            conds= [f"{a[0]} {a[1]} {a[2]}" for a in wh]
            lines.append("WHERE "+ " AND ".join(conds))
        return "\n".join(lines)

    def _get_dml_target(self):
        # find BFS item marked as is_dml_target
        for k,v in self.canvas.table_items.items():
            if hasattr(v,"is_dml_target") and v.is_dml_target:
                # parse => alias.db.tbl
                return k
        return None

    def _parse_mappings(self):
        # gather all mapping lines => source col => target col
        arr=[]
        for ml in self.canvas.mapping_lines:
            sc= ml.source_col
            tc= ml.target_col
            arr.append((sc,tc))
        return arr

    def _gen_insert(self):
        t= self._get_dml_target()
        if not t:
            return "-- No BFS item marked as target => no INSERT"
        mapped= self._parse_mappings()
        if not mapped:
            return "-- No col mappings => no INSERT"
        # Build subselect from non-target BFS items => select the source columns
        # We'll do a quick approach
        # But let's do a simpler approach => _gen_select_noagg
        # We'll remove references to target BFS item from BFS first
        lines= []
        # subselect
        sub= self._gen_select_noagg()
        # target columns
        tCols= [m[1].split(".")[-1] for m in mapped]
        alias= t.split(".")[0] # might not be needed
        db,tbl= None,None
        if "." in t:
            parts= t.split(".")
            if len(parts)>=3:
                db= parts[1]
                tbl= parts[2]
        if not db or not tbl:
            return "-- Could not parse target db.tbl => no INSERT"

        lines.append(f"INSERT INTO {db}.{tbl} ({', '.join(tCols)})")
        lines.append(sub)
        return "\n".join(lines)

    def _gen_update(self):
        t= self._get_dml_target()
        if not t:
            return "-- No BFS item is target => no UPDATE"
        mapped= self._parse_mappings()
        if not mapped:
            return "-- No mapping => no UPDATE"
        sub= self._gen_select_noagg()
        key_col="id"
        # parse => alias.db.tbl
        parts= t.split(".")
        if len(parts)<3:
            return "-- cannot parse target => no UPDATE"
        db= parts[1]
        tbl= parts[2]
        sets=[]
        for (src,tgt) in mapped:
            if tgt.lower() != key_col.lower():
                sets.append(f"{tgt}=src.{src}")
        lines=[]
        lines.append(f"UPDATE {db}.{tbl}")
        lines.append(f"SET {', '.join(sets)}")
        lines.append("FROM (")
        lines.append(sub)
        lines.append(") AS src")
        lines.append(f"WHERE {db}.{tbl}.{key_col}=src.{key_col}")
        return "\n".join(lines)

    def _gen_delete(self):
        t= self._get_dml_target()
        if not t:
            return "-- No BFS item is target => no DELETE"
        # We'll do "DELETE FROM db.tbl WHERE key in (subselect)"
        sub= self._gen_select_noagg()
        key_col="id"
        parts= t.split(".")
        if len(parts)<3:
            return "-- cannot parse target => no DELETE"
        db= parts[1]
        tbl= parts[2]
        lines=[]
        lines.append(f"DELETE FROM {db}.{tbl}")
        lines.append(f"WHERE {key_col} IN (")
        lines.append(sub)
        lines.append(")")
        return "\n".join(lines)

###############################################################################
# The main window
###############################################################################
class MainVQBWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Full VQB - BFS, DML, Expression Builder, etc.")
        self.resize(1200,800)

        self.connections={}
        self.linked_map={}
        self.builder_tab= VisualQueryBuilderTab(
            multi_connections=self.connections,
            linked_map=self.linked_map
        )
        self.setCentralWidget(self.builder_tab)
        self.init_toolbar()

    def init_toolbar(self):
        tb= self.addToolBar("MainToolBar")
        conn_a= QAction("Connections", self)
        conn_a.triggered.connect(self.on_manage_conn)
        tb.addAction(conn_a)

        lnk_a= QAction("Linked Server Config", self)
        lnk_a.triggered.connect(self.on_link_cfg)
        tb.addAction(lnk_a)

        fit_a= QAction("Fit to View", self)
        fit_a.triggered.connect(self.on_fit_view)
        tb.addAction(fit_a)

        layout_a= QAction("Auto-Layout BFS", self)
        layout_a.triggered.connect(self.on_auto_layout)
        tb.addAction(layout_a)

        # Demo BFS lines are not needed

    def on_manage_conn(self):
        d= MultiODBCConnectDialog(self.connections, self)
        if d.exec_()== QDialog.Accepted:
            self.connections= d.get_connections()
            self.builder_tab.set_connections(self.connections)

    def on_link_cfg(self):
        d= LinkedServerConfigDialog(self.linked_map, self)
        if d.exec_()== QDialog.Accepted:
            self.linked_map= d.get_map()
            self.builder_tab.set_federation_map(self.linked_map)
            QMessageBox.information(self,"Linked Config","Cross-DB rewriting is updated.")

    def on_fit_view(self):
        sc= self.builder_tab.canvas.scene_
        self.builder_tab.canvas.fitInView(sc.itemsBoundingRect(), Qt.KeepAspectRatio)

    def on_auto_layout(self):
        items= list(self.builder_tab.canvas.table_items.values())
        col_count=3
        xsp=250
        ysp=180
        for i,it in enumerate(items):
            row= i//col_count
            col= i%col_count
            it.setPos(col*xsp, row*ysp)
        # update lines
        # BFS lines
        for ln in self.builder_tab.canvas.scene_.items():
            if isinstance(ln, JoinLine):
                ln.update_line()
            if isinstance(ln, MappingLine):
                ln.update_pos()


def main():
    app= QApplication(sys.argv)
    apply_fusion_style()
    w= MainVQBWindow()
    w.show()
    sys.exit(app.exec_())

if __name__=="__main__":
    main()