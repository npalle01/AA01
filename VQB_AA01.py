#!/usr/bin/env python3
# final_vqb_no_gaps.py
#
# A single-file Python/PyQt advanced Visual Query Builder for Teradata, featuring:
#  - ODBC connection (no repeated reconnects)
#  - Lazy schema (DB->Tables->Columns) with BFS multi-join, auto-FK, manual join
#  - Collapsible table items (checkable columns) + sticky lines
#  - Nested subquery items (double-click -> sub-VQB)
#  - Combine Query (UNION, etc.) reusing connection
#  - DML placeholders (INSERT/UPDATE/DELETE) + red partition line
#       + “Complex Query” rectangle for BFS columns + target table + sticky mapping lines
#  - Filter (WHERE/HAVING), GroupBy + pivot wizard, Sort/Limit
#  - Advanced expression builder (multi-line text, subquery tokens, CASE wizard, snippets)
#  - Run SQL => fetch up to 10 rows for user to preview
#  - Import SQL => parse with sqlparse
#  - Single file, no “same approach” placeholders or references

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
    QListWidget, QCheckBox
)

###############################################################################
# LOGGING & ODBC
###############################################################################
logging.basicConfig(
    filename="vqb.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG
)
pyodbc.pooling = True

###############################################################################
# 1) Apply Fusion style
###############################################################################
def apply_fusion_style():
    QApplication.setStyle("Fusion")
    pal=QPalette()
    pal.setColor(QPalette.Window, QColor(240,240,240))
    pal.setColor(QPalette.WindowText, Qt.black)
    pal.setColor(QPalette.Base, QColor(255,255,255))
    pal.setColor(QPalette.AlternateBase, QColor(225,225,225))
    pal.setColor(QPalette.ToolTipBase, Qt.yellow)
    pal.setColor(QPalette.ToolTipText, Qt.black)
    pal.setColor(QPalette.Button, QColor(230,230,230))
    pal.setColor(QPalette.ButtonText, Qt.black)
    pal.setColor(QPalette.Highlight, QColor(76,163,224))
    pal.setColor(QPalette.HighlightedText, Qt.white)
    QApplication.setPalette(pal)

    style_sheet="""
        QCheckBox::indicator, QRadioButton::indicator {
            width:12px;
            height:12px;
            spacing:2px;
        }
    """
    QApplication.instance().setStyleSheet(style_sheet)

###############################################################################
# 2) ODBCConnectDialog
###############################################################################
class ODBCConnectDialog(QDialog):
    """
    Connect to Teradata DSN with optional user/pass.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._conn=None
        self._db_type=None
        self.setWindowTitle("Connect to Teradata (ODBC)")
        self.resize(400,230)

        lay=QVBoxLayout(self)
        lay.addWidget(QLabel("Database Type (Fixed to Teradata):"))
        self.dbtype_lab=QLabel("Teradata")
        lay.addWidget(self.dbtype_lab)

        lay.addWidget(QLabel("ODBC DSN (Teradata Only):"))
        self.dsn_cb=QComboBox()
        try:
            dsn_map=pyodbc.dataSources()
            for dsn in sorted(dsn_map.keys()):
                self.dsn_cb.addItem(dsn)
        except:
            pass
        lay.addWidget(self.dsn_cb)

        lay.addWidget(QLabel("Username (optional):"))
        self.user_ed=QLineEdit()
        lay.addWidget(self.user_ed)

        lay.addWidget(QLabel("Password (optional):"))
        self.pass_ed=QLineEdit()
        self.pass_ed.setEchoMode(QLineEdit.Password)
        lay.addWidget(self.pass_ed)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        lay.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(lay)

    def on_ok(self):
        dsn=self.dsn_cb.currentText().strip()
        if not dsn:
            QMessageBox.warning(self,"No DSN","Please pick a DSN.")
            return
        conn_str=f"DSN={dsn};"
        user=self.user_ed.text().strip()
        pwd=self.pass_ed.text().strip()
        if user:
            conn_str+=f"UID={user};"
        if pwd:
            conn_str+=f"PWD={pwd};"
        try:
            cn=pyodbc.connect(conn_str,autocommit=True)
            self._conn=cn
            self._db_type="Teradata"
            self.accept()
        except Exception as ex:
            QMessageBox.critical(self,"Connect Error",f"Failed:\n{ex}")

    def get_connection(self):
        return self._conn
    def get_db_type(self):
        return self._db_type

###############################################################################
# 3) Lazy schema loading & foreign keys
###############################################################################
class LazySchemaWorkerSignals(QObject):
    finished=pyqtSignal(list)
    error=pyqtSignal(str)

class LazySchemaWorker(QRunnable):
    def __init__(self, conn, dbName):
        super().__init__()
        self.conn=conn
        self.dbName=dbName
        self.signals=LazySchemaWorkerSignals()

    @QtCore.pyqtSlot()
    def run(self):
        try:
            c=self.conn.cursor()
            c.execute(f"""
            SELECT TableName
            FROM DBC.TablesV
            WHERE DatabaseName='{self.dbName}' AND TableKind='T'
            ORDER BY TableName
            """)
            rows=c.fetchall()
            tbls=[r[0] for r in rows]
            self.signals.finished.emit(tbls)
        except Exception as ex:
            msg=f"Error loading tables for {self.dbName}: {ex}\n{traceback.format_exc()}"
            self.signals.error.emit(msg)

def load_foreign_keys(connection):
    fk_map={}
    try:
        cu=connection.cursor()
        cu.execute("""
        SELECT 
            ChildDatabaseName, ChildTableName, ChildKeyColumnName,
            ParentDatabaseName, ParentTableName, ParentKeyColumnName
        FROM DBC.All_RI_Children
        """)
        rows=cu.fetchall()
        for r in rows:
            cd=r.ChildDatabaseName.strip()
            ct=r.ChildTableName.strip()
            cc=r.ChildKeyColumnName.strip()
            pd=r.ParentDatabaseName.strip()
            pt=r.ParentTableName.strip()
            pc=r.ParentKeyColumnName.strip()
            child_key=f"{cd}.{ct}.{cc}"
            parent_key=f"{pd}.{pt}.{pc}"
            fk_map[child_key]=parent_key
    except:
        pass
    return fk_map

def load_columns_for_table(conn, dbN, tblN):
    cols=[]
    try:
        c=conn.cursor()
        c.execute(f"""
        SELECT ColumnName
        FROM DBC.ColumnsV
        WHERE DatabaseName='{dbN}' AND TableName='{tblN}'
        ORDER BY ColumnId
        """)
        rows=c.fetchall()
        cols=[r[0] for r in rows]
    except:
        pass
    return cols

###############################################################################
# 4) LazySchemaTreeWidget
###############################################################################
class LazySchemaTreeWidget(QTreeWidget):
    def __init__(self, conn, parent_builder=None, parent=None):
        super().__init__(parent)
        self.conn=conn
        self.parent_builder=parent_builder
        self.setHeaderHidden(True)
        self.setDragEnabled(True)
        self.threadpool=QThreadPool.globalInstance()
        self.populate_top()

    def populate_top(self):
        self.clear()
        root_txt="Not Connected"
        if self.conn:
            try:
                dbms=self.conn.getinfo(pyodbc.SQL_DBMS_NAME).strip()
                if "TERADATA" in dbms.upper():
                    root_txt=dbms
            except:
                pass
        root_item=QTreeWidgetItem([root_txt])
        root_item.setData(0,Qt.UserRole,"conn")
        self.addTopLevelItem(root_item)

        if not self.conn:
            return

        db_list=[]
        try:
            c=self.conn.cursor()
            c.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY DatabaseName")
            rows=c.fetchall()
            db_list=[r[0] for r in rows]
        except Exception as ex:
            QMessageBox.warning(self,"Error",f"Fetching DB list failed:\n{ex}")

        if not db_list:
            root_item.addChild(QTreeWidgetItem(["<No DB>"]))
            return

        for dbn in db_list:
            db_item=QTreeWidgetItem([dbn])
            db_item.setData(0,Qt.UserRole,"db")
            db_item.setData(0,Qt.UserRole+1,False)
            db_item.addChild(QTreeWidgetItem(["Loading..."]))
            root_item.addChild(db_item)

        self.expandItem(root_item)

    def mouseDoubleClickEvent(self,e):
        it=self.itemAt(e.pos())
        if it:
            self.try_expand_item(it)
        super().mouseDoubleClickEvent(e)

    def try_expand_item(self, it):
        dt=it.data(0,Qt.UserRole)
        loaded=it.data(0,Qt.UserRole+1)
        if dt=="db" and not loaded:
            it.takeChildren()
            dbn=it.text(0)
            worker=LazySchemaWorker(self.conn, dbn)
            def on_finish(tbls):
                self.populate_db_node(it,tbls)
            def on_error(msg):
                QMessageBox.critical(self,"Schema Error",msg)
            worker.signals.finished.connect(on_finish)
            worker.signals.error.connect(on_error)
            self.threadpool.start(worker)
        elif dt=="table" and not loaded:
            it.takeChildren()
            dbN=it.parent().text(0)
            tblN=it.text(0)
            cols=load_columns_for_table(self.conn, dbN,tblN)
            if cols:
                for cc in cols:
                    c_item=QTreeWidgetItem([cc])
                    c_item.setData(0,Qt.UserRole,"column")
                    it.addChild(c_item)
            else:
                it.addChild(QTreeWidgetItem(["<No columns>"]))
            it.setData(0,Qt.UserRole+1,True)

    def populate_db_node(self, db_item, tables):
        if not tables:
            db_item.addChild(QTreeWidgetItem(["<No tables>"]))
            db_item.setData(0,Qt.UserRole+1,True)
            return
        db_item.takeChildren()
        for t in tables:
            t_item=QTreeWidgetItem([t])
            t_item.setData(0,Qt.UserRole,"table")
            t_item.setData(0,Qt.UserRole+1,False)
            t_item.addChild(QTreeWidgetItem(["Loading..."]))
            db_item.addChild(t_item)
        db_item.setData(0,Qt.UserRole+1,True)

    def startDrag(self, actions):
        it=self.currentItem()
        if it and it.parent() and it.data(0,Qt.UserRole)=="table":
            dbN=it.parent().text(0)
            tblN=it.text(0)
            full=f"{dbN}.{tblN}"
            drag=QtGui.QDrag(self)
            mime=QtCore.QMimeData()
            mime.setText(full)
            drag.setMimeData(mime)
            drag.exec_(actions)

###############################################################################
# 5) FullSQLParser & SyntaxHighlighter
###############################################################################
class FullSQLParser:
    def __init__(self, sql):
        self.sql=sql
    def parse(self):
        st=sqlparse.parse(self.sql)
        if not st:
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
            "NTILE","LAG","LEAD","CASE","COALESCE","TRIM"
        ]
        for w in keywords:
            pat=QRegularExpression(r'\b'+w+r'\b', QRegularExpression.CaseInsensitiveOption)
            self.rules.append((pat,kwfmt))

        strfmt=QTextCharFormat()
        strfmt.setForeground(Qt.darkRed)
        self.rules.append((QRegularExpression(r"'[^']*'"),strfmt))
        self.rules.append((QRegularExpression(r'"[^"]*"'),strfmt))

        cfmt=QTextCharFormat()
        cfmt.setForeground(Qt.green)
        self.rules.append((QRegularExpression(r'--[^\n]*'),cfmt))
        self.rules.append((QRegularExpression(r'/\*.*\*/',QRegularExpression.DotMatchesEverythingOption),cfmt))

    def highlightBlock(self,text):
        for pat,fmt in self.rules:
            matches=pat.globalMatch(text)
            while matches.hasNext():
                m=matches.next()
                st=m.capturedStart()
                ln=m.capturedLength()
                self.setFormat(st,ln,fmt)
        self.setCurrentBlockState(0)

###############################################################################
# 6) MappingLine & JoinLine (sticky) with context menus
###############################################################################
class MappingLine(QGraphicsLineItem):
    def __init__(self, source_text_item, target_text_item, src_type=None, tgt_type=None):
        super().__init__()
        self.source_text_item=source_text_item
        self.target_text_item=target_text_item
        self.source_col=source_text_item.toPlainText()
        self.target_col=target_text_item.toPlainText()
        self.source_type=src_type
        self.target_type=tgt_type
        self.setPen(QPen(Qt.darkRed,2,Qt.SolidLine))
        self.setZValue(5)
        self.setAcceptHoverEvents(True)
        self.update_pos()

    def update_pos(self):
        s=self.source_text_item.mapToScene(self.source_text_item.boundingRect().center())
        t=self.target_text_item.mapToScene(self.target_text_item.boundingRect().center())
        self.setLine(QtCore.QLineF(s,t))

    def paint(self,painter,option,widget):
        self.update_pos()
        super().paint(painter,option,widget)

    def contextMenuEvent(self,event):
        menu=QMenu()
        rm=menu.addAction("Remove Mapping")
        chosen=menu.exec_(event.screenPos())
        if chosen==rm:
            sc=self.scene()
            if sc:
                sc.removeItem(self)

class JoinLine(QGraphicsLineItem):
    def __init__(self, start_item, end_item, join_type="INNER", condition=""):
        super().__init__()
        self.start_item=start_item
        self.end_item=end_item
        self.join_type=join_type
        self.condition=condition
        self.setZValue(-1)
        self.setAcceptHoverEvents(True)

        self.pen_map={
            "INNER":(Qt.darkBlue,Qt.SolidLine),
            "LEFT":(Qt.darkGreen,Qt.SolidLine),
            "RIGHT":(Qt.magenta,Qt.DotLine),
            "FULL":(Qt.red,Qt.DashLine)
        }
        self.label=QGraphicsTextItem(self.join_type,self)
        self.label.setDefaultTextColor(Qt.blue)
        self.update_line()

    def update_line(self):
        s=self.start_item.scenePos()+QPointF(100,30)
        e=self.end_item.scenePos()+QPointF(100,30)
        self.setLine(QtCore.QLineF(s,e))
        mx=(s.x()+e.x())/2
        my=(s.y()+e.y())/2
        self.label.setPos(mx,my)
        c,style=self.pen_map.get(self.join_type,(Qt.gray,Qt.SolidLine))
        self.setPen(QPen(c,2,style))

    def hoverEnterEvent(self,e):
        p=self.pen()
        p.setColor(Qt.yellow)
        p.setWidth(3)
        self.setPen(p)
        super().hoverEnterEvent(e)

    def hoverLeaveEvent(self,e):
        self.update_line()
        super().hoverLeaveEvent(e)

    def contextMenuEvent(self, event):
        menu=QMenu()
        rm=menu.addAction("Remove Join")
        chosen=menu.exec_(event.screenPos())
        if chosen==rm:
            sc=self.scene()
            if sc:
                sc.removeItem(self)

###############################################################################
# 7) ManualJoinDialog, CollapsibleTableGraphicsItem
###############################################################################
class ManualJoinDialog(QDialog):
    def __init__(self, current_table_key, other_table_keys, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manual Join")
        self.resize(400,200)
        self.current_table_key=current_table_key
        self.other_table_keys=other_table_keys
        self.selected_join_type="INNER"
        self.selected_other=""
        self.join_condition=""

        lay=QVBoxLayout(self)
        form=QFormLayout()

        self.join_cb=QComboBox()
        self.join_cb.addItems(["INNER","LEFT","RIGHT","FULL"])
        form.addRow("Join Type:",self.join_cb)

        self.other_cb=QComboBox()
        self.other_cb.addItems(other_table_keys)
        form.addRow("Other Table:",self.other_cb)

        self.cond_ed=QLineEdit(f"{current_table_key}.id = ???")
        form.addRow("Condition:",self.cond_ed)

        lay.addLayout(form)
        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        lay.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(lay)

    def on_ok(self):
        self.selected_join_type=self.join_cb.currentText()
        self.selected_other=self.other_cb.currentText()
        self.join_condition=self.cond_ed.text().strip()
        if not self.join_condition:
            QMessageBox.warning(self,"No Condition","Join condition is empty.")
            return
        self.accept()

    def get_result(self):
        return (self.selected_join_type,self.selected_other,self.join_condition)

class CollapsibleTableGraphicsItem(QGraphicsRectItem):
    def __init__(self, table_fullname, columns, parent_builder, x=0,y=0):
        super().__init__(0,0,220,40)
        self.setPos(x,y)
        self.setBrush(QBrush(QColor(220,220,255)))
        self.setPen(QPen(Qt.darkGray,2))
        self.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)

        self.table_fullname=table_fullname
        self.columns=columns
        self.parent_builder=parent_builder
        self.is_collapsed=True
        self.title_height=20
        self.column_items=[]  # [rect, text, isChecked]

        self.close_btn=QGraphicsTextItem("[X]",self)
        self.close_btn.setPos(190,2)
        self.close_btn.setDefaultTextColor(Qt.red)

        self.toggle_btn=QGraphicsTextItem("[+]",self)
        self.toggle_btn.setPos(165,2)
        self.toggle_btn.setDefaultTextColor(Qt.blue)

        f=QFont("Arial",9,QFont.Bold)
        self.title_text=QGraphicsTextItem(table_fullname,self)
        self.title_text.setFont(f)
        self.title_text.setPos(5,2)

        yOff=self.title_height
        for ccc in columns:
            r=QGraphicsRectItem(5,yOff+4,10,10,self)
            r.setBrush(QBrush(Qt.white))
            r.setPen(QPen(Qt.black,1))
            t=QGraphicsTextItem(ccc,self)
            t.setPos(20,yOff)
            self.column_items.append([r,t,False])
            yOff+=20

        self.update_layout()

    def update_layout(self):
        if self.is_collapsed:
            self.setRect(0,0,220,self.title_height)
            for (cb,ct,chk) in self.column_items:
                cb.setVisible(False)
                ct.setVisible(False)
            self.toggle_btn.setPlainText("[+]")
        else:
            expanded=self.title_height+len(self.column_items)*20
            self.setRect(0,0,220,expanded)
            for (cb,ct,chk) in self.column_items:
                cb.setVisible(True)
                ct.setVisible(True)
            self.toggle_btn.setPlainText("[-]")

    def mousePressEvent(self,event):
        pos=event.pos()
        cbr=self.close_btn.mapToParent(self.close_btn.boundingRect())
        if cbr.boundingRect().contains(pos):
            sc=self.scene()
            if sc:
                self.parent_builder.handle_remove_table(self)
                sc.removeItem(self)
            event.accept()
            return

        tbr=self.toggle_btn.mapToParent(self.toggle_btn.boundingRect())
        if tbr.boundingRect().contains(pos):
            self.is_collapsed=not self.is_collapsed
            self.update_layout()
            event.accept()
            return

        for i,(cb,ct,chk) in enumerate(self.column_items):
            rr=cb.mapToParent(cb.boundingRect()).boundingRect()
            if rr.contains(pos):
                self.column_items[i][2]=not chk
                if self.column_items[i][2]:
                    cb.setBrush(QBrush(Qt.blue))
                else:
                    cb.setBrush(QBrush(Qt.white))
                if self.parent_builder.auto_generate:
                    self.parent_builder.generate_sql()
                event.accept()
                return
        super().mousePressEvent(event)

    def contextMenuEvent(self,event):
        menu=QMenu()
        rm_act=menu.addAction("Remove Table")
        join_act=menu.addAction("Add Manual Join")
        chosen=menu.exec_(event.screenPos())
        if chosen==rm_act:
            sc=self.scene()
            if sc:
                self.parent_builder.handle_remove_table(self)
                sc.removeItem(self)
        elif chosen==join_act:
            tabmap=self.parent_builder.canvas.table_items
            mykey=None
            for k,v in tabmap.items():
                if v==self:
                    mykey=k
                    break
            if not mykey:
                return
            other_keys=[x for x in tabmap.keys() if x!=mykey and not x.startswith("SubQueryItem_")]
            if not other_keys:
                QMessageBox.information(None,"No Other Tables","No other tables on canvas to join with.")
                return
            dlg=ManualJoinDialog(mykey,other_keys)
            if dlg.exec_()==QDialog.Accepted:
                jt,oth,cond=dlg.get_result()
                if oth in tabmap:
                    other_item=tabmap[oth]
                    jl=JoinLine(self,other_item,jt,cond)
                    self.scene().addItem(jl)
                    self.parent_builder.canvas.join_lines.append(jl)
                    jl.update_line()
                else:
                    QMessageBox.warning(None,"Not found",f"Can't find {oth} on canvas.")

    def get_selected_columns(self):
        arr=[]
        for (cb,ct,chk) in self.column_items:
            if chk:
                colName=ct.toPlainText().strip()
                arr.append(f"{self.table_fullname}.{colName}")
        return arr

    def itemChange(self, change, value):
        if change==QGraphicsItem.ItemPositionHasChanged:
            sc=self.scene()
            if sc and hasattr(sc,"update_lines_for_item"):
                sc.update_lines_for_item(self)
        return super().itemChange(change,value)

###############################################################################
# 8) SubVQBDialog, NestedSubqueryItem
###############################################################################
class SubVQBDialog(QDialog):
    def __init__(self, parent_vqb=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Build Second Query (Full VQB)")
        self.resize(900,600)
        self.operator="UNION"
        self.second_sql=""
        self.parent_vqb=parent_vqb

        lay=QVBoxLayout(self)

        op_h=QHBoxLayout()
        op_h.addWidget(QLabel("Combine Operator:"))
        self.op_combo=QComboBox()
        self.op_combo.addItems(["UNION","UNION ALL","INTERSECT","EXCEPT"])
        op_h.addWidget(self.op_combo)
        op_h.addStretch()
        lay.addLayout(op_h)

        from_vqb=VisualQueryBuilderTab()
        self.sub_vqb=from_vqb
        lay.addWidget(from_vqb)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        lay.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)

        self.setLayout(lay)

        if parent_vqb and hasattr(parent_vqb,"connections"):
            self.sub_vqb.set_connections(parent_vqb.connections)

    def on_ok(self):
        self.operator=self.op_combo.currentText()
        self.second_sql=self.sub_vqb.sql_display.toPlainText().strip()
        if not self.second_sql:
            QMessageBox.warning(self,"No SQL","Second query is empty.")
            return
        self.accept()

    def getResult(self):
        return (self.operator,self.second_sql)

class NestedVQBDialog(QDialog):
    def __init__(self, existing_sql="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Nested SubVQB")
        self.resize(900,600)
        self.existing_sql=existing_sql
        main=QVBoxLayout(self)

        self.sub_vqb=VisualQueryBuilderTab()
        main.addWidget(self.sub_vqb)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        main.addWidget(btns)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        self.setLayout(main)

    def set_connections(self, conns):
        if self.sub_vqb:
            self.sub_vqb.set_connections(conns)

    def get_built_sql(self):
        if self.sub_vqb:
            return self.sub_vqb.sql_display.toPlainText().strip()
        return ""

class NestedSubqueryItem(QGraphicsRectItem):
    def __init__(self, parent_builder=None, x=0, y=0):
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
        self.parent_builder=parent_builder

    def mouseDoubleClickEvent(self,event):
        dlg=NestedVQBDialog(self.query_text)
        if self.parent_builder and hasattr(self.parent_builder,"connections"):
            dlg.set_connections(self.parent_builder.connections)
        if dlg.exec_()==QDialog.Accepted:
            new_sql=dlg.get_built_sql()
            if new_sql:
                self.query_text=new_sql
                self.label.setPlainText("Nested SubQuery\n(VQB built)")
        event.accept()

    def contextMenuEvent(self,event):
        menu=QMenu()
        rmAct=menu.addAction("Remove SubQuery")
        chosen=menu.exec_(event.screenPos())
        if chosen==rmAct:
            sc=self.scene()
            if sc:
                sc.removeItem(self)

    def get_sql(self):
        return self.query_text

    def itemChange(self, change, value):
        if change==QGraphicsItem.ItemPositionHasChanged:
            sc=self.scene()
            if sc and hasattr(sc,"update_lines_for_item"):
                sc.update_lines_for_item(self)
        return super().itemChange(change,value)

###############################################################################
# 9) Expression Builder
###############################################################################
class CaseWizardDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("CASE Wizard")
        self.available_columns=available_columns
        self.conditions=[]
        self.else_result=""

        main=QVBoxLayout(self)
        top_h=QHBoxLayout()
        add_btn=QPushButton("Add Condition")
        add_btn.clicked.connect(self.add_condition)
        rm_btn=QPushButton("Remove Condition")
        rm_btn.clicked.connect(self.remove_condition)
        top_h.addWidget(add_btn)
        top_h.addWidget(rm_btn)
        main.addLayout(top_h)

        self.table=QTableWidget(0,4)
        self.table.setHorizontalHeaderLabels(["Column","Operator","Value","Result"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        main.addWidget(self.table)

        else_h=QHBoxLayout()
        else_h.addWidget(QLabel("ELSE result:"))
        self.else_edit=QLineEdit()
        else_h.addWidget(self.else_edit)
        main.addLayout(else_h)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        main.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(main)

    def add_condition(self):
        d=QDialog(self)
        d.setWindowTitle("Add CASE WHEN")
        fl=QFormLayout(d)
        col_cb=QComboBox()
        col_cb.addItems(self.available_columns)
        op_cb=QComboBox()
        op_cb.addItems(["=","<>","<",">","<=",">=","LIKE"])
        val_ed=QLineEdit("'ABC'")
        res_ed=QLineEdit("'ValIfTrue'")
        fl.addRow("Column:",col_cb)
        fl.addRow("Operator:",op_cb)
        fl.addRow("Value:",val_ed)
        fl.addRow("Result:",res_ed)
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
            o=op_cb.currentText()
            v=val_ed.text().strip()
            r=res_ed.text().strip()
            rr=self.table.rowCount()
            self.table.insertRow(rr)
            self.table.setItem(rr,0,QTableWidgetItem(c))
            self.table.setItem(rr,1,QTableWidgetItem(o))
            self.table.setItem(rr,2,QTableWidgetItem(v))
            self.table.setItem(rr,3,QTableWidgetItem(r))

    def remove_condition(self):
        rows=sorted([x.row() for x in self.table.selectionModel().selectedRows()],reverse=True)
        for rr in rows:
            self.table.removeRow(rr)

    def on_ok(self):
        self.else_result=self.else_edit.text().strip()
        self.accept()

    def build_expression(self):
        lines=["CASE"]
        for r in range(self.table.rowCount()):
            c=self.table.item(r,0).text()
            o=self.table.item(r,1).text()
            v=self.table.item(r,2).text()
            re=self.table.item(r,3).text()
            lines.append(f"  WHEN {c}{o}{v} THEN {re}")
        if self.else_result:
            lines.append(f"  ELSE {self.else_result}")
        lines.append("END")
        return "\n".join(lines)

class AdvancedExpressionBuilderDialog(QDialog):
    def __init__(self, available_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Advanced Expression Builder")
        self.available_cols=available_columns
        self.expr_text=""
        self.alias="ExprAlias"

        main=QVBoxLayout(self)

        snippet_h=QHBoxLayout()
        snippet_h.addWidget(QLabel("Snippets:"))
        self.snippet_cb=QComboBox()
        self.snippet_cb.addItems(["(Pick Snippet)",
                                  "DATE_DIFF example",
                                  "Rolling 90 day",
                                  "COALESCE sample"])
        snippet_btn=QPushButton("Insert Snippet")
        snippet_btn.clicked.connect(self.insert_snippet)
        snippet_h.addWidget(self.snippet_cb)
        snippet_h.addWidget(snippet_btn)
        snippet_h.addStretch()
        main.addLayout(snippet_h)

        self.expr_edit=QTextEdit()
        self.expr_edit.setPlaceholderText("Build or type your expression here.")
        main.addWidget(self.expr_edit)

        row_ops=QHBoxLayout()

        col_cb=QComboBox()
        col_cb.addItems(["(Pick Col)"]+self.available_cols)
        col_btn=QPushButton("Insert Col")
        def add_col():
            c=col_cb.currentText()
            if c and c!="(Pick Col)":
                self.insert_text(c+" ")
        col_btn.clicked.connect(add_col)
        row_ops.addWidget(col_cb)
        row_ops.addWidget(col_btn)

        op_cb=QComboBox()
        op_cb.addItems(["+","-","*","/","=","<>","<",">","<=",">=","AND","OR","LIKE","BETWEEN"])
        op_btn=QPushButton("Insert Op")
        def add_op():
            o=op_cb.currentText()
            self.insert_text(o+" ")
        op_btn.clicked.connect(add_op)
        row_ops.addWidget(op_cb)
        row_ops.addWidget(op_btn)

        func_cb=QComboBox()
        func_cb.addItems(["(Pick Func)",
                          "UPPER","LOWER","ABS","COALESCE","SUBSTR","TRIM","CURRENT_DATE","CURRENT_TIMESTAMP"])
        func_btn=QPushButton("Func")
        def add_func():
            fn=func_cb.currentText()
            if fn and fn!="(Pick Func)":
                self.insert_text(fn+"() ")
        func_btn.clicked.connect(add_func)
        row_ops.addWidget(func_cb)
        row_ops.addWidget(func_btn)

        subq_btn=QPushButton("SubQuery")
        subq_btn.clicked.connect(self.add_subquery)
        row_ops.addWidget(subq_btn)

        case_btn=QPushButton("Case Wizard")
        case_btn.clicked.connect(self.launch_case_wizard)
        row_ops.addWidget(case_btn)

        main.addLayout(row_ops)

        alias_h=QHBoxLayout()
        alias_h.addWidget(QLabel("Alias:"))
        self.alias_ed=QLineEdit("ExprAlias")
        alias_h.addWidget(self.alias_ed)
        main.addLayout(alias_h)

        btns=QDialogButtonBox(QDialogButtonBox.Ok|QDialogButtonBox.Cancel)
        main.addWidget(btns)
        btns.accepted.connect(self.on_ok)
        btns.rejected.connect(self.reject)
        self.setLayout(main)

    def insert_snippet(self):
        choice=self.snippet_cb.currentText()
        snippet_map={
            "DATE_DIFF example":"DATEDIFF(day, colDate, CURRENT_DATE)",
            "Rolling 90 day":"CASE WHEN colDate >= CURRENT_DATE - INTERVAL '90' DAY THEN 1 ELSE 0 END",
            "COALESCE sample":"COALESCE(colA, colB, 'default')"
        }
        if choice in snippet_map:
            snippet=snippet_map[choice]
            self.insert_text(snippet+" ")

    def insert_text(self, txt):
        cur=self.expr_edit.textCursor()
        cur.insertText(txt)
        self.expr_edit.setTextCursor(cur)

    def add_subquery(self):
        d=SubVQBDialog()
        if d.exec_()==QDialog.Accepted:
            op, ssql=d.getResult()
            if ssql:
                self.insert_text(f"({ssql}) ")

    def launch_case_wizard(self):
        d=CaseWizardDialog(self.available_cols, self)
        if d.exec_()==QDialog.Accepted:
            expr=d.build_expression()
            self.insert_text(expr+" ")

    def on_ok(self):
        raw=self.expr_edit.toPlainText().strip()
        if not raw:
            QMessageBox.warning(self,"No Expression","Expression empty.")
            return
        a=self.alias_ed.text().strip()
        if not a:
            QMessageBox.warning(self,"No Alias","Alias needed.")
            return
        self.expr_text=raw
        self.alias=a
        self.accept()

    def get_expression_data(self):
        return (self.alias,self.expr_text)

###############################################################################
# 10) FilterPanel, GroupByPanel + pivot, SortLimitPanel
###############################################################################
class FilterPanel(QGroupBox):
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
        self.where_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.where_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.where_layout.addWidget(self.where_table)

        wh_btn=QHBoxLayout()
        addW=QPushButton("Add WHERE")
        addW.clicked.connect(lambda: self.add_filter("WHERE"))
        rmW=QPushButton("Remove WHERE")
        rmW.clicked.connect(lambda: self.remove_filter("WHERE"))
        wh_btn.addWidget(addW)
        wh_btn.addWidget(rmW)
        self.where_layout.addLayout(wh_btn)

        # HAVING
        self.having_layout=QVBoxLayout(self.having_tab)
        self.having_table=QTableWidget(0,3)
        self.having_table.setHorizontalHeaderLabels(["Column","Operator","Value"])
        self.having_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.having_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.having_layout.addWidget(self.having_table)

        hv_btn=QHBoxLayout()
        addH=QPushButton("Add HAVING")
        addH.clicked.connect(lambda: self.add_filter("HAVING"))
        rmH=QPushButton("Remove HAVING")
        rmH.clicked.connect(lambda: self.remove_filter("HAVING"))
        hv_btn.addWidget(addH)
        hv_btn.addWidget(rmH)
        self.having_layout.addLayout(hv_btn)

    def add_filter(self, which):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No Columns","No columns available.")
            return
        d=QDialog(self)
        d.setWindowTitle(f"Add {which}")
        fl=QFormLayout(d)
        col_cb=QComboBox()
        col_cb.addItems(cols)
        op_cb=QComboBox()
        op_cb.addItems(["=","<>","<",">","<=",">=","IS NULL","IS NOT NULL","LIKE"])
        val_ed=QLineEdit("'ABC'")
        fl.addRow("Column:",col_cb)
        fl.addRow("Operator:",op_cb)
        fl.addRow("Value:",val_ed)
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
            o=op_cb.currentText()
            v=val_ed.text().strip()
            table=self.where_table if which=="WHERE" else self.having_table
            r=table.rowCount()
            table.insertRow(r)
            table.setItem(r,0,QTableWidgetItem(c))
            table.setItem(r,1,QTableWidgetItem(o))
            table.setItem(r,2,QTableWidgetItem(v))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_filter(self, which):
        table=self.where_table if which=="WHERE" else self.having_table
        rows=sorted([x.row() for x in table.selectionModel().selectedRows()],reverse=True)
        for rr in rows:
            table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_filters(self, which):
        table=self.where_table if which=="WHERE" else self.having_table
        res=[]
        for r in range(table.rowCount()):
            col=table.item(r,0).text()
            op=table.item(r,1).text()
            val=table.item(r,2).text()
            res.append((col,op,val))
        return res

class PivotDialog(QDialog):
    def __init__(self, available_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Pivot Wizard")
        self.category_col=""
        self.value_col=""
        self.distinct_vals=[]
        layout=QVBoxLayout(self)

        form=QFormLayout()
        self.cat_cb=QComboBox()
        self.cat_cb.addItems(available_cols)
        form.addRow("Category Column:", self.cat_cb)

        self.val_cb=QComboBox()
        self.val_cb.addItems(available_cols)
        form.addRow("Value Column:", self.val_cb)

        layout.addLayout(form)
        self.val_list=QListWidget()
        self.val_list.setSelectionMode(QAbstractItemView.MultiSelection)
        layout.addWidget(QLabel("Pick categories (demo)"))
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
        cat=self.cat_cb.currentText()
        val=self.val_cb.currentText()
        if not cat or not val:
            QMessageBox.warning(self,"PivotWizard","Must pick category & value col.")
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

        gb_h=QHBoxLayout()
        add_gb=QPushButton("Add GroupBy")
        add_gb.clicked.connect(self.add_group_by)
        rm_gb=QPushButton("Remove GroupBy")
        rm_gb.clicked.connect(self.remove_group_by)
        gb_h.addWidget(add_gb)
        gb_h.addWidget(rm_gb)
        layout.addLayout(gb_h)

        self.agg_table=QTableWidget(0,3)
        self.agg_table.setHorizontalHeaderLabels(["Function","Column","Alias"])
        self.agg_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.agg_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.agg_table)

        agg_h=QHBoxLayout()
        add_agg=QPushButton("Add Agg")
        add_agg.clicked.connect(self.add_agg)
        rm_agg=QPushButton("Remove Agg")
        rm_agg.clicked.connect(self.remove_agg)
        agg_h.addWidget(add_agg)
        agg_h.addWidget(rm_agg)
        layout.addLayout(agg_h)

        pivot_btn=QPushButton("Pivot Wizard")
        pivot_btn.clicked.connect(self.launch_pivot)
        layout.addWidget(pivot_btn)

    def add_group_by(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No Columns","No columns available.")
            return
        (c,ok)=QtWidgets.QInputDialog.getItem(self,"Add GroupBy","Pick column:",cols,0,False)
        if ok and c:
            rr=self.gb_table.rowCount()
            self.gb_table.insertRow(rr)
            self.gb_table.setItem(rr,0,QTableWidgetItem(c))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_group_by(self):
        rows=sorted([x.row() for x in self.gb_table.selectionModel().selectedRows()],reverse=True)
        for rr in rows:
            self.gb_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def add_agg(self):
        cols=self.builder.get_all_possible_columns_for_dialog()
        if not cols:
            QMessageBox.warning(self,"No Columns","No columns available.")
            return
        d=QDialog(self)
        d.setWindowTitle("Add Aggregate")
        fl=QFormLayout(d)
        func_cb=QComboBox()
        func_cb.addItems(["COUNT","SUM","AVG","MIN","MAX","CUSTOM"])
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
                QMessageBox.warning(d,"No col","Pick a column.")
                return
            d.accept()
        btns.accepted.connect(on_ok)
        btns.rejected.connect(d.reject)
        d.setLayout(fl)
        if d.exec_()==QDialog.Accepted:
            f=func_cb.currentText()
            c=col_cb.currentText()
            a=alias_ed.text().strip()
            rr=self.agg_table.rowCount()
            self.agg_table.insertRow(rr)
            self.agg_table.setItem(rr,0,QTableWidgetItem(f))
            self.agg_table.setItem(rr,1,QTableWidgetItem(c))
            self.agg_table.setItem(rr,2,QTableWidgetItem(a))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_agg(self):
        rows=sorted([x.row() for x in self.agg_table.selectionModel().selectedRows()],reverse=True)
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
                rr=self.agg_table.rowCount()
                self.agg_table.insertRow(rr)
                self.agg_table.setItem(rr,0,QTableWidgetItem("CUSTOM"))
                self.agg_table.setItem(rr,1,QTableWidgetItem(ex))
                self.agg_table.setItem(rr,2,QTableWidgetItem("PivotVal"))
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
    def __init__(self,builder,parent=None):
        super().__init__("Sort and Limit",parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        self.setLayout(layout)

        self.sort_table=QTableWidget(0,2)
        self.sort_table.setHorizontalHeaderLabels(["Column","Direction"])
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
            rr=self.sort_table.rowCount()
            self.sort_table.insertRow(rr)
            self.sort_table.setItem(rr,0,QTableWidgetItem(c))
            self.sort_table.setItem(rr,1,QTableWidgetItem(dd))
            if self.builder.auto_generate:
                self.builder.generate_sql()

    def remove_sort(self):
        rows=sorted([x.row() for x in self.sort_table.selectionModel().selectedRows()],reverse=True)
        for rr in rows:
            self.sort_table.removeRow(rr)
        if self.builder.auto_generate:
            self.builder.generate_sql()

    def get_order_bys(self):
        arr=[]
        for r in range(self.sort_table.rowCount()):
            cc=self.sort_table.item(r,0).text()
            dd=self.sort_table.item(r,1).text()
            arr.append(f"{cc} {dd}")
        return arr

    def get_limit(self):
        v=self.limit_spin.value()
        return v if v>0 else None

    def get_offset(self):
        v=self.offset_spin.value()
        return v if v>0 else None

###############################################################################
# 11) SQLImportTab
###############################################################################
class SQLImportTab(QWidget):
    def __init__(self,builder=None,parent=None):
        super().__init__(parent)
        self.builder=builder
        layout=QVBoxLayout(self)
        inst=QLabel("Paste or type your SQL below, then click 'Import'. We'll parse it.")
        layout.addWidget(inst)

        self.sql_edit=QTextEdit()
        layout.addWidget(self.sql_edit)

        btn=QPushButton("Import SQL")
        btn.clicked.connect(self.on_import)
        layout.addWidget(btn)

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
        except Exception as ex:
            QMessageBox.warning(self,"Import Error",f"{ex}")

###############################################################################
# 12) EnhancedCanvasGraphicsView
###############################################################################
class EnhancedCanvasGraphicsView(QGraphicsView):
    def __init__(self,builder,parent=None):
        super().__init__(parent)
        self.builder=builder
        self.scene_=QGraphicsScene(self)
        self.setScene(self.scene_)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QtWidgets.QGraphicsView.RubberBandDrag)

        self.table_items={}
        self.join_lines=[]
        self.mapping_lines=[]
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
        ns=c*z
        if self.min_scale<ns<self.max_scale:
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
        itm=CollapsibleTableGraphicsItem(table_name,columns,self.builder,x,y)
        self.scene_.addItem(itm)
        self.table_items[table_name]=itm
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def add_subquery_item(self, x, y):
        sq=NestedSubqueryItem(parent_builder=self.builder,x=x,y=y)
        self.scene_.addItem(sq)
        key=f"SubQueryItem_{id(sq)}"
        self.table_items[key]=sq
        self.validation_timer.start()

    def remove_table_item(self, table_key):
        if table_key in self.table_items:
            itm=self.table_items[table_key]
            lines_to_remove=[]
            for jl in self.join_lines:
                if jl.start_item==itm or jl.end_item==itm:
                    lines_to_remove.append(jl)
            for ln in lines_to_remove:
                self.scene_.removeItem(ln)
                self.join_lines.remove(ln)
            self.scene_.removeItem(itm)
            del self.table_items[table_key]
            self.validation_timer.start()

    def remove_mapping_lines(self):
        for ml in self.mapping_lines:
            self.scene_.removeItem(ml)
        self.mapping_lines.clear()

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
        ml=MappingLine(source_text_item,target_text_item,src_type,tgt_type)
        self.scene_.addItem(ml)
        self.mapping_lines.append(ml)
        if self.builder.auto_generate:
            self.builder.generate_sql()
        self.validation_timer.start()

    def mouseReleaseEvent(self,event):
        super().mouseReleaseEvent(event)
        for jl in self.join_lines:
            jl.update_line()
        for ml in self.mapping_lines:
            ml.update_pos()

    def update_lines_for_item(self, moved_item):
        for jl in self.join_lines:
            if jl.start_item==moved_item or jl.end_item==moved_item:
                jl.update_line()
        for ml in self.mapping_lines:
            srcp=ml.source_text_item.parentItem()
            tgtp=ml.target_text_item.parentItem()
            if srcp==moved_item or tgtp==moved_item:
                ml.update_pos()

###############################################################################
# 13) SubVQBDialog (already done above)
# 14) A small ResultDataDialog to show sample rows
###############################################################################
class ResultDataDialog(QDialog):
    def __init__(self, rows, columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("SQL Results (Sample Rows)")
        self.resize(700,400)
        main=QVBoxLayout(self)
        tbl=QTableWidget(len(rows), len(columns))
        tbl.setHorizontalHeaderLabels(columns)
        for r_idx, row_val in enumerate(rows):
            for c_idx,val in enumerate(row_val):
                it=QTableWidgetItem(str(val))
                tbl.setItem(r_idx,c_idx,it)
        main.addWidget(tbl)
        btns=QDialogButtonBox(QDialogButtonBox.Ok)
        btns.accepted.connect(self.accept)
        main.addWidget(btns)
        self.setLayout(main)

###############################################################################
# 15) The main VisualQueryBuilderTab
###############################################################################
class VisualQueryBuilderTab(QWidget):
    def __init__(self,parent=None):
        super().__init__(parent)
        self.connections={}
        self.fk_map={}
        self.table_columns_map={}
        self.auto_generate=True
        self.operation_mode="SELECT"
        self.threadpool=QThreadPool.globalInstance()

        self.init_ui()

    def init_ui(self):
        main=QVBoxLayout(self)

        # connection row
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

        self.auto_chk=QCheckBox("Auto-Generate")
        self.auto_chk.setChecked(True)
        self.auto_chk.stateChanged.connect(self.on_auto_gen_changed)
        conn_h.addWidget(self.auto_chk)

        conn_h.addStretch()
        main.addLayout(conn_h)

        # toolbar
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

        comb_btn=QPushButton("Combine Query (Full Sub VQB)")
        comb_btn.clicked.connect(self.combine_with_subvqb)
        tb_h.addWidget(comb_btn)

        self.op_combo=QComboBox()
        self.op_combo.addItems(["SELECT","INSERT","UPDATE","DELETE"])
        self.op_combo.currentIndexChanged.connect(self.on_op_mode_changed)
        tb_h.addWidget(self.op_combo)

        tb_h.addStretch()
        main.addLayout(tb_h)

        self.tabs=QTabWidget()
        main.addWidget(self.tabs)

        self.schema_tab=QWidget()
        self.config_tab=QWidget()
        self.sql_tab=QWidget()
        self.import_tab=SQLImportTab(builder=self)

        self.tabs.addTab(self.schema_tab,"Schema & Canvas")
        self.tabs.addTab(self.config_tab,"Query Config")
        self.tabs.addTab(self.sql_tab,"SQL Preview")
        self.tabs.addTab(self.import_tab,"SQL Import")

        self.status_bar=QStatusBar()
        main.addWidget(self.status_bar)
        self.setLayout(main)

        self.setup_schema_tab()
        self.setup_config_tab()
        self.setup_sql_tab()

    def set_connections(self, conns):
        self.connections=conns
        if conns:
            first_key=list(conns.keys())[0]
            self.load_schema(first_key)

    def open_connect_dialog(self):
        d=ODBCConnectDialog(self)
        if d.exec_()==QDialog.Accepted:
            c=d.get_connection()
            db_type=d.get_db_type()
            if c and db_type and db_type.upper()=="TERADATA":
                alias=f"{db_type}_{len(self.connections)+1}"
                self.connections[alias]={"connection":c}
                self.update_conn_status(True,f"{db_type} ({alias})")
                self.load_schema(alias)
                self.fk_map=load_foreign_keys(c)
            else:
                QMessageBox.warning(self,"Only Teradata","DSN restricted to Teradata")

    def update_conn_status(self, st, info=""):
        if st:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: green;}")
            self.server_label.setText(info)
        else:
            self.status_light.setStyleSheet("QFrame { border-radius:7px; background-color: red;}")
            self.server_label.setText("Not Connected")

    def load_schema(self, alias):
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

    def setup_schema_tab(self):
        lay=QVBoxLayout(self.schema_tab)
        self.search_ed=QLineEdit()
        self.search_ed.setPlaceholderText("Search tables/columns...")
        self.search_ed.textChanged.connect(self.on_schema_filter)
        lay.addWidget(self.search_ed)

        splitter=QSplitter(Qt.Horizontal)
        self.schema_tree=LazySchemaTreeWidget(None,parent_builder=self)
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
        self.group_panel=GroupByPanel(self)
        h.addWidget(self.group_panel,3)
        self.sort_panel=SortLimitPanel(self)
        h.addWidget(self.sort_panel,2)
        self.config_tab.setLayout(h)

    def setup_sql_tab(self):
        lay=QVBoxLayout(self.sql_tab)
        top_h=QHBoxLayout()
        top_h.addWidget(QLabel("Generated SQL:"))
        run_btn=QPushButton("Run SQL")
        run_btn.clicked.connect(self.run_sql)
        top_h.addWidget(run_btn,alignment=Qt.AlignRight)
        lay.addLayout(top_h)

        self.sql_display=QTextEdit()
        self.sql_display.setReadOnly(False)
        self.sql_highlighter=SQLHighlighter(self.sql_display.document())
        lay.addWidget(self.sql_display)

        self.validation_lbl=QLabel("SQL Status: Unknown")
        lay.addWidget(self.validation_lbl)
        self.sql_tab.setLayout(lay)

    def run_sql(self):
        sql=self.sql_display.toPlainText().strip()
        if not sql:
            QMessageBox.information(self,"Empty SQL","No SQL to run.")
            return
        if not self.connections:
            QMessageBox.information(self,"No Conn","No DB connection found.")
            return
        first_key=list(self.connections.keys())[0]
        conn=self.connections[first_key]["connection"]
        try:
            c=conn.cursor()
            if "limit" not in sql.lower():
                sql+="\nLIMIT 10"
            c.execute(sql)
            rows=c.fetchall()
            cols=[desc[0] for desc in c.description]
            rd=ResultDataDialog(rows,cols,self)
            rd.exec_()
        except Exception as ex:
            QMessageBox.warning(self,"SQL Error",f"Failed:\n{ex}")

    def on_schema_filter(self, txt):
        for i in range(self.schema_tree.topLevelItemCount()):
            it=self.schema_tree.topLevelItem(i)
            self._filter_item(it,txt)

    def _filter_item(self, it, txt):
        low=txt.lower()
        match=low in it.text(0).lower()
        child_match=False
        for c in range(it.childCount()):
            child_match=self._filter_item(it.child(c),txt) or child_match
        it.setHidden(not (match or child_match))
        return match or child_match

    def on_auto_gen_changed(self, st):
        self.auto_generate=(st==Qt.Checked)

    def on_op_mode_changed(self, idx):
        modes=["SELECT","INSERT","UPDATE","DELETE"]
        self.operation_mode=modes[idx]
        self.toggle_dml_canvas()
        if self.auto_generate:
            self.generate_sql()

    def add_subquery_to_canvas(self):
        self.canvas.add_subquery_item(200,200)
        if self.auto_generate:
            self.generate_sql()

    def combine_with_subvqb(self):
        d=SubVQBDialog(parent_vqb=self,parent=self)
        if d.exec_()==QDialog.Accepted:
            op,second_sql=d.getResult()
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
            a,exp=dlg.get_expression_data()
            old=self.sql_display.toPlainText()
            self.sql_display.setPlainText(old+f"\n-- Derived: {a}=\n{exp}")
            self.validate_sql()

    def launch_window_func(self):
        QMessageBox.information(self,"Window Function Wizard","Implement advanced wizard or use expression builder if needed.")

    def handle_drop(self, full_name, pos):
        if not self.connections:
            if full_name not in self.table_columns_map:
                self.table_columns_map[full_name]=["id","col1","col2"]
        else:
            if '.' in full_name:
                dbN,tblN=full_name.split('.',1)
                first_key=list(self.connections.keys())[0]
                conn=self.connections[first_key]["connection"]
                realCols=load_columns_for_table(conn,dbN,tblN)
                if not realCols:
                    realCols=["id","col1","col2"]
                self.table_columns_map[full_name]=realCols
            else:
                self.table_columns_map[full_name]=["id","col1","col2"]

        cols=self.table_columns_map[full_name]
        self.canvas.add_table_item(full_name, cols, pos.x(), pos.y())
        self.check_auto_fk(full_name)

    def handle_remove_table(self, table_item):
        for k,v in list(self.canvas.table_items.items()):
            if v==table_item:
                self.canvas.remove_table_item(k)
                break

    def check_auto_fk(self, table_key):
        if not self.fk_map:
            return
        item=self.canvas.table_items.get(table_key,None)
        if not item:
            return
        if not hasattr(item,"columns"):
            return
        col_list=item.columns
        for c in col_list:
            child_key=f"{table_key}.{c}"
            if child_key in self.fk_map:
                pk=self.fk_map[child_key]
                parent_tab=".".join(pk.split('.')[:2])
                pitem=self.canvas.table_items.get(parent_tab,None)
                if pitem:
                    jl=JoinLine(item,pitem,"LEFT",f"{child_key}={pk}")
                    self.canvas.scene_.addItem(jl)
                    self.canvas.join_lines.append(jl)
                    jl.update_line()
        for ck,pk in self.fk_map.items():
            if pk.startswith(table_key+"."):
                child_tab=".".join(ck.split('.')[:2])
                citm=self.canvas.table_items.get(child_tab,None)
                if citm:
                    jl=JoinLine(citm,item,"LEFT",f"{ck}={pk}")
                    self.canvas.scene_.addItem(jl)
                    self.canvas.join_lines.append(jl)
                    jl.update_line()

    def get_selected_columns(self):
        arr=[]
        for k,itm in self.canvas.table_items.items():
            if hasattr(itm,"get_selected_columns"):
                arr.extend(itm.get_selected_columns())
        return arr

    def get_all_possible_columns_for_dialog(self):
        arr=[]
        for k,itm in self.canvas.table_items.items():
            if hasattr(itm,"columns"):
                for c in itm.columns:
                    arr.append(f"{k}.{c}")
        return arr

    def toggle_dml_canvas(self):
        if self.operation_mode=="SELECT":
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

        self.canvas.add_vertical_red_line(450)
        if not self.canvas.complete_query_item:
            rect=QGraphicsRectItem(0,0,200,120)
            rect.setBrush(QBrush(QColor(250,250,180)))
            rect.setPen(QPen(Qt.red,2))
            rect.setPos(100,200)
            rect.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
            lab=QGraphicsTextItem("Complete Query (Source)", rect)
            lab.setPos(5,5)
            self.canvas.scene_.addItem(rect)
            self.canvas.complete_query_item=rect

        if not self.canvas.target_table_item:
            rect2=QGraphicsRectItem(0,0,200,120)
            rect2.setBrush(QBrush(QColor(220,220,255)))
            rect2.setPen(QPen(Qt.darkGray,2))
            rect2.setPos(500,200)
            rect2.setFlags(QGraphicsItem.ItemIsMovable|QGraphicsItem.ItemIsSelectable)
            l2=QGraphicsTextItem("Target: myDB.myTarget", rect2)
            l2.setPos(5,5)
            y2=25
            for cc in ["colA","colB","key"]:
                t2=QGraphicsTextItem(cc,rect2)
                t2.setPos(5,y2)
                y2+=15
            self.canvas.scene_.addItem(rect2)
            self.canvas.target_table_item=rect2

    def generate_sql(self):
        if not self.auto_generate:
            return
        if self.operation_mode!="SELECT":
            self.rebuild_bfs_complex_query_item()

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

    def rebuild_bfs_complex_query_item(self):
        if not self.canvas.complete_query_item:
            return
        old_ch=list(self.canvas.complete_query_item.childItems())
        for ch in old_ch:
            if isinstance(ch,QGraphicsTextItem):
                txt=ch.toPlainText()
                if not txt.startswith("Complete Query"):
                    ch.setParentItem(None)
                    self.canvas.scene_.removeItem(ch)

        bfs_cols=self.get_selected_columns()
        yOff=25
        for col in bfs_cols:
            t=QGraphicsTextItem(col, self.canvas.complete_query_item)
            t.setPos(5,yOff)
            yOff+=15

    def validate_sql(self):
        txt=self.sql_display.toPlainText().strip()
        if not txt:
            self.validation_lbl.setText("SQL Status: No SQL.")
            self.validation_lbl.setStyleSheet("color:orange;")
            return
        try:
            parser=FullSQLParser(txt)
            parser.parse()
            self.validation_lbl.setText("SQL Status: Valid.")
            self.validation_lbl.setStyleSheet("color:green;")
        except Exception as ex:
            self.validation_lbl.setText(f"SQL Status: Invalid - {ex}")
            self.validation_lbl.setStyleSheet("color:red;")

    def _build_bfs_from(self):
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
                    blocks.append("-- Another subgraph:\nFROM "+block)
        if not blocks:
            return "-- no tables on canvas"
        return "\n".join(blocks)

    def _generate_select(self):
        scols=self.get_selected_columns()
        if not scols:
            scols=["*"]
        ags=self.group_panel.get_aggregates()
        final_cols=list(scols)
        for (f,c,a) in ags:
            if f.upper()=="CUSTOM":
                final_cols.append(c)
            else:
                final_cols.append(f"{f}({c}) AS {a}")
        lines=[]
        lines.append("SELECT "+", ".join(final_cols))
        lines.append(self._build_bfs_from())

        wfs=self.filter_panel.get_filters("WHERE")
        if wfs:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in wfs]
            lines.append("WHERE "+" AND ".join(conds))

        gb=self.group_panel.get_group_by()
        if gb:
            lines.append("GROUP BY "+", ".join(gb))

        hv=self.filter_panel.get_filters("HAVING")
        if hv:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in hv]
            lines.append("HAVING "+" AND ".join(conds))

        ob=self.sort_panel.get_order_bys()
        if ob:
            lines.append("ORDER BY "+", ".join(ob))
        lm=self.sort_panel.get_limit()
        if lm is not None:
            lines.append(f"LIMIT {lm}")
        off=self.sort_panel.get_offset()
        if off is not None:
            lines.append(f"OFFSET {off}")

        return "\n".join(lines)

    def _generate_select_sql_only(self):
        scols=self.get_selected_columns()
        if not scols:
            scols=["*"]
        lines=[]
        lines.append("SELECT "+", ".join(scols))
        lines.append(self._build_bfs_from())
        wfs=self.filter_panel.get_filters("WHERE")
        if wfs:
            conds=[f"{x[0]} {x[1]} {x[2]}" for x in wfs]
            lines.append("WHERE "+" AND ".join(conds))
        return "\n".join(lines)

    def _parse_target_info(self):
        if not self.canvas.target_table_item:
            return (None,None)
        for ch in self.canvas.target_table_item.childItems():
            if isinstance(ch,QGraphicsTextItem):
                txt=ch.toPlainText().strip()
                if txt.startswith("Target:"):
                    raw=txt.replace("Target:","").strip()
                    if "." in raw:
                        parts=raw.split(".",1)
                        return (parts[0].strip(), parts[1].strip())
        return (None,None)

    def _parse_mapped_columns(self):
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
        target_cols=[m[1] for m in mapped]
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

###############################################################################
# 16) Main Window
###############################################################################
class MainVQBWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fully Integrated VQB - No Gaps")
        self.resize(1200,800)

        self.builder_tab=VisualQueryBuilderTab()
        self.setCentralWidget(self.builder_tab)
        self.init_toolbar()

    def init_toolbar(self):
        tb=self.addToolBar("Main Toolbar")
        fit_act=QAction("Fit to View",self)
        fit_act.triggered.connect(self.on_fit_view)
        tb.addAction(fit_act)

        layout_act=QAction("Auto-Layout",self)
        layout_act.triggered.connect(self.on_auto_layout)
        tb.addAction(layout_act)

        map_act=QAction("Demo Map (srcCol1->colA)",self)
        map_act.triggered.connect(self.demo_map)
        tb.addAction(map_act)

    def on_fit_view(self):
        sc=self.builder_tab.canvas.scene_
        self.builder_tab.canvas.fitInView(sc.itemsBoundingRect(),Qt.KeepAspectRatio)

    def on_auto_layout(self):
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
        cv=self.builder_tab.canvas
        if not cv.complete_query_item or not cv.target_table_item:
            QMessageBox.information(self,"No placeholders","Switch to DML mode first.")
            return
        left_txt=None
        for ch in cv.complete_query_item.childItems():
            if isinstance(ch,QGraphicsTextItem):
                if ch.toPlainText().strip().lower()=="srccol1":
                    left_txt=ch
                    break
        right_txt=None
        for ch in cv.target_table_item.childItems():
            if isinstance(ch,QGraphicsTextItem):
                if ch.toPlainText().strip().lower()=="cola":
                    right_txt=ch
                    break
        if not left_txt or not right_txt:
            QMessageBox.information(self,"Not found","srcCol1 or colA not found in placeholders.")
            return
        cv.create_mapping_line(left_txt,right_txt)

def main():
    app=QApplication(sys.argv)
    apply_fusion_style()
    w=MainVQBWindow()
    w.show()
    sys.exit(app.exec_())

if __name__=="__main__":
    main()