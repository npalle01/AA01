#!/usr/bin/env python

import sys
import sqlite3
import logging
import json
import math
import re
from datetime import datetime
from collections import deque

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt, QDateTime, QDate, QTimer
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog, QVBoxLayout, QHBoxLayout,
    QFormLayout, QPushButton, QLineEdit, QLabel, QTextEdit, QTableWidget,
    QTableWidgetItem, QMessageBox, QComboBox, QInputDialog, QDockWidget,
    QDateTimeEdit, QTabWidget, QGroupBox, QAbstractItemView, QPlainTextEdit,
    QSplitter
)
import pyqtgraph as pg

##############################################################################
# LOGGING
##############################################################################
logging.basicConfig(
    filename='brmtool_pyqtgraph.log',
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

DB_URI = "file::memory:?cache=shared"

##############################################################################
# DETECT OPERATION TYPE
##############################################################################
def get_op_type_from_sql(sql_text: str) -> str:
    txt = sql_text.strip().upper()
    if txt.startswith("INSERT"):
        return "INSERT"
    elif txt.startswith("DELETE"):
        return "DELETE"
    elif txt.startswith("UPDATE"):
        return "UPDATE"
    elif txt.startswith("SELECT"):
        return "SELECT"
    return "OTHER"

##############################################################################
# DB SETUP (WITH APPROVALS)
##############################################################################
def setup_in_memory_db():
    conn = sqlite3.connect(DB_URI, uri=True, timeout=10.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row

    # 1) USERS, GROUPS
    conn.execute("""
    CREATE TABLE IF NOT EXISTS USERS(
        USER_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        USERNAME TEXT UNIQUE NOT NULL,
        PASSWORD TEXT NOT NULL,
        USER_GROUP TEXT NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BUSINESS_GROUPS(
        GROUP_NAME TEXT PRIMARY KEY,
        DESCRIPTION TEXT,
        EMAIL TEXT
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS GROUP_PERMISSIONS(
        GROUP_NAME TEXT NOT NULL,
        TARGET_TABLE TEXT NOT NULL,
        PRIMARY KEY(GROUP_NAME, TARGET_TABLE),
        FOREIGN KEY(GROUP_NAME) REFERENCES BUSINESS_GROUPS(GROUP_NAME) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_TYPES(
        RULE_TYPE_ID INTEGER PRIMARY KEY,
        RULE_TYPE_NAME TEXT NOT NULL UNIQUE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_GROUPS(
        GROUP_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT UNIQUE NOT NULL,
        DESCRIPTION TEXT
    );
    """)

    # 2) BRM_RULES with APPROVAL_STATUS
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULES(
        RULE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_ID INTEGER,
        PARENT_RULE_ID INTEGER,
        RULE_TYPE_ID INTEGER NOT NULL,
        RULE_NAME TEXT NOT NULL,
        RULE_SQL TEXT NOT NULL,
        EFFECTIVE_START_DATE TEXT NOT NULL,
        EFFECTIVE_END_DATE TEXT,
        STATUS TEXT NOT NULL CHECK (STATUS IN ('ACTIVE','INACTIVE')),
        VERSION INTEGER NOT NULL DEFAULT 1,
        CREATED_BY TEXT NOT NULL,
        DESCRIPTION TEXT,
        OPERATION_TYPE TEXT,
        BUSINESS_JUSTIFICATION TEXT,
        CREATED_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP,
        UPDATED_BY TEXT,
        OWNER_GROUP TEXT NOT NULL,
        CLUSTER_NAME TEXT,
        APPROVAL_STATUS TEXT NOT NULL DEFAULT 'DRAFT',
        FOREIGN KEY(RULE_TYPE_ID) REFERENCES BRM_RULE_TYPES(RULE_TYPE_ID),
        FOREIGN KEY(PARENT_RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE,
        FOREIGN KEY(GROUP_ID) REFERENCES BRM_RULE_GROUPS(GROUP_ID) ON DELETE SET NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_TABLE_DEPENDENCIES(
        DEPENDENCY_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RULE_ID INTEGER NOT NULL,
        DATABASE_NAME TEXT NOT NULL,
        TABLE_NAME TEXT NOT NULL,
        COLUMN_NAME TEXT NOT NULL,
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_AUDIT_LOG(
        AUDIT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        ACTION TEXT NOT NULL,
        TABLE_NAME TEXT NOT NULL,
        RECORD_ID TEXT NOT NULL,
        ACTION_BY TEXT NOT NULL,
        OLD_DATA TEXT,
        NEW_DATA TEXT,
        ACTION_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_LINEAGE(
        LINEAGE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RULE_ID INTEGER NOT NULL,
        SOURCE_INFO TEXT,
        TARGET_INFO TEXT,
        TRANSFORMATION_DETAILS TEXT,
        CREATED_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_GROUP_BACKUPS(
        BACKUP_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT NOT NULL,
        BACKUP_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP,
        BACKUP_VERSION INTEGER NOT NULL,
        BACKUP_JSON TEXT NOT NULL,
        FOREIGN KEY(GROUP_NAME) REFERENCES BUSINESS_GROUPS(GROUP_NAME) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_COLUMN_MAPPING(
        MAPPING_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RULE_ID INTEGER NOT NULL,
        SOURCE_RULE_ID INTEGER NOT NULL,
        SOURCE_COLUMN_NAME TEXT NOT NULL,
        TARGET_COLUMN_NAME TEXT NOT NULL,
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE,
        FOREIGN KEY(SOURCE_RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
    );
    """)

    # 3) CUSTOM GROUPS
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_CUSTOM_RULE_GROUPS(
        CUSTOM_GROUP_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        CUSTOM_GROUP_NAME TEXT NOT NULL UNIQUE,
        OWNER_BUSINESS_GROUP TEXT NOT NULL,
        CREATED_BY TEXT NOT NULL,
        CREATED_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_CUSTOM_GROUP_MEMBERS(
        CUSTOM_GROUP_ID INTEGER NOT NULL,
        RULE_ID INTEGER NOT NULL,
        PRIMARY KEY(CUSTOM_GROUP_ID, RULE_ID),
        FOREIGN KEY(CUSTOM_GROUP_ID) REFERENCES BRM_CUSTOM_RULE_GROUPS(CUSTOM_GROUP_ID) ON DELETE CASCADE,
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
    );
    """)

    # 4) APPROVAL WORKFLOW
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BUSINESS_GROUP_APPROVERS(
        APPROVER_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT NOT NULL,
        USERNAME TEXT NOT NULL,
        FOREIGN KEY(GROUP_NAME) REFERENCES BUSINESS_GROUPS(GROUP_NAME) ON DELETE CASCADE
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_APPROVALS(
        RULE_ID INTEGER NOT NULL,
        GROUP_NAME TEXT NOT NULL,
        USERNAME TEXT NOT NULL,
        APPROVED_FLAG INTEGER NOT NULL DEFAULT 0,
        APPROVED_TIMESTAMP DATETIME,
        PRIMARY KEY(RULE_ID, GROUP_NAME, USERNAME),
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE,
        FOREIGN KEY(GROUP_NAME) REFERENCES BUSINESS_GROUPS(GROUP_NAME) ON DELETE CASCADE
    );
    """)

    # Seed some data
    groups = [
        ("Admin","Admin group","admin@example.com"),
        ("BG1","First group","bg1@example.com"),
        ("BG2","Second group","bg2@example.com"),
        ("BG3","Third group","bg3@example.com"),
    ]
    for g in groups:
        conn.execute("""
        INSERT OR IGNORE INTO BUSINESS_GROUPS(GROUP_NAME,DESCRIPTION,EMAIL)
        VALUES(?,?,?)
        """,(g[0],g[1],g[2]))

    users = [
        ("admin","admin","Admin"),
        ("bg1_user","user","BG1"),
        ("bg2_user","user","BG2"),
        ("bg3_user","user","BG3"),
    ]
    for u in users:
        conn.execute("""
        INSERT OR IGNORE INTO USERS(USERNAME,PASSWORD,USER_GROUP)
        VALUES(?,?,?)
        """,(u[0],u[1],u[2]))

    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(1,'DQ')")
    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(2,'DM')")

    # Example perms
    perms=[
        ("Admin","TABLE_A"),
        ("Admin","TABLE_B"),
        ("BG1","TABLE_A"),
        ("BG1","TABLE_B"),
        ("BG2","TABLE_C"),
        ("BG3","TABLE_D")
    ]
    for p in perms:
        conn.execute("""
        INSERT OR IGNORE INTO GROUP_PERMISSIONS(GROUP_NAME,TARGET_TABLE)
        VALUES(?,?)
        """,p)

    conn.commit()
    return conn

##############################################################################
# APPROVAL BFS
##############################################################################
def find_impacted_business_groups(conn, rule_id):
    impacted=set()
    c=conn.cursor()
    c.execute("SELECT OWNER_GROUP FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
    row=c.fetchone()
    if row:
        impacted.add(row["OWNER_GROUP"])
    visited=set()
    queue=[rule_id]
    while queue:
        current=queue.pop()
        if current in visited:
            continue
        visited.add(current)
        c.execute("SELECT RULE_ID FROM BRM_COLUMN_MAPPING WHERE SOURCE_RULE_ID=?",(current,))
        children=c.fetchall()
        for ch in children:
            cid=ch["RULE_ID"]
            c.execute("SELECT OWNER_GROUP FROM BRM_RULES WHERE RULE_ID=?",(cid,))
            r2=c.fetchone()
            if r2:
                impacted.add(r2["OWNER_GROUP"])
            queue.append(cid)
    return list(impacted)

def create_approval_requests(conn, rule_id, impacted_groups):
    c=conn.cursor()
    for grp in impacted_groups:
        c.execute("SELECT USERNAME FROM BUSINESS_GROUP_APPROVERS WHERE GROUP_NAME=?",(grp,))
        approvers=c.fetchall()
        for ap in approvers:
            user_ap=ap["USERNAME"]
            c.execute("""
            INSERT OR IGNORE INTO BRM_RULE_APPROVALS(RULE_ID,GROUP_NAME,USERNAME,APPROVED_FLAG)
            VALUES(?,?,?,0)
            """,(rule_id,grp,user_ap))
        logger.info(f"Approvals required for group {grp} on rule {rule_id}")
    conn.commit()

def check_if_all_approved(conn, rule_id):
    c=conn.cursor()
    c.execute("""
    SELECT COUNT(*) as pending
    FROM BRM_RULE_APPROVALS
    WHERE RULE_ID=? AND APPROVED_FLAG=0
    """,(rule_id,))
    row=c.fetchone()
    return (row["pending"]==0)

##############################################################################
# CRUD (FORCE APPROVAL WORKFLOW)
##############################################################################
def add_audit_log(conn, action, table_name, record_id, action_by, old_data, new_data):
    c=conn.cursor()
    c.execute("""
    INSERT INTO BRM_AUDIT_LOG(ACTION,TABLE_NAME,RECORD_ID,ACTION_BY,OLD_DATA,NEW_DATA)
    VALUES(?,?,?,?,?,?)
    """,(action, table_name,str(record_id),action_by,
         json.dumps(old_data) if old_data else None,
         json.dumps(new_data) if new_data else None))
    conn.commit()

def extract_tables(sql_text):
    pattern = re.compile(r'\bFROM\s+([^\s,]+)', re.IGNORECASE)
    matches=pattern.findall(sql_text)
    result=[]
    for m in matches:
        if '.' in m:
            db,tbl=m.split('.',1)
            result.append((db,tbl))
        else:
            result.append(("DEFAULT_DB",m))
    return result

def find_child_rules(conn, parent_rule_id):
    results=[]
    queue=[parent_rule_id]
    visited=set()
    c=conn.cursor()
    while queue:
        curr=queue.pop()
        if curr in visited:
            continue
        visited.add(curr)
        c.execute("SELECT RULE_ID FROM BRM_RULES WHERE PARENT_RULE_ID=?",(curr,))
        kids=c.fetchall()
        for k in kids:
            results.append(k["RULE_ID"])
            queue.append(k["RULE_ID"])
    return results

def add_rule(conn, rule_data, created_by, user_group):
    c=conn.cursor()
    # Force new rule => INACTIVE => DRAFT
    c.execute("""
    INSERT INTO BRM_RULES(
      GROUP_ID,PARENT_RULE_ID,RULE_TYPE_ID,RULE_NAME,RULE_SQL,
      EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,
      STATUS,VERSION,CREATED_BY,DESCRIPTION,OPERATION_TYPE,
      BUSINESS_JUSTIFICATION,OWNER_GROUP,APPROVAL_STATUS
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
      rule_data.get("GROUP_ID"),
      rule_data.get("PARENT_RULE_ID"),
      rule_data["RULE_TYPE_ID"],
      rule_data["RULE_NAME"],
      rule_data["RULE_SQL"],
      rule_data["EFFECTIVE_START_DATE"],
      rule_data.get("EFFECTIVE_END_DATE"),
      "INACTIVE",
      1,
      created_by,
      rule_data.get("DESCRIPTION"),
      rule_data["OPERATION_TYPE"],
      rule_data.get("BUSINESS_JUSTIFICATION",""),
      rule_data["OWNER_GROUP"],
      "DRAFT"
    ))
    new_id=c.lastrowid

    # dependencies
    deps=extract_tables(rule_data["RULE_SQL"])
    for db_name,tbl_name in deps:
        c.execute("""
        INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(RULE_ID,DATABASE_NAME,TABLE_NAME,COLUMN_NAME)
        VALUES(?,?,?,?)
        """,(new_id,db_name,tbl_name,"DerivedCol"))
    add_audit_log(conn,"ADD","BRM_RULES",new_id,created_by,None,rule_data)
    conn.commit()

    # BFS => create approval requests
    impacted=find_impacted_business_groups(conn,new_id)
    create_approval_requests(conn,new_id,impacted)
    return new_id

def update_rule(conn, rule_data, updated_by, user_group):
    c=conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rule_data["RULE_ID"],))
    old=c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    old_data=dict(old)

    # if was APPROVED => set to REVIEW_IN_PROGRESS
    new_approval_status="REVIEW_IN_PROGRESS" if old["APPROVAL_STATUS"]=="APPROVED" else old["APPROVAL_STATUS"]

    c.execute("""
    UPDATE BRM_RULES
    SET
      GROUP_ID=?,
      PARENT_RULE_ID=?,
      RULE_TYPE_ID=?,
      RULE_NAME=?,
      RULE_SQL=?,
      EFFECTIVE_START_DATE=?,
      EFFECTIVE_END_DATE=?,
      STATUS='INACTIVE',
      VERSION=VERSION+1,
      UPDATED_BY=?,
      DESCRIPTION=?,
      OPERATION_TYPE=?,
      BUSINESS_JUSTIFICATION=?,
      OWNER_GROUP=?,
      APPROVAL_STATUS=?
    WHERE RULE_ID=?
    """,(
      rule_data.get("GROUP_ID"),
      rule_data.get("PARENT_RULE_ID"),
      rule_data["RULE_TYPE_ID"],
      rule_data["RULE_NAME"],
      rule_data["RULE_SQL"],
      rule_data["EFFECTIVE_START_DATE"],
      rule_data.get("EFFECTIVE_END_DATE"),
      updated_by,
      rule_data.get("DESCRIPTION"),
      rule_data.get("OPERATION_TYPE"),
      rule_data.get("BUSINESS_JUSTIFICATION",""),
      rule_data["OWNER_GROUP"],
      new_approval_status,
      rule_data["RULE_ID"]
    ))
    c.execute("DELETE FROM BRM_RULE_TABLE_DEPENDENCIES WHERE RULE_ID=?",(rule_data["RULE_ID"],))
    deps=extract_tables(rule_data["RULE_SQL"])
    for db_name,tbl_name in deps:
        c.execute("""
        INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(RULE_ID,DATABASE_NAME,TABLE_NAME,COLUMN_NAME)
        VALUES(?,?,?,?)
        """,(rule_data["RULE_ID"],db_name,tbl_name,"DerivedCol"))

    new_data=dict(old_data)
    for k,v in rule_data.items():
        new_data[k]=v
    new_data["VERSION"]= old["VERSION"]+1
    add_audit_log(conn,"UPDATE","BRM_RULES",rule_data["RULE_ID"],updated_by,old_data,new_data)
    conn.commit()

    impacted=find_impacted_business_groups(conn,rule_data["RULE_ID"])
    create_approval_requests(conn,rule_data["RULE_ID"],impacted)

    # Return child rules
    kids=find_child_rules(conn,rule_data["RULE_ID"])
    # For existing UI
    return [{"RULE_ID":x, "RULE_NAME":""} for x in kids]

def deactivate_rule(conn, rule_id, updated_by, user_group):
    c=conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
    old=c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    if old["APPROVAL_STATUS"]!="APPROVED":
        raise ValueError("Cannot deactivate unless rule is APPROVED.")
    c.execute("SELECT * FROM BRM_RULES WHERE PARENT_RULE_ID=? AND STATUS='ACTIVE'",(rule_id,))
    kids=c.fetchall()
    if kids:
        raise ValueError("Cannot deactivate rule. Child rules must be deactivated first.")

    old_data=dict(old)
    c.execute("""
    UPDATE BRM_RULES
    SET STATUS='INACTIVE', VERSION=VERSION+1, UPDATED_BY=?
    WHERE RULE_ID=?
    """,(updated_by, rule_id))
    new_data=dict(old_data)
    new_data["STATUS"]="INACTIVE"
    new_data["VERSION"]= old["VERSION"]+1
    add_audit_log(conn,"DEACTIVATE","BRM_RULES",rule_id,updated_by,old_data,new_data)
    conn.commit()

def delete_rule(conn, rule_id, action_by, user_group):
    c=conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
    old=c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    if old["APPROVAL_STATUS"]!="APPROVED":
        raise ValueError("Cannot delete unless rule is APPROVED.")
    if old["STATUS"]!="INACTIVE":
        raise ValueError("Must be INACTIVE first.")

    c.execute("SELECT * FROM BRM_RULES WHERE PARENT_RULE_ID=?",(rule_id,))
    kids=c.fetchall()
    if kids:
        raise ValueError("Cannot delete rule. Child rules exist.")

    c.execute("SELECT * FROM BRM_COLUMN_MAPPING WHERE SOURCE_RULE_ID=? OR RULE_ID=?",(rule_id,rule_id))
    leftover=c.fetchall()
    if leftover:
        raise ValueError("Remove or re-map references first.")

    old_data=dict(old)
    c.execute("DELETE FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
    add_audit_log(conn,"DELETE","BRM_RULES",rule_id,action_by,old_data,None)
    conn.commit()

##############################################################################
# LINEAGE VISUALIZATION (EnhancedLineageGraphWidget)
##############################################################################
class RuleRectItem(QtWidgets.QGraphicsRectItem):
    def __init__(self, x, y, w, h, rule_data, cluster_name="", parent=None):
        super().__init__(x,y,w,h,parent)
        self.rule_data=rule_data
        self.cluster_name=cluster_name
        self.highlighted=False
    def setHighlight(self, highlight):
        self.highlighted=highlight
        if highlight:
            self.setPen(QtGui.QPen(QtGui.QColor("yellow"),4))
        else:
            self.setPen(QtGui.QPen(QtCore.Qt.black,2))

class RuleEllipseItem(QtWidgets.QGraphicsEllipseItem):
    def __init__(self, x, y, w, h, rule_data, cluster_name="", parent=None):
        super().__init__(x,y,w,h,parent)
        self.rule_data=rule_data
        self.cluster_name=cluster_name
        self.highlighted=False
    def setHighlight(self, highlight):
        self.highlighted=highlight
        if highlight:
            self.setPen(QtGui.QPen(QtGui.QColor("yellow"),4))
        else:
            self.setPen(QtGui.QPen(QtCore.Qt.black,2))

class EnhancedLineageGraphWidget(QtWidgets.QGraphicsView):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.scene=QtWidgets.QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QtWidgets.QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)

        self.node_map={}
        self.children_map={}
        self.parents_map={}

        self.minimap=QtWidgets.QGraphicsView(self.scene)
        self.minimap.setRenderHint(QtGui.QPainter.Antialiasing)
        self.minimap.setFixedSize(200,150)
        self.minimap.setStyleSheet("background: rgba(255,255,255,0.7); border:1px solid gray;")
        self.minimap.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.minimap.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)

        self.detail_dock=QDockWidget("Rule Details", self.parentWidget())
        self.detail_panel=QTextEdit()
        self.detail_panel.setReadOnly(True)
        self.detail_dock.setWidget(self.detail_panel)
        self.detail_dock.setAllowedAreas(Qt.LeftDockWidgetArea|Qt.RightDockWidgetArea)
        mainwin=self.find_main_window()
        if mainwin:
            mainwin.addDockWidget(Qt.RightDockWidgetArea, self.detail_dock)
        else:
            self.detail_dock.hide()

        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_context_menu)

    def find_main_window(self):
        p=self.parent()
        while p:
            if isinstance(p, QMainWindow):
                return p
            p=p.parent()
        return None

    def resizeEvent(self, event):
        super().resizeEvent(event)
        x=self.width()-self.minimap.width()-20
        y=20
        self.minimap.move(x,y)

    def populate_graph(self):
        from collections import deque
        self.scene.clear()
        self.node_map.clear()
        self.children_map.clear()
        self.parents_map.clear()

        c=self.connection.cursor()
        c.execute("""
        SELECT RULE_ID,RULE_NAME,PARENT_RULE_ID,STATUS,RULE_TYPE_ID,CLUSTER_NAME
        FROM BRM_RULES
        ORDER BY RULE_ID
        """)
        rules=c.fetchall()
        if not rules:
            no_data=QtWidgets.QGraphicsTextItem("No rules found.")
            self.scene.addItem(no_data)
            return
        for r in rules:
            rid=r["RULE_ID"]
            pid=r["PARENT_RULE_ID"]
            if pid:
                self.children_map.setdefault(pid,[]).append(rid)
                self.parents_map[rid]=pid
        rule_lookup={r["RULE_ID"]:r for r in rules}
        roots=[r for r in rules if not r["PARENT_RULE_ID"]]

        queue=deque()
        level_map={}
        visited=set()
        for rt in roots:
            queue.append((rt["RULE_ID"],0))

        while queue:
            rid,depth=queue.popleft()
            if rid in visited:
                continue
            visited.add(rid)
            rinfo=rule_lookup[rid]
            count_so_far=level_map.get(depth,0)
            level_map[depth]=count_so_far+1
            x=depth*220
            y=count_so_far*120
            node_item=self.create_node(rinfo)
            node_item.setPos(x,y)
            self.scene.addItem(node_item)
            self.node_map[rid]=node_item

            if rid in self.children_map:
                for ch in self.children_map[rid]:
                    queue.append((ch, depth+1))

        for r in rules:
            pid=r["PARENT_RULE_ID"]
            rid=r["RULE_ID"]
            if pid and pid in self.node_map and rid in self.node_map:
                self.draw_edge(pid,rid)

        self.scene.setSceneRect(self.scene.itemsBoundingRect())
        self.reset_minimap()

    def create_node(self, rinfo):
        rtype=rinfo["RULE_TYPE_ID"]
        status=rinfo["STATUS"]
        cluster=rinfo.get("CLUSTER_NAME","") or ""
        if rtype==1:
            node_item=RuleRectItem(0,0,120,50,rinfo,cluster)
        else:
            node_item=RuleEllipseItem(0,0,120,50,rinfo,cluster)
        if status.lower()=="active":
            base_color=QtGui.QColor("lightgreen")
        else:
            base_color=QtGui.QColor("tomato")
        if cluster:
            hue_val=abs(hash(cluster))%360
            base_color=QtGui.QColor.fromHsv(hue_val,128,255)
        node_item.setBrush(QtGui.QBrush(base_color))
        node_item.setPen(QtGui.QPen(QtCore.Qt.black,2))
        return node_item

    def draw_edge(self, parent_id, child_id):
        p_item=self.node_map[parent_id]
        c_item=self.node_map[child_id]
        p_rect=p_item.sceneBoundingRect()
        c_rect=c_item.sceneBoundingRect()
        line=QtWidgets.QGraphicsLineItem(
            p_rect.center().x(),p_rect.center().y(),
            c_rect.center().x(),c_rect.center().y()
        )
        line.setPen(QtGui.QPen(QtGui.QColor("darkblue"),2))
        self.scene.addItem(line)

    def reset_minimap(self):
        if self.scene and self.scene.sceneRect().isValid():
            self.minimap.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)

    def resetView(self):
        if self.scene and self.scene.sceneRect().isValid():
            self.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)
        self.reset_minimap()

    def mousePressEvent(self, event):
        if event.button()==Qt.LeftButton:
            item=self.itemAt(event.pos())
            if isinstance(item,(RuleRectItem,RuleEllipseItem)):
                self.show_rule_details(item.rule_data)
        super().mousePressEvent(event)

    def show_rule_details(self, rinfo):
        if not self.detail_dock or not self.detail_panel:
            return
        msg=(f"Rule ID: {rinfo['RULE_ID']}\n"
             f"Name: {rinfo['RULE_NAME']}\n"
             f"Status: {rinfo['STATUS']}\n"
             f"Type: {rinfo['RULE_TYPE_ID']}\n"
             f"Parent: {rinfo.get('PARENT_RULE_ID')}\n")
        self.detail_panel.setPlainText(msg)

    def show_context_menu(self, pos):
        item=self.itemAt(pos)
        menu=QtWidgets.QMenu()
        if isinstance(item,(RuleRectItem,RuleEllipseItem)):
            rinfo=item.rule_data
            edit_action=QtWidgets.QAction("Edit Rule")
            edit_action.triggered.connect(lambda:self.edit_rule(rinfo["RULE_ID"]))
            menu.addAction(edit_action)

            hi_up=QtWidgets.QAction("Highlight Ancestors")
            hi_up.triggered.connect(lambda:self.highlight_ancestors(rinfo["RULE_ID"]))
            menu.addAction(hi_up)

            hi_dn=QtWidgets.QAction("Highlight Descendants")
            hi_dn.triggered.connect(lambda:self.highlight_descendants(rinfo["RULE_ID"]))
            menu.addAction(hi_dn)
        else:
            reset_action=QtWidgets.QAction("Clear Highlights")
            reset_action.triggered.connect(self.clear_highlights)
            menu.addAction(reset_action)
        menu.exec_(self.mapToGlobal(pos))

    def edit_rule(self,rule_id):
        c=self.connection.cursor()
        c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
        row=c.fetchone()
        if not row:
            QMessageBox.warning(self,"Not Found",f"Rule {rule_id} not found.")
            return
        rule_data=dict(row)

        c2=self.connection.cursor()
        c2.execute("SELECT RULE_TYPE_NAME,RULE_TYPE_ID FROM BRM_RULE_TYPES")
        rts=c2.fetchall()
        rtypes={r_["RULE_TYPE_NAME"]:r_["RULE_TYPE_ID"] for r_ in rts}
        dlg=RuleEditorDialog(self.connection,rtypes,"Admin",rule_data=rule_data,parent=self)
        dlg.exec_()
        self.populate_graph()

    def highlight_ancestors(self, start_id):
        self.clear_highlights()
        cur=start_id
        while cur in self.parents_map:
            it=self.node_map.get(cur)
            if it:
                it.setHighlight(True)
            pid=self.parents_map[cur]
            if pid and pid in self.node_map:
                self.node_map[pid].setHighlight(True)
            cur=pid if pid else None

    def highlight_descendants(self, start_id):
        from collections import deque
        self.clear_highlights()
        queue=deque([start_id])
        visited=set()
        while queue:
            cid=queue.pop()
            if cid in visited:
                continue
            visited.add(cid)
            if cid in self.node_map:
                self.node_map[cid].setHighlight(True)
            if cid in self.children_map:
                for ch in self.children_map[cid]:
                    queue.append(ch)

    def clear_highlights(self):
        for n in self.node_map.values():
            n.setHighlight(False)

    def search_nodes(self, query):
        self.clear_highlights()
        c=self.connection.cursor()
        found_any=False
        qlower=query.lower()

        for rid,it in self.node_map.items():
            nm=it.rule_data["RULE_NAME"].lower()
            rid_str=str(it.rule_data["RULE_ID"])
            if (qlower in nm) or (qlower==rid_str):
                it.setHighlight(True)
                found_any=True

        c.execute("""
        SELECT RULE_ID,SOURCE_COLUMN_NAME,TARGET_COLUMN_NAME
        FROM BRM_COLUMN_MAPPING
        WHERE LOWER(SOURCE_COLUMN_NAME) LIKE ? OR LOWER(TARGET_COLUMN_NAME) LIKE ?
        """,(f"%{qlower}%",f"%{qlower}%"))
        for rw in c.fetchall():
            cid=rw["RULE_ID"]
            if cid in self.node_map:
                self.node_map[cid].setHighlight(True)
                found_any=True

        if not found_any:
            QMessageBox.information(self,"No Match",f"No rule or column found for '{query}'")
        else:
            self.reset_minimap()

##############################################################################
# RULE EDITOR
##############################################################################
class RuleEditorDialog(QDialog):
    def __init__(self, connection, rule_types, logged_in_user, rule_data=None, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.rule_types=rule_types
        self.logged_in_user=logged_in_user
        self.rule_data=rule_data

        title="Edit Rule" if rule_data else "Add New Rule"
        self.setWindowTitle(title)
        self.resize(900,500)

        main_layout=QHBoxLayout(self)

        left_box=QGroupBox("Basic Info")
        left_layout=QFormLayout(left_box)

        self.group_combo=QComboBox()
        self.group_combo.addItem("None",None)
        try:
            c=self.connection.cursor()
            c.execute("SELECT GROUP_ID,GROUP_NAME FROM BRM_RULE_GROUPS ORDER BY GROUP_NAME")
            for row in c.fetchall():
                self.group_combo.addItem(row["GROUP_NAME"],row["GROUP_ID"])
        except:
            pass
        left_layout.addRow("Rule Group:",self.group_combo)

        self.parent_rule_combo=QComboBox()
        self.parent_rule_combo.addItem("None",None)
        try:
            c=self.connection.cursor()
            c.execute("SELECT RULE_ID,RULE_NAME FROM BRM_RULES WHERE STATUS='ACTIVE'")
            for row in c.fetchall():
                self.parent_rule_combo.addItem(f"{row['RULE_NAME']} (ID:{row['RULE_ID']})",row["RULE_ID"])
        except:
            pass
        left_layout.addRow("Parent Rule:",self.parent_rule_combo)

        self.name_edit=QLineEdit()
        left_layout.addRow("Rule Name:",self.name_edit)

        self.type_combo=QComboBox()
        for rt_name in self.rule_types:
            self.type_combo.addItem(rt_name)
        left_layout.addRow("Rule Type:",self.type_combo)

        self.status_combo=QComboBox()
        self.status_combo.addItems(["ACTIVE","INACTIVE"])
        left_layout.addRow("Status:",self.status_combo)

        self.start_dt=QDateTimeEdit(QDateTime.currentDateTime())
        self.start_dt.setCalendarPopup(True)
        self.start_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        left_layout.addRow("Start Date:",self.start_dt)

        self.end_dt=QDateTimeEdit(QDateTime.currentDateTime().addDays(30))
        self.end_dt.setCalendarPopup(True)
        self.end_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        left_layout.addRow("End Date:",self.end_dt)

        self.owner_grp_combo=QComboBox()
        try:
            c=self.connection.cursor()
            c.execute("SELECT DISTINCT GROUP_NAME FROM GROUP_PERMISSIONS ORDER BY GROUP_NAME")
            for g in c.fetchall():
                self.owner_grp_combo.addItem(g["GROUP_NAME"],g["GROUP_NAME"])
        except:
            pass
        left_layout.addRow("Owner Group:",self.owner_grp_combo)
        main_layout.addWidget(left_box)

        right_box=QGroupBox("Details & Logic")
        right_layout=QFormLayout(right_box)

        self.sql_editor=QPlainTextEdit()
        font=QtGui.QFont("Courier",10)
        self.sql_editor.setFont(font)
        right_layout.addRow(QLabel("Rule SQL:"),self.sql_editor)

        self.description_edit=QTextEdit()
        right_layout.addRow(QLabel("Description:"),self.description_edit)

        self.justification_edit=QTextEdit()
        right_layout.addRow(QLabel("Justification:"),self.justification_edit)

        btn_hbox=QHBoxLayout()
        self.save_btn=QPushButton("Save" if rule_data else "Add")
        self.save_btn.clicked.connect(self.on_save)
        btn_hbox.addWidget(self.save_btn)

        cancel_btn=QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_hbox.addWidget(cancel_btn)
        right_layout.addRow(btn_hbox)
        main_layout.addWidget(right_box)
        self.setLayout(main_layout)

        if self.rule_data:
            self.load_rule_data(self.rule_data)

    def load_rule_data(self, rd):
        if rd["GROUP_ID"]:
            idx=self.group_combo.findData(rd["GROUP_ID"])
            if idx>=0:
                self.group_combo.setCurrentIndex(idx)
        if rd["PARENT_RULE_ID"]:
            idx2=self.parent_rule_combo.findData(rd["PARENT_RULE_ID"])
            if idx2>=0:
                self.parent_rule_combo.setCurrentIndex(idx2)
        self.name_edit.setText(rd["RULE_NAME"])

        for nm,tid in self.rule_types.items():
            if tid==rd["RULE_TYPE_ID"]:
                i=self.type_combo.findText(nm)
                if i>=0:
                    self.type_combo.setCurrentIndex(i)
                break
        st=rd["STATUS"]
        i_st=self.status_combo.findText(st)
        if i_st>=0:
            self.status_combo.setCurrentIndex(i_st)

        try:
            sdt=datetime.strptime(rd["EFFECTIVE_START_DATE"],"%Y-%m-%d %H:%M:%S")
            self.start_dt.setDateTime(QtCore.QDateTime(sdt))
        except:
            pass
        if rd["EFFECTIVE_END_DATE"]:
            try:
                edt=datetime.strptime(rd["EFFECTIVE_END_DATE"],"%Y-%m-%d %H:%M:%S")
                self.end_dt.setDateTime(QtCore.QDateTime(edt))
            except:
                pass

        og=rd["OWNER_GROUP"]
        iog=self.owner_grp_combo.findText(og)
        if iog>=0:
            self.owner_grp_combo.setCurrentIndex(iog)
        self.sql_editor.setPlainText(rd["RULE_SQL"] or "")
        if rd.get("DESCRIPTION"):
            self.description_edit.setText(rd["DESCRIPTION"])
        if rd.get("BUSINESS_JUSTIFICATION"):
            self.justification_edit.setText(rd["BUSINESS_JUSTIFICATION"])

    def on_save(self):
        if not self.name_edit.text().strip():
            QMessageBox.warning(self,"Validation Error","Rule name cannot be empty.")
            return
        sql_text=self.sql_editor.toPlainText().strip()
        if not sql_text:
            QMessageBox.warning(self,"Validation Error","Rule SQL cannot be empty.")
            return
        op_type=get_op_type_from_sql(sql_text)

        rule_dict={
            "GROUP_ID": self.group_combo.currentData(),
            "PARENT_RULE_ID": self.parent_rule_combo.currentData(),
            "RULE_TYPE_ID": self.rule_types.get(self.type_combo.currentText()),
            "RULE_NAME": self.name_edit.text().strip(),
            "RULE_SQL": sql_text,
            "EFFECTIVE_START_DATE": self.start_dt.dateTime().toString("yyyy-MM-dd HH:mm:ss"),
            "EFFECTIVE_END_DATE": self.end_dt.dateTime().toString("yyyy-MM-dd HH:mm:ss"),
            "STATUS": self.status_combo.currentText(),
            "DESCRIPTION": self.description_edit.toPlainText().strip(),
            "OPERATION_TYPE": op_type,
            "BUSINESS_JUSTIFICATION": self.justification_edit.toPlainText().strip(),
            "OWNER_GROUP": self.owner_grp_combo.currentText().strip()
        }
        created_by=self.logged_in_user

        if self.rule_data:
            rule_dict["RULE_ID"]=self.rule_data["RULE_ID"]
            confirm=QMessageBox.question(self,"Confirm","Update this rule?")
            if confirm!=QMessageBox.Yes:
                return
            try:
                impacted=update_rule(self.connection,rule_dict,created_by,self.logged_in_user)
                if impacted:
                    msg="Child rules that may be impacted:\n\n"
                    for ch in impacted:
                        msg+=f"- ID:{ch['RULE_ID']}\n"
                    QMessageBox.information(self,"Impact",msg)
                QMessageBox.information(self,"Success","Rule updated.")
                self.accept()
            except Exception as e:
                QMessageBox.critical(self,"DB Error",str(e))
        else:
            confirm=QMessageBox.question(self,"Confirm","Create new rule?")
            if confirm!=QMessageBox.Yes:
                return
            try:
                new_id=add_rule(self.connection,rule_dict,created_by,self.logged_in_user)
                QMessageBox.information(self,"Success",f"Rule created with ID={new_id}")
                self.accept()
            except Exception as e:
                QMessageBox.critical(self,"DB Error",str(e))

##############################################################################
# RULE ANALYTICS
##############################################################################
class RuleAnalyticsDialog(QDialog):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.setWindowTitle("Rule Analytics")
        self.resize(800,600)
        layout=QVBoxLayout(self)

        chart_box=QHBoxLayout()

        self.bar_chart=pg.PlotWidget(title="Number of Rules by Creator")
        self.bar_chart.setBackground('w')
        chart_box.addWidget(self.bar_chart)

        self.pie_chart=pg.PlotWidget(title="Rule Status Distribution")
        self.pie_chart.setBackground('w')
        chart_box.addWidget(self.pie_chart)

        layout.addLayout(chart_box)
        close_btn=QPushButton("Close")
        close_btn.clicked.connect(self.close)
        layout.addWidget(close_btn)
        self.setLayout(layout)
        self.load_charts()

    def load_charts(self):
        c=self.connection.cursor()
        c.execute("SELECT CREATED_BY,COUNT(*) as cnt FROM BRM_RULES GROUP BY CREATED_BY")
        creators_data=c.fetchall()
        creators={row["CREATED_BY"]:row["cnt"] for row in creators_data}

        status_counts={"ACTIVE":0,"INACTIVE":0,"DELETED":0}
        c.execute("SELECT STATUS,COUNT(*) as sc FROM BRM_RULES GROUP BY STATUS")
        for strow in c.fetchall():
            s_up=strow["STATUS"].upper()
            status_counts[s_up]=strow["sc"]
        c.execute("SELECT COUNT(*) as deleted_count FROM BRM_AUDIT_LOG WHERE ACTION='DELETE'")
        row2=c.fetchone()
        if row2:
            status_counts["DELETED"]=row2["deleted_count"]

        self.bar_chart.clear()
        if creators:
            sorted_creators=sorted(creators.items(),key=lambda x:x[1],reverse=True)
            c_names=[sc[0] for sc in sorted_creators]
            c_vals=[sc[1] for sc in sorted_creators]
            bg=pg.BarGraphItem(x=range(len(c_names)), height=c_vals, width=0.6, brush="skyblue")
            self.bar_chart.addItem(bg)
            ax=self.bar_chart.getAxis("bottom")
            ax.setTicks([ list(zip(range(len(c_names)), c_names)) ])
            self.bar_chart.setLabel("left","Number of Rules")
            self.bar_chart.setLabel("bottom","Created By")
            self.bar_chart.showGrid(x=True,y=True)

        self.pie_chart.clear()
        total=sum(status_counts.values())
        if total>0:
            angles=[360*(v/total) for v in status_counts.values()]
            start=90
            color_map={"ACTIVE":"green","INACTIVE":"red","DELETED":"gray"}
            scene=self.pie_chart.scene()
            if not scene:
                from PyQt5.QtWidgets import QGraphicsScene
                scene=QGraphicsScene()
                self.pie_chart.setScene(scene)
            import math
            for (k,v),ang in zip(status_counts.items(),angles):
                if ang>0:
                    wedge=QtGui.QPainterPath()
                    wedge.moveTo(0,0)
                    wedge.arcTo(-100,-100,200,200,start,ang)
                    wedge.closeSubpath()
                    brush=QtGui.QBrush(QtGui.QColor(color_map.get(k,"blue")))
                    pi=pg.QtWidgets.QGraphicsPathItem(wedge)
                    pi.setBrush(brush)
                    pi.setPen(pg.mkPen("black"))
                    scene.addItem(pi)

                    mid=start+(ang/2)
                    rad=(mid*math.pi)/180
                    xx=50*math.cos(rad)
                    yy=50*math.sin(rad)
                    perc=math.floor((ang/360)*100)
                    lab=pg.TextItem(f"{k} ({perc}%)",anchor=(0.5,0.5))
                    lab.setPos(xx,yy)
                    scene.addItem(lab)
                    start+=ang
            self.pie_chart.setAspectLocked(True)

##############################################################################
# AUDIT LOG & SEARCH
##############################################################################
class AuditLogViewer(QDialog):
    def __init__(self, connection, user_group, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Audit Logs")
        self.resize(800,600)
        self.connection=connection
        self.user_group=user_group

        v=QVBoxLayout(self)
        hb=QHBoxLayout()
        self.search_edit=QLineEdit()
        self.search_edit.setPlaceholderText("Search by Action, Table, or Action By...")
        self.search_edit.textChanged.connect(self.perform_search)
        hb.addWidget(QLabel("Search:"))
        hb.addWidget(self.search_edit)
        v.addLayout(hb)

        self.audit_table=QTableWidget(0,8)
        self.audit_table.setHorizontalHeaderLabels(["Audit ID","Action","Table Name","Record ID","Action By","Old Data","New Data","Timestamp"])
        self.audit_table.horizontalHeader().setStretchLastSection(True)
        self.audit_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.audit_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.audit_table)

        rb=QPushButton("Refresh Logs")
        rb.clicked.connect(self.load_audit_logs)
        v.addWidget(rb)
        self.setLayout(v)
        self.load_audit_logs()

    def load_audit_logs(self):
        c=self.connection.cursor()
        c.execute("""
        SELECT AUDIT_ID,ACTION,TABLE_NAME,RECORD_ID,ACTION_BY,OLD_DATA,NEW_DATA,ACTION_TIMESTAMP
        FROM BRM_AUDIT_LOG
        ORDER BY ACTION_TIMESTAMP DESC
        LIMIT 1000
        """)
        rows=c.fetchall()
        self.audit_table.setRowCount(0)
        for row in rows:
            r=self.audit_table.rowCount()
            self.audit_table.insertRow(r)
            for i,val in enumerate(row):
                if i in [5,6]:
                    if val:
                        try:
                            parsed=json.loads(val)
                            txt=json.dumps(parsed,indent=4)
                        except:
                            txt=str(val)
                        item=QTableWidgetItem(txt)
                    else:
                        item=QTableWidgetItem("None")
                else:
                    item=QTableWidgetItem(str(val) if val else "None")
                self.audit_table.setItem(r,i,item)

    def perform_search(self, text):
        for row in range(self.audit_table.rowCount()):
            match=False
            for col in [1,2,4]:
                it=self.audit_table.item(row,col)
                if it and text.lower() in it.text().lower():
                    match=True
                    break
            self.audit_table.setRowHidden(row, not match)

class SearchRuleDialog(QDialog):
    def __init__(self, connection, user_group, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Search Rules")
        self.resize(800,600)
        self.connection=connection
        self.user_group=user_group

        v=QVBoxLayout(self)
        hb=QHBoxLayout()
        self.search_edit=QLineEdit()
        self.search_edit.setPlaceholderText("Enter rule name or SQL snippet...")
        self.search_edit.textChanged.connect(self.load_search_results)
        hb.addWidget(QLabel("Search:"))
        hb.addWidget(self.search_edit)
        v.addLayout(hb)

        self.results_view=QTableWidget(0,6)
        self.results_view.setHorizontalHeaderLabels(["Rule ID","Name","SQL","Status","Version","Created By"])
        self.results_view.horizontalHeader().setStretchLastSection(True)
        self.results_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.results_view)

        rb=QPushButton("Refresh Results")
        rb.clicked.connect(self.load_search_results)
        v.addWidget(rb)
        self.setLayout(v)
        self.load_search_results()

    def load_search_results(self):
        query=self.search_edit.text().strip()
        c=self.connection.cursor()
        if query:
            c.execute("""
            SELECT RULE_ID,RULE_NAME,RULE_SQL,STATUS,VERSION,CREATED_BY
            FROM BRM_RULES
            WHERE (RULE_NAME LIKE ? OR RULE_SQL LIKE ?)
            ORDER BY RULE_ID DESC
            LIMIT 1000
            """,(f"%{query}%",f"%{query}%"))
        else:
            c.execute("""
            SELECT RULE_ID,RULE_NAME,RULE_SQL,STATUS,VERSION,CREATED_BY
            FROM BRM_RULES
            ORDER BY RULE_ID DESC
            LIMIT 1000
            """)
        rows=c.fetchall()
        self.results_view.setRowCount(0)
        for rd in rows:
            r=self.results_view.rowCount()
            self.results_view.insertRow(r)
            self.results_view.setItem(r,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.results_view.setItem(r,1,QTableWidgetItem(rd["RULE_NAME"]))
            self.results_view.setItem(r,2,QTableWidgetItem(rd["RULE_SQL"]))
            self.results_view.setItem(r,3,QTableWidgetItem(rd["STATUS"]))
            self.results_view.setItem(r,4,QTableWidgetItem(str(rd["VERSION"])))
            self.results_view.setItem(r,5,QTableWidgetItem(rd["CREATED_BY"]))

##############################################################################
# RULE DASHBOARD
##############################################################################
class RuleDashboard(QGroupBox):
    def __init__(self, connection, user_id, user_group, parent=None):
        super().__init__("Rule Dashboard",parent)
        self.connection=connection
        self.user_id=user_id
        self.user_group=user_group
        self.selected_rule_id=None
        self.current_page=1
        self.records_per_page=50
        self.total_pages=1
        self.main_app=None

        layout=QVBoxLayout(self)
        fl=QHBoxLayout()

        self.search_edit=QLineEdit()
        self.search_edit.setPlaceholderText("Search rules by name or SQL...")
        fl.addWidget(QLabel("Search:"))
        fl.addWidget(self.search_edit)

        self.status_filter=QComboBox()
        self.status_filter.addItem("All Statuses",None)
        self.status_filter.addItem("ACTIVE","ACTIVE")
        self.status_filter.addItem("INACTIVE","INACTIVE")
        self.status_filter.addItem("DELETED","DELETED")
        fl.addWidget(QLabel("Status:"))
        fl.addWidget(self.status_filter)

        self.creator_filter=QComboBox()
        self.creator_filter.addItem("All Creators",None)
        c=self.connection.cursor()
        c.execute("SELECT DISTINCT CREATED_BY FROM BRM_RULES")
        for rr in c.fetchall():
            self.creator_filter.addItem(rr["CREATED_BY"], rr["CREATED_BY"])
        fl.addWidget(QLabel("Creator:"))
        fl.addWidget(self.creator_filter)
        layout.addLayout(fl)

        self.rules_table=QTableWidget(0,7)
        self.rules_table.setHorizontalHeaderLabels([
            "Rule ID","Name","SQL","Status","Version","Owner Group","Created Timestamp"
        ])
        self.rules_table.horizontalHeader().setStretchLastSection(True)
        self.rules_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.rules_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.rules_table)
        self.rules_table.itemSelectionChanged.connect(self.update_selected_rule_id)

        nav_box=QHBoxLayout()
        self.prev_page_btn=QPushButton("Previous")
        self.next_page_btn=QPushButton("Next")
        self.page_label=QLabel("Page 1/1")
        nav_box.addWidget(self.prev_page_btn)
        nav_box.addWidget(self.page_label)
        nav_box.addWidget(self.next_page_btn)
        layout.addLayout(nav_box)

        btn_box=QHBoxLayout()
        ref_btn=QPushButton("Refresh")
        ref_btn.clicked.connect(self.load_rules)
        btn_box.addWidget(ref_btn)

        analytics_btn=QPushButton("Rule Analytics")
        analytics_btn.clicked.connect(self.show_analytics_popup)
        btn_box.addWidget(analytics_btn)

        btn_box.addStretch()
        layout.addLayout(btn_box)
        self.setLayout(layout)

        self.search_edit.textChanged.connect(self.load_rules)
        self.status_filter.currentIndexChanged.connect(self.load_rules)
        self.creator_filter.currentIndexChanged.connect(self.load_rules)
        self.prev_page_btn.clicked.connect(self.prev_page)
        self.next_page_btn.clicked.connect(self.next_page)
        self.load_rules()

    def show_analytics_popup(self):
        dlg=RuleAnalyticsDialog(self.connection,self)
        dlg.exec_()

    def build_filter_query(self):
        filters=[]
        params=[]
        txt=self.search_edit.text().strip()
        if txt:
            filters.append("(RULE_NAME LIKE ? OR RULE_SQL LIKE ?)")
            params.extend([f"%{txt}%",f"%{txt}%"])
        st=self.status_filter.currentData()
        if st:
            if st.upper()=="DELETED":
                filters.append("RULE_ID IN (SELECT RECORD_ID FROM BRM_AUDIT_LOG WHERE ACTION='DELETE')")
            else:
                filters.append("STATUS=?")
                params.append(st)
        cr=self.creator_filter.currentData()
        if cr:
            filters.append("CREATED_BY=?")
            params.append(cr)
        clause=" AND ".join(filters) if filters else "1"
        return clause,params

    def load_rules(self):
        c=self.connection.cursor()
        clause,params=self.build_filter_query()

        c.execute(f"SELECT COUNT(*) as ccount FROM BRM_RULES WHERE {clause}",params)
        rowc=c.fetchone()
        total=rowc["ccount"] if rowc else 0
        self.total_pages=max(1, math.ceil(total/self.records_per_page))
        if self.current_page>self.total_pages:
            self.current_page=self.total_pages
        elif self.current_page<1:
            self.current_page=1
        self.page_label.setText(f"Page {self.current_page}/{self.total_pages}")

        offset=(self.current_page-1)*self.records_per_page
        c.execute(f"""
        SELECT RULE_ID,RULE_NAME,RULE_SQL,STATUS,VERSION,OWNER_GROUP,CREATED_TIMESTAMP
        FROM BRM_RULES
        WHERE {clause}
        ORDER BY RULE_ID DESC
        LIMIT ? OFFSET ?
        """,(*params,self.records_per_page,offset))
        rows=c.fetchall()

        self.rules_table.setRowCount(0)
        for rd in rows:
            r=self.rules_table.rowCount()
            self.rules_table.insertRow(r)
            self.rules_table.setItem(r,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.rules_table.setItem(r,1,QTableWidgetItem(rd["RULE_NAME"]))
            self.rules_table.setItem(r,2,QTableWidgetItem(rd["RULE_SQL"]))
            sitem=QTableWidgetItem(rd["STATUS"])
            if rd["STATUS"].lower()=="active":
                sitem.setBackground(QColor(144,238,144))
            else:
                sitem.setBackground(QColor(255,182,193))
            self.rules_table.setItem(r,3,sitem)
            self.rules_table.setItem(r,4,QTableWidgetItem(str(rd["VERSION"])))
            self.rules_table.setItem(r,5,QTableWidgetItem(rd["OWNER_GROUP"]))
            self.rules_table.setItem(r,6,QTableWidgetItem(str(rd["CREATED_TIMESTAMP"])))

    def update_selected_rule_id(self):
        sel=self.rules_table.selectedItems()
        if not sel:
            self.selected_rule_id=None
            return
        row=sel[0].row()
        it=self.rules_table.item(row,0)
        if it:
            self.selected_rule_id=int(it.text())
        else:
            self.selected_rule_id=None

    def get_selected_rule_ids(self):
        idxs=self.rules_table.selectionModel().selectedRows()
        rids=[]
        for ix in idxs:
            row=ix.row()
            it=self.rules_table.item(row,0)
            if it:
                rids.append(int(it.text()))
        return rids

    def prev_page(self):
        if self.current_page>1:
            self.current_page-=1
            self.load_rules()

    def next_page(self):
        if self.current_page<self.total_pages:
            self.current_page+=1
            self.load_rules()

##############################################################################
# GROUP MANAGEMENT (WITH APPROVER MANAGEMENT SUBTAB)
##############################################################################
class GroupManagementTab(QWidget):
    def __init__(self, main_app, connection, user_id, user_group, parent=None):
        super().__init__(parent)
        self.main_app=main_app
        self.connection=connection
        self.user_id=user_id
        self.user_group=user_group

        if user_group!="Admin":
            ly=QVBoxLayout(self)
            ly.addWidget(QLabel("Access Denied: Only Admin can manage groups."))
            self.setLayout(ly)
            return

        main_layout=QVBoxLayout(self)
        self.tabs=QTabWidget()
        main_layout.addWidget(self.tabs)

        # 1) Groups & membership
        groups_membership_tab=QWidget()
        gm_layout=QVBoxLayout(groups_membership_tab)

        group_details_box=QGroupBox("Group Details")
        group_details_layout=QVBoxLayout(group_details_box)
        self.groups_table=QTableWidget()
        self.groups_table.setColumnCount(3)
        self.groups_table.setHorizontalHeaderLabels(["Group Name","Description","Email"])
        self.groups_table.setSortingEnabled(True)
        self.groups_table.horizontalHeader().setStretchLastSection(True)
        group_details_layout.addWidget(self.groups_table)

        group_btn_layout=QHBoxLayout()
        add_group_btn=QPushButton("Add Group")
        add_group_btn.clicked.connect(self.on_add_group)
        group_btn_layout.addWidget(add_group_btn)

        rename_group_btn=QPushButton("Rename Group")
        rename_group_btn.clicked.connect(self.on_rename_group)
        group_btn_layout.addWidget(rename_group_btn)

        del_group_btn=QPushButton("Delete Group")
        del_group_btn.clicked.connect(self.on_delete_group)
        group_btn_layout.addWidget(del_group_btn)

        backup_group_btn=QPushButton("Backup Group")
        backup_group_btn.clicked.connect(self.on_backup_group)
        group_btn_layout.addWidget(backup_group_btn)

        restore_group_btn=QPushButton("Restore Group")
        restore_group_btn.clicked.connect(self.on_restore_group)
        group_btn_layout.addWidget(restore_group_btn)

        group_btn_layout.addStretch()
        group_details_layout.addLayout(group_btn_layout)
        gm_layout.addWidget(group_details_box)

        membership_box=QGroupBox("Membership Management (User -> Group)")
        membership_layout=QVBoxLayout(membership_box)
        self.users_table=QTableWidget()
        self.users_table.setColumnCount(3)
        self.users_table.setHorizontalHeaderLabels(["User ID","Username","Group"])
        self.users_table.setSortingEnabled(True)
        self.users_table.horizontalHeader().setStretchLastSection(True)
        membership_layout.addWidget(self.users_table)

        membership_btn_layout=QHBoxLayout()
        add_user_btn=QPushButton("Add User to Group")
        add_user_btn.clicked.connect(self.on_add_user_to_group)
        membership_btn_layout.addWidget(add_user_btn)

        remove_user_btn=QPushButton("Remove User from Group")
        remove_user_btn.clicked.connect(self.on_remove_user_from_group)
        membership_btn_layout.addWidget(remove_user_btn)

        membership_btn_layout.addStretch()
        membership_layout.addLayout(membership_btn_layout)
        gm_layout.addWidget(membership_box)

        self.tabs.addTab(groups_membership_tab,"Groups & Membership")

        # 2) Permissions
        perm_tab=QWidget()
        perm_layout=QVBoxLayout(perm_tab)
        perm_box=QGroupBox("Group Permissions")
        perm_box_layout=QVBoxLayout(perm_box)

        group_dropdown_layout=QHBoxLayout()
        group_dropdown_layout.addWidget(QLabel("Select Group:"))
        self.perm_group_combo=QComboBox()
        group_dropdown_layout.addWidget(self.perm_group_combo)
        group_dropdown_layout.addStretch()
        perm_box_layout.addLayout(group_dropdown_layout)

        self.perm_table=QTableWidget()
        self.perm_table.setColumnCount(1)
        self.perm_table.setHorizontalHeaderLabels(["Target Table"])
        self.perm_table.setSortingEnabled(True)
        self.perm_table.horizontalHeader().setStretchLastSection(True)
        perm_box_layout.addWidget(self.perm_table)

        perm_btn_layout=QHBoxLayout()
        add_perm_btn=QPushButton("Add Permission")
        add_perm_btn.clicked.connect(self.on_add_permission)
        perm_btn_layout.addWidget(add_perm_btn)

        remove_perm_btn=QPushButton("Remove Permission")
        remove_perm_btn.clicked.connect(self.on_remove_permission)
        perm_btn_layout.addWidget(remove_perm_btn)

        perm_btn_layout.addStretch()
        perm_box_layout.addLayout(perm_btn_layout)
        perm_layout.addWidget(perm_box)
        self.tabs.addTab(perm_tab,"Group Permissions")

        # 3) Approver Management
        approver_tab=QWidget()
        approver_layout=QVBoxLayout(approver_tab)

        top_h=QHBoxLayout()
        self.approver_group_combo=QComboBox()
        top_h.addWidget(QLabel("Select Group:"))
        top_h.addWidget(self.approver_group_combo)
        top_h.addStretch()
        approver_layout.addLayout(top_h)

        self.approvers_table=QTableWidget()
        self.approvers_table.setColumnCount(1)
        self.approvers_table.setHorizontalHeaderLabels(["Approver Username"])
        self.approvers_table.setSortingEnabled(True)
        self.approvers_table.horizontalHeader().setStretchLastSection(True)
        approver_layout.addWidget(self.approvers_table)

        appr_btn_layout=QHBoxLayout()
        add_appr_btn=QPushButton("Add Approver")
        add_appr_btn.clicked.connect(self.on_add_approver)
        appr_btn_layout.addWidget(add_appr_btn)

        rem_appr_btn=QPushButton("Remove Approver")
        rem_appr_btn.clicked.connect(self.on_remove_approver)
        appr_btn_layout.addWidget(rem_appr_btn)

        appr_btn_layout.addStretch()
        approver_layout.addLayout(appr_btn_layout)
        self.tabs.addTab(approver_tab,"Approver Management")

        # 4) refresh
        refresh_btn=QPushButton("Refresh All")
        refresh_btn.clicked.connect(self.load_data)
        main_layout.addWidget(refresh_btn)

        self.setLayout(main_layout)
        self.load_data()
        self.perm_group_combo.currentIndexChanged.connect(self.load_permissions)
        self.approver_group_combo.currentIndexChanged.connect(self.load_approvers)

    def load_data(self):
        self.load_groups()
        self.load_users()
        self.load_group_combo()
        self.load_approver_group_combo()

    def load_groups(self):
        c=self.connection.cursor()
        c.execute("SELECT GROUP_NAME,DESCRIPTION,EMAIL FROM BUSINESS_GROUPS ORDER BY GROUP_NAME")
        rows=c.fetchall()
        self.groups_table.setRowCount(0)
        for r in rows:
            rr=self.groups_table.rowCount()
            self.groups_table.insertRow(rr)
            self.groups_table.setItem(rr,0,QTableWidgetItem(r["GROUP_NAME"]))
            self.groups_table.setItem(rr,1,QTableWidgetItem(r["DESCRIPTION"] or ""))
            self.groups_table.setItem(rr,2,QTableWidgetItem(r["EMAIL"] or ""))

    def load_users(self):
        c=self.connection.cursor()
        c.execute("SELECT USER_ID,USERNAME,USER_GROUP FROM USERS ORDER BY USER_ID")
        rows=c.fetchall()
        self.users_table.setRowCount(0)
        for row in rows:
            rr=self.users_table.rowCount()
            self.users_table.insertRow(rr)
            self.users_table.setItem(rr,0,QTableWidgetItem(str(row["USER_ID"])))
            self.users_table.setItem(rr,1,QTableWidgetItem(row["USERNAME"]))
            self.users_table.setItem(rr,2,QTableWidgetItem(row["USER_GROUP"]))

    def load_group_combo(self):
        c=self.connection.cursor()
        c.execute("SELECT GROUP_NAME FROM BUSINESS_GROUPS ORDER BY GROUP_NAME")
        rows=c.fetchall()
        self.perm_group_combo.clear()
        for row in rows:
            self.perm_group_combo.addItem(row["GROUP_NAME"],row["GROUP_NAME"])

    def load_permissions(self):
        group=self.perm_group_combo.currentText().strip()
        c=self.connection.cursor()
        c.execute("SELECT TARGET_TABLE FROM GROUP_PERMISSIONS WHERE GROUP_NAME=?",(group,))
        rows=c.fetchall()
        self.perm_table.setRowCount(0)
        for r in rows:
            rr=self.perm_table.rowCount()
            self.perm_table.insertRow(rr)
            self.perm_table.setItem(rr,0,QTableWidgetItem(r["TARGET_TABLE"]))

    def load_approver_group_combo(self):
        c=self.connection.cursor()
        c.execute("SELECT GROUP_NAME FROM BUSINESS_GROUPS ORDER BY GROUP_NAME")
        rows=c.fetchall()
        self.approver_group_combo.clear()
        for row in rows:
            self.approver_group_combo.addItem(row["GROUP_NAME"], row["GROUP_NAME"])

    def load_approvers(self):
        grp=self.approver_group_combo.currentText().strip()
        c=self.connection.cursor()
        c.execute("SELECT USERNAME FROM BUSINESS_GROUP_APPROVERS WHERE GROUP_NAME=?",(grp,))
        rows=c.fetchall()
        self.approvers_table.setRowCount(0)
        for r in rows:
            rr=self.approvers_table.rowCount()
            self.approvers_table.insertRow(rr)
            self.approvers_table.setItem(rr,0,QTableWidgetItem(r["USERNAME"]))

    def on_add_approver(self):
        grp=self.approver_group_combo.currentText().strip()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected in Approver Management.")
            return
        username,ok=QInputDialog.getText(self,"Add Approver","Enter username:")
        if not ok or not username.strip():
            return
        c=self.connection.cursor()
        c.execute("SELECT * FROM USERS WHERE USERNAME=?",(username.strip(),))
        if not c.fetchone():
            QMessageBox.warning(self,"Not found","No such user.")
            return
        c.execute("""
        INSERT OR IGNORE INTO BUSINESS_GROUP_APPROVERS(GROUP_NAME,USERNAME)
        VALUES(?,?)
        """,(grp,username.strip()))
        c.connection.commit()
        QMessageBox.information(self,"Success",f"Approver {username} added for {grp}.")
        self.load_approvers()

    def on_remove_approver(self):
        grp=self.approver_group_combo.currentText().strip()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected in Approver Management.")
            return
        row=self.approvers_table.currentRow()
        if row<0:
            QMessageBox.warning(self,"No selection","No row selected in Approvers table.")
            return
        it=self.approvers_table.item(row,0)
        if not it:
            return
        username=it.text().strip()
        confirm=QMessageBox.question(self,"Confirm",f"Remove {username} from {grp} approvers?")
        if confirm!=QMessageBox.Yes:
            return
        c=self.connection.cursor()
        c.execute("""
        DELETE FROM BUSINESS_GROUP_APPROVERS
        WHERE GROUP_NAME=? AND USERNAME=?
        """,(grp,username))
        c.connection.commit()
        QMessageBox.information(self,"Success",f"Approver {username} removed from {grp}.")
        self.load_approvers()

    def get_selected_group(self):
        idx=self.groups_table.currentRow()
        if idx<0:
            return None
        it=self.groups_table.item(idx,0)
        if not it:
            return None
        return it.text().strip()

    def on_add_group(self):
        name,ok=QInputDialog.getText(self,"Add Group","Group Name:")
        if not ok or not name.strip():
            return
        desc,ok2=QInputDialog.getText(self,"Add Group","Description:")
        if not ok2:
            desc=""
        email,ok3=QInputDialog.getText(self,"Add Group","Email:")
        if not ok3:
            email=""
        name=name.strip()
        if not name:
            return
        c=self.connection.cursor()
        c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?",(name,))
        if c.fetchone():
            QMessageBox.warning(self,"Error","Group already exists.")
            return
        c.execute("""
        INSERT INTO BUSINESS_GROUPS(GROUP_NAME,DESCRIPTION,EMAIL)
        VALUES(?,?,?)
        """,(name,desc.strip(),email.strip()))
        c.connection.commit()
        QMessageBox.information(self,"Success","Group added.")
        self.load_data()

    def on_rename_group(self):
        grp=self.get_selected_group()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        new_name,ok=QInputDialog.getText(self,"Rename Group","New group name:")
        if not ok or not new_name.strip():
            return
        new_name=new_name.strip()
        c=self.connection.cursor()
        c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?",(new_name,))
        if c.fetchone():
            QMessageBox.warning(self,"Error","New group name already exists.")
            return
        try:
            c.execute("BEGIN")
            c.execute("UPDATE BUSINESS_GROUPS SET GROUP_NAME=? WHERE GROUP_NAME=?",(new_name,grp))
            c.execute("UPDATE BRM_RULES SET OWNER_GROUP=? WHERE OWNER_GROUP=?",(new_name,grp))
            c.execute("UPDATE BRM_RULE_GROUPS SET GROUP_NAME=? WHERE GROUP_NAME=?",(new_name,grp))
            c.execute("UPDATE BUSINESS_GROUP_APPROVERS SET GROUP_NAME=? WHERE GROUP_NAME=?",(new_name,grp))
            c.execute("COMMIT")

            add_audit_log(self.connection,"RENAME_GROUP","BUSINESS_GROUPS",grp,"Admin",
                          {"old_group_name":grp},
                          {"new_group_name":new_name})
            QMessageBox.information(self,"Success",f"Group renamed to {new_name}")
            self.load_data()
        except Exception as e:
            c.execute("ROLLBACK")
            QMessageBox.critical(self,"DB Error",str(e))

    def on_delete_group(self):
        grp=self.get_selected_group()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        confirm=QMessageBox.question(self,"Confirm",f"Delete group '{grp}'?")
        if confirm!=QMessageBox.Yes:
            return
        c=self.connection.cursor()
        try:
            c.execute("DELETE FROM BUSINESS_GROUPS WHERE GROUP_NAME=?",(grp,))
            c.connection.commit()
            QMessageBox.information(self,"Success","Group deleted.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self,"DB Error",str(e))

    def on_backup_group(self):
        grp=self.get_selected_group()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        try:
            ver=backup_group(self.connection,grp,"Admin")
            QMessageBox.information(self,"Backup Created",f"Group '{grp}' backed up as version {ver}.")
        except Exception as e:
            QMessageBox.critical(self,"DB Error",str(e))

    def on_restore_group(self):
        grp=self.get_selected_group()
        if not grp:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        c=self.connection.cursor()
        c.execute("""
        SELECT BACKUP_VERSION,BACKUP_TIMESTAMP
        FROM BRM_GROUP_BACKUPS
        WHERE GROUP_NAME=?
        ORDER BY BACKUP_VERSION DESC
        """,(grp,))
        rows=c.fetchall()
        if not rows:
            QMessageBox.information(self,"No Backups",f"No backups exist for '{grp}'")
            return
        items=[f"Version {row['BACKUP_VERSION']} (created {row['BACKUP_TIMESTAMP']})" for row in rows]
        sel,ok=QInputDialog.getItem(self,"Restore Group","Choose backup version:",items,0,False)
        if not ok:
            return
        match=re.search(r"Version\s+(\d+)",sel)
        if not match:
            return
        chosen_ver=int(match.group(1))

        confirm=QMessageBox.question(self,"Restore",
            f"Restore group '{grp}' to version {chosen_ver}? Overwrites current rules.")
        if confirm!=QMessageBox.Yes:
            return
        try:
            restore_group(self.connection,grp,chosen_ver,"Admin")
            QMessageBox.information(self,"Restored",f"Group '{grp}' restored to version {chosen_ver}.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self,"DB Error",str(e))

    def get_selected_user(self):
        idx=self.users_table.currentRow()
        if idx<0:
            return None
        it=self.users_table.item(idx,0)
        if not it:
            return None
        try:
            return int(it.text())
        except:
            return None

    def on_add_user_to_group(self):
        uid=self.get_selected_user()
        if not uid:
            QMessageBox.warning(self,"No selection","No user selected.")
            return
        grp,ok=QInputDialog.getText(self,"Add to Group","Enter group name:")
        if not ok or not grp.strip():
            return
        c=self.connection.cursor()
        c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?",(grp.strip(),))
        if not c.fetchone():
            QMessageBox.warning(self,"Error","Group not found.")
            return
        c.execute("SELECT * FROM USERS WHERE USER_ID=?",(uid,))
        user_data=c.fetchone()
        if not user_data:
            QMessageBox.warning(self,"Error","User not found.")
            return
        if user_data["USER_GROUP"]==grp.strip():
            QMessageBox.warning(self,"Error","User already in that group.")
            return
        c.execute("UPDATE USERS SET USER_GROUP=? WHERE USER_ID=?",(grp.strip(),uid))
        c.connection.commit()
        QMessageBox.information(self,"Success","User added to group.")
        self.load_data()

    def on_remove_user_from_group(self):
        uid=self.get_selected_user()
        if not uid:
            QMessageBox.warning(self,"No selection","No user selected.")
            return
        confirm=QMessageBox.question(self,"Confirm","Remove user from group?")
        if confirm!=QMessageBox.Yes:
            return
        c=self.connection.cursor()
        c.execute("UPDATE USERS SET USER_GROUP='BG1' WHERE USER_ID=?",(uid,))
        c.connection.commit()
        QMessageBox.information(self,"Success","User removed from group (now BG1).")
        self.load_data()

    def on_add_permission(self):
        group=self.perm_group_combo.currentText().strip()
        if not group:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        table,ok=QInputDialog.getText(self,"Add Permission","Enter target table:")
        if not ok or not table.strip():
            return
        c=self.connection.cursor()
        c.execute("""
        INSERT OR IGNORE INTO GROUP_PERMISSIONS(GROUP_NAME,TARGET_TABLE)
        VALUES(?,?)
        """,(group,table.strip()))
        c.connection.commit()
        QMessageBox.information(self,"Success",f"Permission '{table.strip()}' added to '{group}'.")
        self.load_permissions()

    def on_remove_permission(self):
        group=self.perm_group_combo.currentText().strip()
        if not group:
            QMessageBox.warning(self,"No selection","No group selected.")
            return
        row=self.perm_table.currentRow()
        if row<0:
            QMessageBox.warning(self,"No selection","No permission row selected.")
            return
        it=self.perm_table.item(row,0)
        if not it:
            QMessageBox.warning(self,"No selection","No table in selected row.")
            return
        tbl=it.text().strip()
        confirm=QMessageBox.question(self,"Confirm",f"Remove '{tbl}' from '{group}'?")
        if confirm!=QMessageBox.Yes:
            return
        c=self.connection.cursor()
        c.execute("DELETE FROM GROUP_PERMISSIONS WHERE GROUP_NAME=? AND TARGET_TABLE=?",(group,tbl))
        c.connection.commit()
        QMessageBox.information(self,"Success",f"Permission '{tbl}' removed from '{group}'.")
        self.load_permissions()

##############################################################################
# CUSTOM GROUPS (SAME LOGIC)
##############################################################################
class CustomRuleGroupTab(QWidget):
    def __init__(self, main_app, connection, user_id, user_group, parent=None):
        super().__init__(parent)
        self.main_app=main_app
        self.connection=connection
        self.user_id=user_id
        self.user_group=user_group

        layout=QVBoxLayout(self)
        top_h=QHBoxLayout()
        self.new_group_name_edit=QLineEdit()
        self.new_group_name_edit.setPlaceholderText("Custom Group Name")
        top_h.addWidget(self.new_group_name_edit)
        create_btn=QPushButton("Create Custom Group")
        create_btn.clicked.connect(self.create_custom_group)
        top_h.addWidget(create_btn)
        layout.addLayout(top_h)

        self.custom_group_table=QTableWidget()
        self.custom_group_table.setColumnCount(3)
        self.custom_group_table.setHorizontalHeaderLabels(["Group ID","Group Name","Owner BG"])
        self.custom_group_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.custom_group_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.custom_group_table.itemSelectionChanged.connect(self.on_custom_group_selected)
        layout.addWidget(self.custom_group_table)

        hsplit=QSplitter()
        hsplit.setOrientation(Qt.Horizontal)

        self.rule_list=QTableWidget(0,3)
        self.rule_list.setHorizontalHeaderLabels(["Rule ID","Name","Owner BG"])
        self.rule_list.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.rule_list.setEditTriggers(QAbstractItemView.NoEditTriggers)

        middle_panel=QVBoxLayout()
        self.add_rule_btn=QPushButton("Add Rule →")
        self.add_rule_btn.clicked.connect(self.add_rule_to_custom_group)
        self.remove_rule_btn=QPushButton("← Remove Rule")
        self.remove_rule_btn.clicked.connect(self.remove_rule_from_custom_group)
        middle_panel.addWidget(self.add_rule_btn)
        middle_panel.addWidget(self.remove_rule_btn)
        middle_panel.addStretch()
        mid_widget=QWidget()
        mid_widget.setLayout(middle_panel)

        self.group_members_view=QTableWidget(0,3)
        self.group_members_view.setHorizontalHeaderLabels(["Rule ID","Name","Owner BG"])
        self.group_members_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.group_members_view.setEditTriggers(QAbstractItemView.NoEditTriggers)

        left_widget=QWidget()
        left_layout=QVBoxLayout(left_widget)
        left_layout.addWidget(self.rule_list)
        hsplit.addWidget(left_widget)

        mid_w=QWidget()
        mid_w.setLayout(middle_panel)
        hsplit.addWidget(mid_w)

        right_widget=QWidget()
        right_layout=QVBoxLayout(right_widget)
        right_layout.addWidget(self.group_members_view)
        hsplit.addWidget(right_widget)

        layout.addWidget(hsplit)

        br_box=QHBoxLayout()
        backup_btn=QPushButton("Backup Custom Group")
        backup_btn.clicked.connect(self.backup_selected_group)
        restore_btn=QPushButton("Restore Custom Group")
        restore_btn.clicked.connect(self.restore_selected_group)
        br_box.addWidget(backup_btn)
        br_box.addWidget(restore_btn)
        br_box.addStretch()
        layout.addLayout(br_box)
        self.setLayout(layout)

        self.load_custom_groups()
        self.load_all_rules()

    def create_custom_group(self):
        grp_name=self.new_group_name_edit.text().strip()
        if not grp_name:
            QMessageBox.warning(self,"Error","Please enter a custom group name.")
            return
        c=self.connection.cursor()
        c.execute("""
        INSERT INTO BRM_CUSTOM_RULE_GROUPS(CUSTOM_GROUP_NAME,OWNER_BUSINESS_GROUP,CREATED_BY)
        VALUES(?,?,?)
        """,(grp_name,self.user_group,f"UserID:{self.user_id}"))
        c.connection.commit()
        QMessageBox.information(self,"Created",f"Custom group '{grp_name}' created.")
        self.new_group_name_edit.clear()
        self.load_custom_groups()

    def load_custom_groups(self):
        c=self.connection.cursor()
        c.execute("""
        SELECT CUSTOM_GROUP_ID,CUSTOM_GROUP_NAME,OWNER_BUSINESS_GROUP
        FROM BRM_CUSTOM_RULE_GROUPS
        ORDER BY CUSTOM_GROUP_ID DESC
        """)
        rows=c.fetchall()
        self.custom_group_table.setRowCount(0)
        for row in rows:
            r=self.custom_group_table.rowCount()
            self.custom_group_table.insertRow(r)
            self.custom_group_table.setItem(r,0,QTableWidgetItem(str(row["CUSTOM_GROUP_ID"])))
            self.custom_group_table.setItem(r,1,QTableWidgetItem(row["CUSTOM_GROUP_NAME"]))
            self.custom_group_table.setItem(r,2,QTableWidgetItem(row["OWNER_BUSINESS_GROUP"]))
        self.group_members_view.setRowCount(0)

    def load_all_rules(self):
        c=self.connection.cursor()
        c.execute("""
        SELECT RULE_ID,RULE_NAME,OWNER_GROUP
        FROM BRM_RULES
        ORDER BY RULE_ID DESC
        """)
        rows=c.fetchall()
        self.rule_list.setRowCount(0)
        for rd in rows:
            r=self.rule_list.rowCount()
            self.rule_list.insertRow(r)
            self.rule_list.setItem(r,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.rule_list.setItem(r,1,QTableWidgetItem(rd["RULE_NAME"]))
            self.rule_list.setItem(r,2,QTableWidgetItem(rd["OWNER_GROUP"]))

    def on_custom_group_selected(self):
        sel=self.custom_group_table.selectedItems()
        if not sel:
            self.group_members_view.setRowCount(0)
            return
        row=sel[0].row()
        cgid_item=self.custom_group_table.item(row,0)
        if not cgid_item:
            self.group_members_view.setRowCount(0)
            return
        cgid=int(cgid_item.text())
        self.load_custom_group_members(cgid)

    def load_custom_group_members(self, cgid):
        c=self.connection.cursor()
        c.execute("""
        SELECT R.RULE_ID,R.RULE_NAME,R.OWNER_GROUP
        FROM BRM_CUSTOM_GROUP_MEMBERS M
        JOIN BRM_RULES R ON M.RULE_ID=R.RULE_ID
        WHERE M.CUSTOM_GROUP_ID=?
        ORDER BY R.RULE_ID
        """,(cgid,))
        rows=c.fetchall()
        self.group_members_view.setRowCount(0)
        for rd in rows:
            rr=self.group_members_view.rowCount()
            self.group_members_view.insertRow(rr)
            self.group_members_view.setItem(rr,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.group_members_view.setItem(rr,1,QTableWidgetItem(rd["RULE_NAME"]))
            self.group_members_view.setItem(rr,2,QTableWidgetItem(rd["OWNER_GROUP"]))

    def get_selected_custom_group_id(self):
        sel=self.custom_group_table.selectedItems()
        if not sel:
            return None
        row=sel[0].row()
        cgid_item=self.custom_group_table.item(row,0)
        if not cgid_item:
            return None
        return int(cgid_item.text())

    def add_rule_to_custom_group(self):
        cgid=self.get_selected_custom_group_id()
        if not cgid:
            QMessageBox.warning(self,"No Selection","No custom group selected.")
            return
        sel_rows=self.rule_list.selectionModel().selectedRows()
        if not sel_rows:
            QMessageBox.warning(self,"No Selection","No rule selected from 'All Rules'.")
            return
        c=self.connection.cursor()
        added_count=0
        for sr in sel_rows:
            row=sr.row()
            rid_item=self.rule_list.item(row,0)
            if not rid_item:
                continue
            rid=int(rid_item.text())
            try:
                c.execute("""
                INSERT OR IGNORE INTO BRM_CUSTOM_GROUP_MEMBERS(CUSTOM_GROUP_ID,RULE_ID)
                VALUES(?,?)
                """,(cgid,rid))
                added_count+=1
            except Exception as e:
                logger.error(str(e))
        c.connection.commit()
        QMessageBox.information(self,"Rules Added",f"{added_count} rule(s) added.")
        self.load_custom_group_members(cgid)

    def remove_rule_from_custom_group(self):
        cgid=self.get_selected_custom_group_id()
        if not cgid:
            QMessageBox.warning(self,"No Selection","No custom group selected.")
            return
        sel_rows=self.group_members_view.selectionModel().selectedRows()
        if not sel_rows:
            QMessageBox.warning(self,"No Selection","No rule selected in group membership list.")
            return
        c=self.connection.cursor()
        removed_count=0
        for sr in sel_rows:
            row=sr.row()
            rid_item=self.group_members_view.item(row,0)
            if not rid_item:
                continue
            rid=int(rid_item.text())
            try:
                c.execute("""
                DELETE FROM BRM_CUSTOM_GROUP_MEMBERS
                WHERE CUSTOM_GROUP_ID=? AND RULE_ID=?
                """,(cgid,rid))
                removed_count+=1
            except Exception as e:
                logger.error(str(e))
        c.connection.commit()
        QMessageBox.information(self,"Rules Removed",f"{removed_count} rule(s) removed.")
        self.load_custom_group_members(cgid)

    def backup_selected_group(self):
        cgid=self.get_selected_custom_group_id()
        if not cgid:
            QMessageBox.warning(self,"No Selection","No custom group selected.")
            return
        try:
            ver=backup_custom_group(self.connection,cgid,action_by=f"User:{self.user_id}")
            QMessageBox.information(self,"Backup Complete",f"Backup version {ver} created.")
        except Exception as e:
            QMessageBox.critical(self,"Backup Error",str(e))

    def restore_selected_group(self):
        cgid=self.get_selected_custom_group_id()
        if not cgid:
            QMessageBox.warning(self,"No Selection","No custom group selected.")
            return
        c=self.connection.cursor()
        c.execute("""
        SELECT BACKUP_VERSION,BACKUP_TIMESTAMP
        FROM BRM_CUSTOM_GROUP_BACKUPS
        WHERE CUSTOM_GROUP_ID=?
        ORDER BY BACKUP_VERSION DESC
        """,(cgid,))
        rows=c.fetchall()
        if not rows:
            QMessageBox.information(self,"No Backups",f"No backups exist for custom group {cgid}")
            return
        items=[f"Version {r['BACKUP_VERSION']} (ts {r['BACKUP_TIMESTAMP']})" for r in rows]
        sel,ok=QInputDialog.getItem(self,"Restore","Choose backup version:",items,0,False)
        if not ok:
            return
        m=re.search(r"Version\s+(\d+)",sel)
        if not m:
            return
        chosen_ver=int(m.group(1))
        confirm=QMessageBox.question(self,"Restore",
            f"Restore custom group {cgid} to version {chosen_ver}? Overwrites rules in group.")
        if confirm!=QMessageBox.Yes:
            return
        try:
            restore_custom_group(self.connection,cgid,chosen_ver,action_by=f"User:{self.user_id}")
            QMessageBox.information(self,"Restore Done",f"Custom group {cgid} restored to version {chosen_ver}.")
            self.load_custom_group_members(cgid)
        except Exception as e:
            QMessageBox.critical(self,"Restore Error",str(e))

##############################################################################
# CTRL_TBL TAB (BROWSE DB)
##############################################################################
class CtrlTablesTab(QWidget):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        v=QVBoxLayout(self)

        self.table_list=[
            "USERS",
            "BUSINESS_GROUPS",
            "GROUP_PERMISSIONS",
            "BRM_RULE_TYPES",
            "BRM_RULE_GROUPS",
            "BRM_RULES",
            "BRM_RULE_TABLE_DEPENDENCIES",
            "BRM_AUDIT_LOG",
            "BRM_RULE_LINEAGE",
            "BRM_GROUP_BACKUPS",
            "BRM_COLUMN_MAPPING",
            "BRM_CUSTOM_RULE_GROUPS",
            "BRM_CUSTOM_GROUP_MEMBERS",
            "BUSINESS_GROUP_APPROVERS",
            "BRM_RULE_APPROVALS"
        ]

        self.table_combo=QComboBox()
        for t in self.table_list:
            self.table_combo.addItem(t)
        v.addWidget(QLabel("Select Table:"))
        v.addWidget(self.table_combo)

        self.load_btn=QPushButton("Load Data")
        self.load_btn.clicked.connect(self.on_load_data)
        v.addWidget(self.load_btn)

        self.table_view=QTableWidget(0,0)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.table_view)
        self.setLayout(v)

    def on_load_data(self):
        tbl=self.table_combo.currentText()
        if not tbl:
            return
        c=self.connection.cursor()
        c.execute(f"PRAGMA table_info({tbl})")
        info=c.fetchall()
        col_names=[x["name"] for x in info]
        if not col_names:
            # fallback
            c.execute(f"SELECT * FROM {tbl} LIMIT 1")
            col_names=[desc[0] for desc in c.description]
        c.execute(f"SELECT * FROM {tbl}")
        rows=c.fetchall()

        self.table_view.setRowCount(0)
        self.table_view.setColumnCount(len(col_names))
        self.table_view.setHorizontalHeaderLabels(col_names)

        for rd in rows:
            r=self.table_view.rowCount()
            self.table_view.insertRow(r)
            for j,cn in enumerate(col_names):
                val=rd[cn]
                self.table_view.setItem(r,j,QTableWidgetItem(str(val) if val is not None else ""))

##############################################################################
# LOGIN DIALOG
##############################################################################
class LoginDialog(QDialog):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.user_id=None
        self.user_group=None
        self.setWindowTitle("Login")
        self.resize(300,200)

        layout=QVBoxLayout(self)
        self.username_edit=QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        layout.addWidget(QLabel("Username:"))
        layout.addWidget(self.username_edit)

        self.password_edit=QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        layout.addWidget(QLabel("Password:"))
        layout.addWidget(self.password_edit)

        btn=QPushButton("Login")
        btn.clicked.connect(self.authenticate)
        layout.addWidget(btn)
        self.setLayout(layout)

    def authenticate(self):
        username=self.username_edit.text().strip()
        password=self.password_edit.text().strip()
        if not username or not password:
            QMessageBox.warning(self,"Input Error","Please enter both username and password.")
            return
        c=self.connection.cursor()
        c.execute("SELECT USER_ID,USER_GROUP FROM USERS WHERE USERNAME=? AND PASSWORD=?",(username,password))
        row=c.fetchone()
        if row:
            self.user_id=row["USER_ID"]
            self.user_group=row["USER_GROUP"]
            self.accept()
        else:
            QMessageBox.warning(self,"Login Failed","Invalid username or password.")

##############################################################################
# BUSINESSRULEMANAGEMENTTAB
##############################################################################
class BusinessRuleManagementTab(QWidget):
    def __init__(self, main_app, connection, user_id, user_group, parent=None):
        super().__init__(parent)
        self.main_app=main_app
        self.connection=connection
        self.user_id=user_id
        self.user_group=user_group

        v=QVBoxLayout(self)
        crud_box=QHBoxLayout()

        add_btn=QPushButton("Add Rule")
        add_btn.clicked.connect(self.on_add_rule)
        crud_box.addWidget(add_btn)

        up_btn=QPushButton("Update Rule")
        up_btn.clicked.connect(self.on_update_rule)
        crud_box.addWidget(up_btn)

        deact_btn=QPushButton("Deactivate Selected")
        deact_btn.clicked.connect(self.on_deactivate_rules)
        crud_box.addWidget(deact_btn)

        del_btn=QPushButton("Delete Rule")
        del_btn.clicked.connect(self.on_delete_rule)
        crud_box.addWidget(del_btn)

        aud_btn=QPushButton("View Audit Logs")
        aud_btn.clicked.connect(self.main_app.launch_audit_log_viewer)
        crud_box.addWidget(aud_btn)

        sr_btn=QPushButton("Search Rules")
        sr_btn.clicked.connect(self.main_app.launch_search_rule_dialog)
        crud_box.addWidget(sr_btn)

        crud_box.addStretch()
        v.addLayout(crud_box)

        self.rule_dashboard=RuleDashboard(self.connection,self.user_id,self.user_group)
        self.rule_dashboard.main_app=self.main_app
        v.addWidget(self.rule_dashboard)
        v.addStretch()
        self.setLayout(v)

    def on_add_rule(self):
        rtypes=self.main_app.get_rule_types()
        dlg=RuleEditorDialog(self.connection,rtypes,self.user_group,rule_data=None,parent=self)
        if dlg.exec_()==QDialog.Accepted:
            self.rule_dashboard.load_rules()

    def on_update_rule(self):
        rid=self.rule_dashboard.selected_rule_id
        if not rid:
            QMessageBox.warning(self,"No Selection","Pick a rule to update.")
            return
        c=self.connection.cursor()
        c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rid,))
        row=c.fetchone()
        if not row:
            QMessageBox.warning(self,"Not Found","Rule not found.")
            return
        rule_data=dict(row)
        rtypes=self.main_app.get_rule_types()
        dlg=RuleEditorDialog(self.connection,rtypes,self.user_group,rule_data=rule_data,parent=self)
        if dlg.exec_()==QDialog.Accepted:
            self.rule_dashboard.load_rules()

    def on_deactivate_rules(self):
        rids=self.rule_dashboard.get_selected_rule_ids()
        if not rids:
            QMessageBox.warning(self,"No Selection","No rules selected.")
            return
        success=0
        fails=[]
        c=self.connection.cursor()
        for rid in rids:
            c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rid,))
            old=c.fetchone()
            if not old:
                fails.append(f"Rule {rid} not found.")
                continue
            if old["STATUS"]!="ACTIVE":
                fails.append(f"Rule {rid} is not ACTIVE.")
                continue
            try:
                deactivate_rule(self.connection,rid,old["UPDATED_BY"] or old["CREATED_BY"],self.user_group)
                success+=1
            except Exception as e:
                fails.append(f"Rule {rid}: {str(e)}")
        msg=f"Deactivation done. Success: {success}"
        if fails:
            msg+="\nFailures:\n"+" \n".join(fails)
        QMessageBox.information(self,"Deactivate",msg)
        self.rule_dashboard.load_rules()

    def on_delete_rule(self):
        rid=self.rule_dashboard.selected_rule_id
        if not rid:
            QMessageBox.warning(self,"No Selection","Pick a rule to delete.")
            return
        c=self.connection.cursor()
        c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?",(rid,))
        old=c.fetchone()
        if not old:
            QMessageBox.warning(self,"Not Found","Rule not found.")
            return
        if old["STATUS"]!="INACTIVE":
            QMessageBox.warning(self,"Error","Rule must be INACTIVE first.")
            return
        action_by=old["UPDATED_BY"] or old["CREATED_BY"] or "Unknown"
        confirm=QMessageBox.question(self,"Confirm Delete",f"Delete rule ID {rid}?")
        if confirm!=QMessageBox.Yes:
            return
        try:
            delete_rule(self.connection,rid,action_by,self.user_group)
            QMessageBox.information(self,"Success","Rule deleted.")
            self.rule_dashboard.load_rules()
        except Exception as e:
            QMessageBox.critical(self,"DB Error",str(e))

##############################################################################
# APPROVAL TAB
##############################################################################
class ApprovalTab(QWidget):
    """
    Tab that shows *all* pending approvals for the current user,
    plus a multi-group summary: which groups are still holding approval for each rule.
    """
    def __init__(self, connection, logged_in_username, user_group, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.logged_in_username=logged_in_username
        self.user_group=user_group

        layout=QVBoxLayout(self)
        self.pending_table=QTableWidget(0,5)
        self.pending_table.setHorizontalHeaderLabels(["Rule ID","Rule Name","Group Name","Approved?","Approve"])
        self.pending_table.horizontalHeader().setStretchLastSection(True)
        self.pending_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.pending_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.pending_table)

        # a button to show multi-group status of each rule
        self.status_btn=QPushButton("Show Multi-Group Approval Status")
        self.status_btn.clicked.connect(self.show_multigroup_status)
        layout.addWidget(self.status_btn)

        ref_btn=QPushButton("Refresh Approvals")
        ref_btn.clicked.connect(self.load_approvals)
        layout.addWidget(ref_btn)
        self.setLayout(layout)
        self.load_approvals()

    def load_approvals(self):
        c=self.connection.cursor()
        c.execute("""
        SELECT A.RULE_ID,A.GROUP_NAME,A.USERNAME,A.APPROVED_FLAG,
               R.RULE_NAME,R.APPROVAL_STATUS
        FROM BRM_RULE_APPROVALS A
        JOIN BRM_RULES R ON A.RULE_ID=R.RULE_ID
        WHERE A.USERNAME=? AND A.APPROVED_FLAG=0
        ORDER BY A.RULE_ID
        """,(self.logged_in_username,))
        rows=c.fetchall()
        self.pending_table.setRowCount(0)
        for rd in rows:
            rr=self.pending_table.rowCount()
            self.pending_table.insertRow(rr)
            self.pending_table.setItem(rr,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.pending_table.setItem(rr,1,QTableWidgetItem(rd["RULE_NAME"]))
            self.pending_table.setItem(rr,2,QTableWidgetItem(rd["GROUP_NAME"]))
            self.pending_table.setItem(rr,3,QTableWidgetItem(str(rd["APPROVED_FLAG"])))
            approve_btn=QPushButton("Approve")
            approve_btn.clicked.connect(lambda _, rowx=rr:self.do_approve(rowx))
            self.pending_table.setCellWidget(rr,4,approve_btn)

    def do_approve(self,row_index):
        rid_item=self.pending_table.item(row_index,0)
        grp_item=self.pending_table.item(row_index,2)
        if not rid_item or not grp_item:
            return
        rule_id=int(rid_item.text())
        group_name=grp_item.text()
        c=self.connection.cursor()
        c.execute("""
        UPDATE BRM_RULE_APPROVALS
        SET APPROVED_FLAG=1,APPROVED_TIMESTAMP=CURRENT_TIMESTAMP
        WHERE RULE_ID=? AND GROUP_NAME=? AND USERNAME=?
        """,(rule_id,group_name,self.logged_in_username))
        c.connection.commit()

        # check if all are approved
        c.execute("""
        SELECT COUNT(*) as pending
        FROM BRM_RULE_APPROVALS
        WHERE RULE_ID=? AND APPROVED_FLAG=0
        """,(rule_id,))
        rowp=c.fetchone()
        if rowp and rowp["pending"]==0:
            c.execute("""
            UPDATE BRM_RULES
            SET APPROVAL_STATUS='APPROVED',STATUS='ACTIVE'
            WHERE RULE_ID=?
            """,(rule_id,))
            c.connection.commit()
            QMessageBox.information(self,"Approved",f"Rule {rule_id} fully approved. Now ACTIVE.")
        else:
            QMessageBox.information(self,"Approved",f"You approved rule {rule_id}. Others pending.")
        self.load_approvals()

    def show_multigroup_status(self):
        """
        Show a dialog that displays for each rule that's not fully approved,
        which groups have/haven't approved, and who is holding the approval.
        """
        dlg=MultiGroupApprovalStatusDialog(self.connection,self)
        dlg.exec_()

class MultiGroupApprovalStatusDialog(QDialog):
    """
    Displays a summary of each rule that is not fully approved,
    listing each group that must approve, plus whether it's approved and by whom.
    """
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.setWindowTitle("Multi-Group Approval Status")
        self.resize(900,500)

        v=QVBoxLayout(self)
        self.table=QTableWidget(0,5)
        self.table.setHorizontalHeaderLabels(["Rule ID","Rule Name","Group Name","Approved?","Approved By"])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.table)

        ref_btn=QPushButton("Refresh")
        ref_btn.clicked.connect(self.load_data)
        v.addWidget(ref_btn)
        self.setLayout(v)
        self.load_data()

    def load_data(self):
        """
        We query all rules where APPROVAL_STATUS!='APPROVED', then
        for each rule, we fetch rows from BRM_RULE_APPROVALS to see
        which group is pending/approved.
        """
        self.table.setRowCount(0)
        c=self.connection.cursor()
        c.execute("""
        SELECT RULE_ID,RULE_NAME,APPROVAL_STATUS
        FROM BRM_RULES
        WHERE APPROVAL_STATUS!='APPROVED'
        ORDER BY RULE_ID
        """)
        rules=c.fetchall()
        for rd in rules:
            rid=rd["RULE_ID"]
            rname=rd["RULE_NAME"]

            c2=self.connection.cursor()
            c2.execute("""
            SELECT GROUP_NAME,USERNAME,APPROVED_FLAG,APPROVED_TIMESTAMP
            FROM BRM_RULE_APPROVALS
            WHERE RULE_ID=?
            ORDER BY GROUP_NAME,USERNAME
            """,(rid,))
            app_rows=c2.fetchall()
            if not app_rows:
                # means no approvals are actually required? We'll still show it
                r2=self.table.rowCount()
                self.table.insertRow(r2)
                self.table.setItem(r2,0,QTableWidgetItem(str(rid)))
                self.table.setItem(r2,1,QTableWidgetItem(rname))
                self.table.setItem(r2,2,QTableWidgetItem("(No approvals)"))
                self.table.setItem(r2,3,QTableWidgetItem("N/A"))
                self.table.setItem(r2,4,QTableWidgetItem(""))
            else:
                for app in app_rows:
                    r3=self.table.rowCount()
                    self.table.insertRow(r3)
                    self.table.setItem(r3,0,QTableWidgetItem(str(rid)))
                    self.table.setItem(r3,1,QTableWidgetItem(rname))
                    self.table.setItem(r3,2,QTableWidgetItem(app["GROUP_NAME"]))
                    approved_str="YES" if app["APPROVED_FLAG"]==1 else "NO"
                    self.table.setItem(r3,3,QTableWidgetItem(approved_str))
                    who=app["USERNAME"]
                    if app["APPROVED_FLAG"]==1 and app["APPROVED_TIMESTAMP"]:
                        who+=f" at {app['APPROVED_TIMESTAMP']}"
                    self.table.setItem(r3,4,QTableWidgetItem(who))

##############################################################################
# CONTROL TABLES (CTRL_TBL TAB)
##############################################################################
class CtrlTablesTab(QWidget):
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection=connection
        v=QVBoxLayout(self)

        self.table_list=[
            "USERS",
            "BUSINESS_GROUPS",
            "GROUP_PERMISSIONS",
            "BRM_RULE_TYPES",
            "BRM_RULE_GROUPS",
            "BRM_RULES",
            "BRM_RULE_TABLE_DEPENDENCIES",
            "BRM_AUDIT_LOG",
            "BRM_RULE_LINEAGE",
            "BRM_GROUP_BACKUPS",
            "BRM_COLUMN_MAPPING",
            "BRM_CUSTOM_RULE_GROUPS",
            "BRM_CUSTOM_GROUP_MEMBERS",
            "BUSINESS_GROUP_APPROVERS",
            "BRM_RULE_APPROVALS"
        ]
        self.table_combo=QComboBox()
        for t in self.table_list:
            self.table_combo.addItem(t)
        v.addWidget(QLabel("Select Table:"))
        v.addWidget(self.table_combo)

        self.load_btn=QPushButton("Load Data")
        self.load_btn.clicked.connect(self.on_load_data)
        v.addWidget(self.load_btn)

        self.table_view=QTableWidget(0,0)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.table_view)
        self.setLayout(v)

    def on_load_data(self):
        tbl=self.table_combo.currentText()
        if not tbl:
            return
        c=self.connection.cursor()
        c.execute(f"PRAGMA table_info({tbl})")
        info=c.fetchall()
        col_names=[x["name"] for x in info]
        if not col_names:
            # fallback
            c.execute(f"SELECT * FROM {tbl} LIMIT 1")
            col_names=[desc[0] for desc in c.description]
        c.execute(f"SELECT * FROM {tbl}")
        rows=c.fetchall()
        self.table_view.setRowCount(0)
        self.table_view.setColumnCount(len(col_names))
        self.table_view.setHorizontalHeaderLabels(col_names)
        for rd in rows:
            r=self.table_view.rowCount()
            self.table_view.insertRow(r)
            for j,cn in enumerate(col_names):
                val=rd[cn]
                self.table_view.setItem(r,j,QTableWidgetItem(str(val) if val is not None else ""))

##############################################################################
# MAIN APP
##############################################################################
class BRMTool(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BRM Master - Multi-Group Approval + Full Tabs")
        self.resize(1200,800)

        self.connection=setup_in_memory_db()
        self.login_dialog=LoginDialog(self.connection)
        if self.login_dialog.exec_()==QDialog.Accepted:
            self.user_id=self.login_dialog.user_id
            c=self.connection.cursor()
            c.execute("SELECT USERNAME,USER_GROUP FROM USERS WHERE USER_ID=?",(self.user_id,))
            row=c.fetchone()
            self.logged_in_username=row["USERNAME"]
            self.user_group=row["USER_GROUP"]
            self.init_ui()
        else:
            sys.exit()

    def init_ui(self):
        cw=QWidget()
        self.setCentralWidget(cw)
        layout=QVBoxLayout(cw)

        # If admin, show user impersonation
        if self.user_group=="Admin":
            top_h=QHBoxLayout()
            self.switch_user_combo=QComboBox()
            c=self.connection.cursor()
            c.execute("SELECT USER_ID,USERNAME,USER_GROUP FROM USERS ORDER BY USER_ID")
            for rw in c.fetchall():
                disp=f"{rw['USERNAME']} ({rw['USER_GROUP']})"
                self.switch_user_combo.addItem(disp,(rw["USER_ID"], rw["USER_GROUP"]))
            self.switch_user_btn=QPushButton("Switch User")
            self.switch_user_btn.clicked.connect(self.on_switch_user_click)
            top_h.addWidget(QLabel("Impersonate:"))
            top_h.addWidget(self.switch_user_combo)
            top_h.addWidget(self.switch_user_btn)
            top_h.addStretch()
            layout.addLayout(top_h)

        self.tabs=QTabWidget()
        layout.addWidget(self.tabs)

        # 1) Business Rule Management
        self.brm_tab=BusinessRuleManagementTab(self,self.connection,self.user_id,self.user_group)
        self.tabs.addTab(self.brm_tab,"Business Rule Management")

        # 2) Group Management if admin
        if self.user_group=="Admin":
            self.grp_tab=GroupManagementTab(self,self.connection,self.user_id,self.user_group)
            self.tabs.addTab(self.grp_tab,"Group Management")

        # 3) BFS-based lineage
        self.lineage_tab=EnhancedLineageGraphWidget(self.connection)
        lineage_container=QWidget()
        vv=QVBoxLayout(lineage_container)
        vv.addWidget(QLabel("Lineage Visualization",self))
        vv.addWidget(self.lineage_tab)
        hb2=QHBoxLayout()
        self.lineage_search_edit=QLineEdit()
        self.lineage_search_edit.setPlaceholderText("Search rule or column...")
        sr_btn=QPushButton("Search")
        sr_btn.clicked.connect(lambda: self.lineage_tab.search_nodes(self.lineage_search_edit.text()))
        rst_btn=QPushButton("Reset View")
        rst_btn.clicked.connect(self.lineage_tab.resetView)
        ref_btn=QPushButton("Refresh Graph")
        ref_btn.clicked.connect(self.lineage_tab.populate_graph)
        hb2.addWidget(self.lineage_search_edit)
        hb2.addWidget(sr_btn)
        hb2.addWidget(rst_btn)
        hb2.addWidget(ref_btn)
        hb2.addStretch()
        vv.addLayout(hb2)
        self.tabs.addTab(lineage_container,"Lineage Visualization")

        # 4) Custom Groups
        self.custom_tab=CustomRuleGroupTab(self,self.connection,self.user_id,self.user_group)
        self.tabs.addTab(self.custom_tab,"Custom Rule Groups")

        # 5) Approvals (multi-group) - each user sees pending items
        self.approval_tab=ApprovalTab(self.connection,self.logged_in_username,self.user_group)
        self.tabs.addTab(self.approval_tab,"Approvals")

        # 6) CTRL_TBL for raw db data
        self.ctrl_tab=CtrlTablesTab(self.connection)
        self.tabs.addTab(self.ctrl_tab,"CTRL_TBL")

        self.setLayout(layout)
        self.init_timer()
        self.show()

        self.lineage_tab.populate_graph()

    def on_switch_user_click(self):
        data=self.switch_user_combo.currentData()
        if not data:
            return
        new_uid,new_ug=data
        if new_uid==self.user_id:
            return
        self.user_id=new_uid
        self.user_group=new_ug
        self.init_ui()

    def init_timer(self):
        self.timer=QTimer(self)
        self.timer.timeout.connect(self.refresh_dashboard)
        self.timer.start(5000)

    def refresh_dashboard(self):
        # e.g. auto-refresh approvals
        self.approval_tab.load_approvals()

    def launch_audit_log_viewer(self):
        dlg=AuditLogViewer(self.connection,self.user_group,self)
        dlg.exec_()

    def launch_search_rule_dialog(self):
        dlg=SearchRuleDialog(self.connection,self.user_group,self)
        dlg.exec_()

    def get_rule_types(self):
        c=self.connection.cursor()
        c.execute("SELECT RULE_TYPE_NAME,RULE_TYPE_ID FROM BRM_RULE_TYPES")
        rows=c.fetchall()
        return {r["RULE_TYPE_NAME"]:r["RULE_TYPE_ID"] for r in rows}

    def closeEvent(self,event):
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        event.accept()

def main():
    app=QApplication(sys.argv)
    app.setStyle("Fusion")
    w=BRMTool()
    w.show()
    sys.exit(app.exec_())

if __name__=="__main__":
    main()
