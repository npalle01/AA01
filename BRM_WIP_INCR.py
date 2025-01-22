#!/usr/bin/env python

import sys
import sqlite3
import logging
import json
import math
import re
import smtplib
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
    QTreeWidget, QTreeWidgetItem, QButtonGroup, QRadioButton, QSplitter
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
# DETECT OP TYPE
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
# IN-MEMORY DB SETUP (WITH APPROVALS)
##############################################################################
def setup_in_memory_db():
    conn = sqlite3.connect(DB_URI, uri=True, timeout=10.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row

    # 1) CORE TABLES
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

    # 4) APPROVAL WORKFLOW TABLES
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

    # SEED data
    groups = [
        ("Admin","Admin group","admin@example.com"),
        ("BG1","Group1","bg1@example.com"),
        ("BG2","Group2","bg2@example.com"),
        ("BG3","Group3","bg3@example.com"),
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

    perms = [
        ("Admin","TABLE_A"),
        ("Admin","TABLE_B"),
        ("Admin","TABLE_C"),
        ("Admin","TABLE_D"),
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

    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(1,'DQ')")
    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(2,'DM')")

    conn.commit()
    return conn

##############################################################################
# UTILS
##############################################################################
def send_email(to_addr, subject, body):
    # Stub or real SMTP
    pass

def notify_group(conn, group_name, subject, body):
    c=conn.cursor()
    c.execute("SELECT EMAIL FROM BUSINESS_GROUPS WHERE GROUP_NAME=?",(group_name,))
    row=c.fetchone()
    if row and row["EMAIL"]:
        logger.info(f"[NOTIFY MOCK] Group={group_name} => {subject}\n{body}")

def add_audit_log(conn, action, table_name, record_id, action_by, old_data, new_data):
    c=conn.cursor()
    c.execute("""
    INSERT INTO BRM_AUDIT_LOG(ACTION,TABLE_NAME,RECORD_ID,ACTION_BY,OLD_DATA,NEW_DATA)
    VALUES(?,?,?,?,?,?)
    """,(action,table_name,str(record_id),action_by,
         json.dumps(old_data) if old_data else None,
         json.dumps(new_data) if new_data else None))
    conn.commit()

def extract_tables(sql_text):
    pattern = re.compile(r'\bFROM\s+([^\s,]+)', re.IGNORECASE)
    matches = pattern.findall(sql_text)
    result=[]
    for m in matches:
        if '.' in m:
            db,tbl=m.split('.',1)
            result.append((db,tbl))
        else:
            result.append(("DEFAULT_DB",m))
    return result

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
        curr=queue.pop()
        if curr in visited:
            continue
        visited.add(curr)
        c.execute("""
        SELECT RULE_ID
        FROM BRM_COLUMN_MAPPING
        WHERE SOURCE_RULE_ID=?
        """,(curr,))
        kids=c.fetchall()
        for k in kids:
            cid=k["RULE_ID"]
            c.execute("SELECT OWNER_GROUP FROM BRM_RULES WHERE RULE_ID=?",(cid,))
            row2=c.fetchone()
            if row2:
                impacted.add(row2["OWNER_GROUP"])
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
        notify_group(conn, grp,
                     f"Approval needed for Rule {rule_id}",
                     f"Rule ID {rule_id} might impact your group. Please approve.")
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
# CRUD (UPDATED FOR APPROVALS)
##############################################################################
def find_child_rules(conn, parent_rule_id):
    # (already defined above, keep the BFS logic if needed)
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
        cr=c.fetchall()
        for ch in cr:
            results.append(ch["RULE_ID"])
            queue.append(ch["RULE_ID"])
    return results

def add_rule(conn, rule_data, created_by, user_group):
    c=conn.cursor()
    # Force new rule to be INACTIVE until fully approved
    c.execute("""
    INSERT INTO BRM_RULES(
      GROUP_ID,PARENT_RULE_ID,RULE_TYPE_ID,RULE_NAME,RULE_SQL,
      EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,STATUS,VERSION,CREATED_BY,
      DESCRIPTION,OPERATION_TYPE,BUSINESS_JUSTIFICATION,OWNER_GROUP,
      APPROVAL_STATUS
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
      rule_data.get("GROUP_ID"),
      rule_data.get("PARENT_RULE_ID"),
      rule_data["RULE_TYPE_ID"],
      rule_data["RULE_NAME"],
      rule_data["RULE_SQL"],
      rule_data["EFFECTIVE_START_DATE"],
      rule_data.get("EFFECTIVE_END_DATE"),
      "INACTIVE",  # forced
      1,
      created_by,
      rule_data.get("DESCRIPTION"),
      rule_data["OPERATION_TYPE"],
      rule_data.get("BUSINESS_JUSTIFICATION",""),
      rule_data["OWNER_GROUP"],
      "DRAFT"
    ))
    new_id=c.lastrowid

    deps=extract_tables(rule_data["RULE_SQL"])
    for db_name,tbl_name in deps:
        c.execute("""
        INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(RULE_ID,DATABASE_NAME,TABLE_NAME,COLUMN_NAME)
        VALUES(?,?,?,?)
        """,(new_id,db_name,tbl_name,"DerivedCol"))
    add_audit_log(conn,"ADD","BRM_RULES",new_id,created_by,None,rule_data)
    conn.commit()

    # BFS => impacted => create approvals
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

    # If rule was APPROVED, we reset it to REVIEW_IN_PROGRESS so it needs new approvals
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

    # Return impacted children
    child_list = find_child_rules(conn, rule_data["RULE_ID"])
    # For the UI's existing code, we produce a list of dicts
    return [ {"RULE_ID": cid, "RULE_NAME": ""} for cid in child_list]

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
        raise ValueError("Rule must be INACTIVE first.")

    c.execute("SELECT * FROM BRM_RULES WHERE PARENT_RULE_ID=?",(rule_id,))
    kids=c.fetchall()
    if kids:
        raise ValueError("Cannot delete rule. Child rules exist.")

    c.execute("SELECT * FROM BRM_COLUMN_MAPPING WHERE SOURCE_RULE_ID=? OR RULE_ID=?",(rule_id,rule_id))
    leftover=c.fetchall()
    if leftover:
        raise ValueError("Remove or re-map column references first before deleting rule.")

    old_data=dict(old)
    c.execute("DELETE FROM BRM_RULES WHERE RULE_ID=?",(rule_id,))
    add_audit_log(conn,"DELETE","BRM_RULES",rule_id,action_by,old_data,None)
    conn.commit()

##############################################################################
# APPROVAL TAB
##############################################################################
class ApprovalTab(QWidget):
    """
    Allows each user to see & approve rules that need their sign-off.
    Once all impacted groups have approved => rule => APPROVAL_STATUS='APPROVED' => STATUS='ACTIVE'
    """
    def __init__(self, connection, logged_in_username, user_group, parent=None):
        super().__init__(parent)
        self.connection=connection
        self.logged_in_username=logged_in_username
        self.user_group=user_group

        v=QVBoxLayout(self)
        self.approval_table=QTableWidget(0,6)
        self.approval_table.setHorizontalHeaderLabels([
            "Rule ID","Group Name","Rule Name","Needs Approval?","Approved?","Approve"
        ])
        self.approval_table.horizontalHeader().setStretchLastSection(True)
        self.approval_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.approval_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.approval_table)

        ref_btn=QPushButton("Refresh Approvals")
        ref_btn.clicked.connect(self.load_approvals)
        v.addWidget(ref_btn)
        self.setLayout(v)

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
        self.approval_table.setRowCount(0)
        for rd in rows:
            r=self.approval_table.rowCount()
            self.approval_table.insertRow(r)
            self.approval_table.setItem(r,0,QTableWidgetItem(str(rd["RULE_ID"])))
            self.approval_table.setItem(r,1,QTableWidgetItem(rd["GROUP_NAME"]))
            self.approval_table.setItem(r,2,QTableWidgetItem(rd["RULE_NAME"]))
            need_txt = "YES" if rd["APPROVAL_STATUS"]!="APPROVED" else "NO"
            self.approval_table.setItem(r,3,QTableWidgetItem(need_txt))
            self.approval_table.setItem(r,4,QTableWidgetItem("0"))
            btn=QPushButton("Approve")
            btn.clicked.connect(lambda _, rowx=r: self.do_approve(rowx))
            self.approval_table.setCellWidget(r,5,btn)

    def do_approve(self, row_index):
        rid_item=self.approval_table.item(row_index,0)
        grp_item=self.approval_table.item(row_index,1)
        if not rid_item or not grp_item:
            return
        rule_id=int(rid_item.text())
        group_name=grp_item.text()
        c=self.connection.cursor()
        c.execute("""
        UPDATE BRM_RULE_APPROVALS
        SET APPROVED_FLAG=1, APPROVED_TIMESTAMP=CURRENT_TIMESTAMP
        WHERE RULE_ID=? AND GROUP_NAME=? AND USERNAME=?
        """,(rule_id,group_name,self.logged_in_username))
        c.connection.commit()

        # check if all done
        c.execute("""
        SELECT COUNT(*) as pending
        FROM BRM_RULE_APPROVALS
        WHERE RULE_ID=? AND APPROVED_FLAG=0
        """,(rule_id,))
        rowp=c.fetchone()
        if rowp and rowp["pending"]==0:
            # all approved
            c.execute("""
            UPDATE BRM_RULES
            SET APPROVAL_STATUS='APPROVED',STATUS='ACTIVE'
            WHERE RULE_ID=?
            """,(rule_id,))
            c.connection.commit()
            QMessageBox.information(self,"Approved",f"Rule {rule_id} fully approved. Now ACTIVE.")
        else:
            QMessageBox.information(self,"Approved",f"You approved rule {rule_id}. Others still pending.")
        self.load_approvals()

##############################################################################
# MAIN APPLICATION
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
            QMessageBox.warning(self,"Error","Enter both username and password.")
            return
        c=self.connection.cursor()
        c.execute("SELECT USER_ID,USER_GROUP FROM USERS WHERE USERNAME=? AND PASSWORD=?",(username,password))
        row=c.fetchone()
        if row:
            self.user_id=row["USER_ID"]
            self.user_group=row["USER_GROUP"]
            self.accept()
        else:
            QMessageBox.warning(self,"Login Failed","Invalid credentials.")

class BRMTool(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BRM Tool - Full Integrated with Approvals")
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

        # Admin impersonation
        if self.user_group=="Admin":
            top_h=QHBoxLayout()
            self.switch_user_combo=QComboBox()
            c=self.connection.cursor()
            c.execute("SELECT USER_ID,USERNAME,USER_GROUP FROM USERS ORDER BY USER_ID")
            for rw in c.fetchall():
                disp=f"{rw['USERNAME']} ({rw['USER_GROUP']})"
                self.switch_user_combo.addItem(disp,(rw["USER_ID"],rw["USER_GROUP"]))
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

        # 2) Group Management
        if self.user_group=="Admin":
            self.grp_tab=GroupManagementTab(self,self.connection,self.user_id,self.user_group)
            self.tabs.addTab(self.grp_tab,"Group Management")

        # 3) BFS-based lineage
        self.lineage_tab=EnhancedLineageGraphWidget(self.connection)
        line_container=QWidget()
        vv=QVBoxLayout(line_container)
        vv.addWidget(self.lineage_tab)
        h2=QHBoxLayout()
        self.lineage_search_edit=QLineEdit()
        self.lineage_search_edit.setPlaceholderText("Search rule or column name...")
        sr_btn=QPushButton("Search")
        sr_btn.clicked.connect(lambda: self.lineage_tab.search_nodes(self.lineage_search_edit.text()))
        rst_btn=QPushButton("Reset View")
        rst_btn.clicked.connect(self.lineage_tab.resetView)
        ref_btn=QPushButton("Refresh Graph")
        ref_btn.clicked.connect(self.lineage_tab.populate_graph)
        h2.addWidget(self.lineage_search_edit)
        h2.addWidget(sr_btn)
        h2.addWidget(rst_btn)
        h2.addWidget(ref_btn)
        h2.addStretch()
        vv.addLayout(h2)
        self.tabs.addTab(line_container,"Lineage Visualization")
        self.lineage_tab.populate_graph()

        # 4) Custom Rule Groups
        self.custom_tab=CustomRuleGroupTab(self,self.connection,self.user_id,self.user_group)
        self.tabs.addTab(self.custom_tab,"Custom Rule Groups")

        # 5) Approvals tab
        self.approval_tab=ApprovalTab(self.connection,self.logged_in_username,self.user_group)
        self.tabs.addTab(self.approval_tab,"Approvals")

        self.setLayout(layout)
        self.init_timer()
        self.show()

    def init_timer(self):
        self.timer=QTimer(self)
        self.timer.timeout.connect(self.refresh_dashboard)
        self.timer.start(5000)

    def refresh_dashboard(self):
        # e.g. auto-refresh approvals
        self.approval_tab.load_approvals()

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

    def get_rule_types(self):
        c=self.connection.cursor()
        c.execute("SELECT RULE_TYPE_NAME,RULE_TYPE_ID FROM BRM_RULE_TYPES")
        rows=c.fetchall()
        return {r["RULE_TYPE_NAME"]:r["RULE_TYPE_ID"] for r in rows}

    def launch_audit_log_viewer(self):
        dlg=AuditLogViewer(self.connection,self.user_group,self)
        dlg.exec_()

    def launch_search_rule_dialog(self):
        dlg=SearchRuleDialog(self.connection,self.user_group,self)
        dlg.exec_()

    def closeEvent(self, event):
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
