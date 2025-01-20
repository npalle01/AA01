#!/usr/bin/env python

##############################################################################
# PART 1 of 6
# DESCRIPTION:
#   1) Imports + Logging + Constants + Email
#   2) In-memory DB Setup w/ backup table (BRM_GROUP_BACKUPS)
#   3) rename_derived_column_in_children / backup_group / restore_group
#   4) CRUD logic (add_rule_sync, update_rule_sync, etc.)
#   5) Confirmation dialogs for CRUD
##############################################################################

import sys
import sqlite3
import logging
import json
import math
import smtplib
import sqlparse

from datetime import datetime
from email.mime.text import MIMEText

# PyQt5 Imports
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import (
    Qt, QDateTime, QDate, QCoreApplication, QTimer, QPointF
)
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog, QVBoxLayout, QHBoxLayout,
    QFormLayout, QPushButton, QLineEdit, QLabel, QTextEdit, QTableWidget,
    QTableWidgetItem, QMessageBox, QComboBox, QProgressDialog, QTabWidget,
    QGroupBox, QDateTimeEdit, QHeaderView, QRadioButton, QButtonGroup,
    QInputDialog, QAbstractItemView, QListWidget, QListWidgetItem, QGraphicsView
)

import pyqtgraph as pg

##############################################################################
# Logging Setup
##############################################################################
logging.basicConfig(
    filename='brmtool_pyqtgraph.log',
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)

##############################################################################
# GLOBAL CONSTANTS
##############################################################################
DB_URI = "file::memory:?cache=shared"

##############################################################################
# Email Sending
##############################################################################
def send_email(to_addr, subject, body):
    """
    Simple helper to send an email using SMTP.
    Replace smtp.example.com, username, and password with real values.
    """
    smtp_server   = "smtp.example.com"
    smtp_port     = 587
    smtp_user     = "username@example.com"
    smtp_password = "password"
    from_addr     = "noreply@example.com"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
        logger.info(f"Email sent to {to_addr} with subject '{subject}'")
    except Exception as e:
        logger.error(f"Email sending error: {e}")

##############################################################################
# SQL Parsing Helpers
##############################################################################
def parse_identifier(identifier):
    """
    e.g. "mydb.mytable" -> ("mydb", "mytable")
    If no dot found, assume ("DefaultDB", name).
    """
    name = identifier.get_real_name()
    parts = name.split(".")
    if len(parts) == 2:
        return (parts[0], parts[1])
    elif len(parts) == 1:
        return ("DefaultDB", parts[0])
    else:
        return ("Unknown", "Unknown")

def extract_tables(sql_text):
    """
    Naive table extraction using sqlparse.
    Looks for FROM / JOIN in SELECT/INSERT/UPDATE/DELETE.
    """
    from sqlparse.sql import IdentifierList, Identifier
    from sqlparse.tokens import Keyword

    parsed = sqlparse.parse(sql_text)
    tables = []
    join_keywords = {
        "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
        "INNER JOIN", "OUTER JOIN", "CROSS JOIN"
    }

    for stmt in parsed:
        stype = stmt.get_type()
        if stype not in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
            continue
        tokens = stmt.tokens
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if t.ttype is Keyword:
                kw = t.value.upper()
                if kw == "FROM" or any(kw == x or kw.endswith(x) for x in join_keywords):
                    j = i + 1
                    while j < len(tokens):
                        nt = tokens[j]
                        if isinstance(nt, IdentifierList):
                            for identifier in nt.get_identifiers():
                                tables.append(parse_identifier(identifier))
                            break
                        elif isinstance(nt, Identifier):
                            tables.append(parse_identifier(nt))
                            break
                        elif nt.ttype is Keyword:
                            break
                        j += 1
            i += 1
    return tables

##############################################################################
# DB Setup with Backup Table
##############################################################################
def setup_in_memory_db():
    conn = sqlite3.connect(DB_URI, uri=True, timeout=10.0)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row

    # Core tables
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
        PRIMARY KEY(GROUP_NAME, TARGET_TABLE)
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_TYPES(
        RULE_TYPE_ID INTEGER PRIMARY KEY,
        RULE_TYPE_NAME TEXT NOT NULL UNIQUE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULES(
        RULE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
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
        FOREIGN KEY(RULE_TYPE_ID) REFERENCES BRM_RULE_TYPES(RULE_TYPE_ID),
        FOREIGN KEY(PARENT_RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_RULE_TABLE_DEPENDENCIES(
        DEPENDENCY_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RULE_ID INTEGER NOT NULL,
        DATABASE_NAME TEXT NOT NULL,
        TABLE_NAME TEXT NOT NULL,
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
    CREATE TABLE IF NOT EXISTS BRM_COLUMN_MAPPING(
        MAPPING_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        RULE_ID INTEGER NOT NULL,
        SOURCE_TABLE TEXT NOT NULL,
        SOURCE_COLUMN TEXT NOT NULL,
        TARGET_TABLE TEXT NOT NULL,
        TARGET_COLUMN TEXT NOT NULL,
        BUSINESS_COLUMN_NAME TEXT,
        COLUMN_DESCRIPTION TEXT,
        FOREIGN KEY(RULE_ID) REFERENCES BRM_RULES(RULE_ID) ON DELETE CASCADE
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

    # NEW: Backup table
    conn.execute("""
    CREATE TABLE IF NOT EXISTS BRM_GROUP_BACKUPS(
        BACKUP_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        GROUP_NAME TEXT NOT NULL,
        BACKUP_TIMESTAMP DATETIME DEFAULT CURRENT_TIMESTAMP,
        BACKUP_VERSION INTEGER NOT NULL,
        BACKUP_JSON TEXT NOT NULL
    );
    """)

    # Seed data
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
        """, p)

    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(1,'DQ')")
    conn.execute("INSERT OR IGNORE INTO BRM_RULE_TYPES(RULE_TYPE_ID,RULE_TYPE_NAME) VALUES(2,'DM')")

    conn.commit()
    return conn

##############################################################################
# rename_derived_column_in_children
##############################################################################
def rename_derived_column_in_children(conn, parent_rule_id, old_col, new_col, updated_by):
    """
    Recursively rename `old_col` -> `new_col` in SOURCE_COLUMN among child rules.
    Also updates parent's TARGET_COLUMN if it was old_col, and re-generates
    final DML if needed. Logs changes in audit.
    """
    c = conn.cursor()
    # 1) Update parent's BRM_COLUMN_MAPPING for the old_col in TARGET_COLUMN
    c.execute("""
        UPDATE BRM_COLUMN_MAPPING
        SET TARGET_COLUMN=?
        WHERE RULE_ID=? AND TARGET_COLUMN=?
    """, (new_col, parent_rule_id, old_col))

    # 2) Find direct children
    c.execute("""
      SELECT RULE_ID
      FROM BRM_RULES
      WHERE PARENT_RULE_ID=?
    """,(parent_rule_id,))
    direct_children = c.fetchall()

    for row in direct_children:
        child_id = row["RULE_ID"]
        # rename child's SOURCE_COLUMN
        c2 = conn.cursor()
        c2.execute("""
          UPDATE BRM_COLUMN_MAPPING
          SET SOURCE_COLUMN=?
          WHERE RULE_ID=? AND SOURCE_COLUMN=?
        """,(new_col, child_id, old_col))

        # If child is DML, rebuild final SQL
        c2.execute("SELECT OPERATION_TYPE FROM BRM_RULES WHERE RULE_ID=?", (child_id,))
        child_op = c2.fetchone()
        if child_op:
            op_type = (child_op["OPERATION_TYPE"] or "").upper()
            if op_type in ["INSERT","UPDATE","DELETE"]:
                new_sql = build_final_dml_script(conn, child_id)
                c2.execute("""
                  UPDATE BRM_RULES
                  SET RULE_SQL=?, VERSION=VERSION+1, UPDATED_BY=?
                  WHERE RULE_ID=?
                """,(new_sql, updated_by, child_id))

        # Recurse deeper
        rename_derived_column_in_children(conn, child_id, old_col, new_col, updated_by)

    # 3) update parent's own DML if it's DML
    c2 = conn.cursor()
    c2.execute("SELECT OPERATION_TYPE FROM BRM_RULES WHERE RULE_ID=?", (parent_rule_id,))
    p_op = c2.fetchone()
    if p_op:
        op_type = (p_op["OPERATION_TYPE"] or "").upper()
        if op_type in ["INSERT","UPDATE","DELETE"]:
            p_new_sql = build_final_dml_script(conn, parent_rule_id)
            c2.execute("""
              UPDATE BRM_RULES
              SET RULE_SQL=?, VERSION=VERSION+1, UPDATED_BY=?
              WHERE RULE_ID=?
            """,(p_new_sql, updated_by, parent_rule_id))

    conn.commit()

    # Log
    add_audit_log(
        conn,
        "RENAME_COLUMN",
        "BRM_COLUMN_MAPPING",
        f"ParentRule:{parent_rule_id}",
        updated_by,
        {"old_column": old_col},
        {"new_column": new_col}
    )

##############################################################################
# backup_group / restore_group
##############################################################################
def backup_group(conn, group_name, action_by="System"):
    """
    Backs up all rules, mappings, etc. from a group into BRM_GROUP_BACKUPS.
    """
    c = conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE OWNER_GROUP=?", (group_name,))
    rules = c.fetchall()

    backup_data = {"rules":[]}
    for rule in rules:
        rdict = dict(rule)
        # Mappings
        c.execute("SELECT * FROM BRM_COLUMN_MAPPING WHERE RULE_ID=?", (rule["RULE_ID"],))
        rdict["mappings"] = [dict(m) for m in c.fetchall()]
        # Dependencies
        c.execute("SELECT * FROM BRM_RULE_TABLE_DEPENDENCIES WHERE RULE_ID=?", (rule["RULE_ID"],))
        rdict["dependencies"] = [dict(d) for d in c.fetchall()]
        # Lineage
        c.execute("SELECT * FROM BRM_RULE_LINEAGE WHERE RULE_ID=?", (rule["RULE_ID"],))
        rdict["lineage"] = [dict(l) for l in c.fetchall()]
        backup_data["rules"].append(rdict)

    json_str = json.dumps(backup_data, default=str)
    c.execute("SELECT COALESCE(MAX(BACKUP_VERSION),0) as maxver FROM BRM_GROUP_BACKUPS WHERE GROUP_NAME=?",(group_name,))
    row = c.fetchone()
    next_ver = row["maxver"] + 1

    c.execute("""
      INSERT INTO BRM_GROUP_BACKUPS(GROUP_NAME,BACKUP_VERSION,BACKUP_JSON)
      VALUES(?,?,?)
    """,(group_name, next_ver, json_str))
    conn.commit()

    add_audit_log(conn,"BACKUP","BRM_GROUP_BACKUPS", group_name, action_by,
                  {"group_name": group_name}, {"backup_version": next_ver})

    return next_ver

def restore_group(conn, group_name, backup_version, action_by="System"):
    """
    Restores group rules from a prior backup version.
    """
    c = conn.cursor()
    c.execute("""
      SELECT BACKUP_JSON
      FROM BRM_GROUP_BACKUPS
      WHERE GROUP_NAME=? AND BACKUP_VERSION=?
    """,(group_name, backup_version))
    row = c.fetchone()
    if not row:
        raise ValueError(f"No backup found for group {group_name} v{backup_version}")

    backup_data = json.loads(row["BACKUP_JSON"])

    # Clear existing
    c.execute("""
      DELETE FROM BRM_RULE_LINEAGE
      WHERE RULE_ID IN (SELECT RULE_ID FROM BRM_RULES WHERE OWNER_GROUP=?)
    """,(group_name,))
    c.execute("""
      DELETE FROM BRM_RULE_TABLE_DEPENDENCIES
      WHERE RULE_ID IN (SELECT RULE_ID FROM BRM_RULES WHERE OWNER_GROUP=?)
    """,(group_name,))
    c.execute("""
      DELETE FROM BRM_COLUMN_MAPPING
      WHERE RULE_ID IN (SELECT RULE_ID FROM BRM_RULES WHERE OWNER_GROUP=?)
    """,(group_name,))
    c.execute("DELETE FROM BRM_RULES WHERE OWNER_GROUP=?",(group_name,))

    # Insert from backup
    for rdict in backup_data["rules"]:
        c.execute("""
          INSERT INTO BRM_RULES(
            RULE_ID, PARENT_RULE_ID, RULE_TYPE_ID, RULE_NAME,
            RULE_SQL, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,
            STATUS, VERSION, CREATED_BY, DESCRIPTION, OPERATION_TYPE,
            BUSINESS_JUSTIFICATION, CREATED_TIMESTAMP, UPDATED_BY, OWNER_GROUP
          ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,(
            rdict["RULE_ID"], rdict["PARENT_RULE_ID"], rdict["RULE_TYPE_ID"],
            rdict["RULE_NAME"], rdict["RULE_SQL"], rdict["EFFECTIVE_START_DATE"],
            rdict["EFFECTIVE_END_DATE"], rdict["STATUS"], rdict["VERSION"],
            rdict["CREATED_BY"], rdict["DESCRIPTION"], rdict["OPERATION_TYPE"],
            rdict["BUSINESS_JUSTIFICATION"], rdict["CREATED_TIMESTAMP"],
            rdict["UPDATED_BY"], rdict["OWNER_GROUP"]
        ))

        # Mappings
        for m in rdict["mappings"]:
            c.execute("""
              INSERT INTO BRM_COLUMN_MAPPING(
                MAPPING_ID,RULE_ID,SOURCE_TABLE,SOURCE_COLUMN,
                TARGET_TABLE,TARGET_COLUMN,BUSINESS_COLUMN_NAME,
                COLUMN_DESCRIPTION
              ) VALUES(?,?,?,?,?,?,?,?)
            """,(
                m["MAPPING_ID"], m["RULE_ID"], m["SOURCE_TABLE"], m["SOURCE_COLUMN"],
                m["TARGET_TABLE"], m["TARGET_COLUMN"], m["BUSINESS_COLUMN_NAME"],
                m["COLUMN_DESCRIPTION"]
            ))

        # Dependencies
        for d in rdict["dependencies"]:
            c.execute("""
              INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(
                DEPENDENCY_ID,RULE_ID,DATABASE_NAME,TABLE_NAME
              ) VALUES(?,?,?,?)
            """,(d["DEPENDENCY_ID"], d["RULE_ID"], d["DATABASE_NAME"], d["TABLE_NAME"]))

        # Lineage
        for l in rdict["lineage"]:
            c.execute("""
              INSERT INTO BRM_RULE_LINEAGE(
                LINEAGE_ID,RULE_ID,SOURCE_INFO,TARGET_INFO,
                TRANSFORMATION_DETAILS,CREATED_TIMESTAMP
              ) VALUES(?,?,?,?,?,?)
            """,(l["LINEAGE_ID"], l["RULE_ID"], l["SOURCE_INFO"], l["TARGET_INFO"],
                 l["TRANSFORMATION_DETAILS"], l["CREATED_TIMESTAMP"]))
    conn.commit()

    # Audit
    add_audit_log(conn,"RESTORE","BRM_RULES", group_name, action_by,
                  {"group_name": group_name, "backup_version": backup_version},None)

##############################################################################
# Audit + Mapping checks + Final DML
##############################################################################
def add_audit_log(conn, action, table_name, record_id, action_by, old_data, new_data):
    c = conn.cursor()
    c.execute("""
      INSERT INTO BRM_AUDIT_LOG(ACTION,TABLE_NAME,RECORD_ID,ACTION_BY,OLD_DATA,NEW_DATA)
      VALUES(?,?,?,?,?,?)
    """,(
      action, table_name, str(record_id), action_by,
      json.dumps(old_data) if old_data else None,
      json.dumps(new_data) if new_data else None
    ))
    conn.commit()

def notify_group(conn, group_name, subject, body):
    c = conn.cursor()
    c.execute("SELECT EMAIL FROM BUSINESS_GROUPS WHERE GROUP_NAME=?", (group_name,))
    row = c.fetchone()
    if row and row["EMAIL"]:
        send_email(row["EMAIL"], subject, body)

def check_has_mapping(conn, rule_id):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as cnt FROM BRM_COLUMN_MAPPING WHERE RULE_ID=?", (rule_id,))
    row = c.fetchone()
    return (row["cnt"]>0)

def auto_extract_mapping_for_dml(conn, rule_id, rule_sql, operation_type):
    tokens = sqlparse.parse(rule_sql)
    if not tokens:
        return
    op = operation_type.upper()
    if op not in ("INSERT","UPDATE","DELETE"):
        return
    c = conn.cursor()
    c.execute("DELETE FROM BRM_COLUMN_MAPPING WHERE RULE_ID=?", (rule_id,))
    conn.commit()

    txt_up = rule_sql.strip().upper()
    # ... (same naive parse logic from the original code)...

    if txt_up.startswith("INSERT"):
        # parse naive
        insert_prefix = "INSERT INTO"
        idx = txt_up.find(insert_prefix)
        if idx >= 0:
            after_insert = rule_sql[idx+len(insert_prefix):].strip()
            bopen = after_insert.find("(")
            if bopen>0:
                table_name = after_insert[:bopen].strip()
                remainder = after_insert[bopen:]
                bclose = remainder.find(")")
                col_part = remainder[1:bclose].strip()
                targets = [x.strip() for x in col_part.split(",") if x.strip()]

                after_close = remainder[bclose+1:].strip()
                sel_idx = after_close.upper().find("SELECT")
                if sel_idx>=0:
                    after_select = after_close[sel_idx+len("SELECT"):].strip()
                    from_idx = after_select.upper().find("FROM")
                    source_expr = after_select
                    if from_idx>=0:
                        source_expr = after_select[:from_idx].strip()
                    sources = [x.strip() for x in source_expr.split(",") if x.strip()]

                    length = min(len(targets),len(sources))
                    for i in range(length):
                        tcol = targets[i]
                        scol = sources[i]
                        c.execute("""
                          INSERT INTO BRM_COLUMN_MAPPING(
                            RULE_ID,SOURCE_TABLE,SOURCE_COLUMN,
                            TARGET_TABLE,TARGET_COLUMN
                          ) VALUES(?,?,?,?,?)
                        """,(rule_id,"(Auto)",scol, table_name, tcol))
                    conn.commit()

    elif txt_up.startswith("UPDATE"):
        update_prefix = "UPDATE"
        idx = txt_up.find(update_prefix)
        if idx>=0:
            after_update = rule_sql[idx+len(update_prefix):].strip()
            set_idx = after_update.upper().find("SET")
            if set_idx>=0:
                table_name = after_update[:set_idx].strip()
                set_part = after_update[set_idx+len("SET"):].strip()
                where_idx = set_part.upper().find("WHERE")
                if where_idx<0:
                    where_idx=len(set_part)
                assignments = set_part[:where_idx].strip()
                pairs = assignments.split(",")
                for p in pairs:
                    p = p.strip()
                    eq_idx = p.find("=")
                    if eq_idx>0:
                        tcol = p[:eq_idx].strip()
                        scol = p[eq_idx+1:].strip()
                        c.execute("""
                          INSERT INTO BRM_COLUMN_MAPPING(
                            RULE_ID,SOURCE_TABLE,SOURCE_COLUMN,
                            TARGET_TABLE,TARGET_COLUMN
                          ) VALUES(?,?,?,?,?)
                        """,(rule_id,"(Auto)",scol, table_name, tcol))
                conn.commit()

    elif txt_up.startswith("DELETE"):
        delete_prefix = "DELETE FROM"
        idx = txt_up.find(delete_prefix)
        if idx>=0:
            after_delete = rule_sql[idx+len(delete_prefix):].strip()
            parts = after_delete.split()
            table_name = parts[0] if parts else "(Unknown)"
            c.execute("""
              INSERT INTO BRM_COLUMN_MAPPING(
                RULE_ID,SOURCE_TABLE,SOURCE_COLUMN,
                TARGET_TABLE,TARGET_COLUMN
              ) VALUES(?,?,?,?,?)
            """,(rule_id,"(Auto)","(all columns)", table_name,"(all columns)"))
            conn.commit()

def build_final_dml_script(conn, rule_id):
    c = conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?", (rule_id,))
    row = c.fetchone()
    if not row:
        return ""
    op_type = (row["OPERATION_TYPE"] or "").upper()
    original_sql = row["RULE_SQL"]

    c.execute("""
      SELECT SOURCE_TABLE,SOURCE_COLUMN,TARGET_TABLE,TARGET_COLUMN
      FROM BRM_COLUMN_MAPPING
      WHERE RULE_ID=?
    """,(rule_id,))
    mappings = c.fetchall()
    if not mappings:
        return original_sql

    tgt_table = mappings[0]["TARGET_TABLE"]
    if not tgt_table:
        return original_sql

    if op_type=="INSERT":
        scols=[]
        tcols=[]
        for m in mappings:
            sc = (m["SOURCE_TABLE"]+"."+m["SOURCE_COLUMN"]) if m["SOURCE_TABLE"] else m["SOURCE_COLUMN"]
            scols.append(sc)
            tcols.append(m["TARGET_COLUMN"])
        col_list = ", ".join(tcols)
        src_list = ", ".join(scols)
        return f"INSERT INTO {tgt_table} ({col_list})\nSELECT {src_list}\n/* FROM ??? WHERE ??? */"

    elif op_type=="UPDATE":
        set_list=[]
        for m in mappings:
            sc = (m["SOURCE_TABLE"]+"."+m["SOURCE_COLUMN"]) if m["SOURCE_TABLE"] else m["SOURCE_COLUMN"]
            set_list.append(f"{m['TARGET_COLUMN']} = {sc}")
        return f"UPDATE {tgt_table}\n   SET {', '.join(set_list)}\n   WHERE /* condition */"

    elif op_type=="DELETE":
        return f"DELETE FROM {tgt_table}\nWHERE /* condition */"

    return original_sql

##############################################################################
# CRUD: add_rule_sync, update_rule_sync, ...
##############################################################################

def add_rule_sync(conn, rule_data, created_by, user_group):
    c = conn.cursor()
    c.execute("""
      INSERT INTO BRM_RULES(
        PARENT_RULE_ID,RULE_TYPE_ID,RULE_NAME,RULE_SQL,EFFECTIVE_START_DATE,
        EFFECTIVE_END_DATE,STATUS,VERSION,CREATED_BY,DESCRIPTION,
        OPERATION_TYPE,BUSINESS_JUSTIFICATION,OWNER_GROUP
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
      rule_data.get("PARENT_RULE_ID"),
      rule_data["RULE_TYPE_ID"],
      rule_data["RULE_NAME"],
      rule_data["RULE_SQL"],
      rule_data["EFFECTIVE_START_DATE"],
      rule_data.get("EFFECTIVE_END_DATE"),
      rule_data["STATUS"],
      rule_data["VERSION"],
      created_by,
      rule_data.get("DESCRIPTION"),
      rule_data.get("OPERATION_TYPE"),
      rule_data.get("BUSINESS_JUSTIFICATION",""),
      rule_data["OWNER_GROUP"]
    ))
    new_id = c.lastrowid

    deps = extract_tables(rule_data["RULE_SQL"])
    for db_name, tbl_name in deps:
        c.execute("""
          INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(RULE_ID,DATABASE_NAME,TABLE_NAME)
          VALUES(?,?,?)
        """,(new_id, db_name, tbl_name))

    if rule_data["STATUS"]=="ACTIVE":
        if not check_has_mapping(conn,new_id):
            c.execute("UPDATE BRM_RULES SET STATUS='INACTIVE' WHERE RULE_ID=?", (new_id,))
            conn.commit()

    add_audit_log(conn,"ADD","BRM_RULES",new_id, created_by, None, rule_data)
    conn.commit()

    # DML => auto extract
    op_type = (rule_data.get("OPERATION_TYPE") or "").upper()
    if op_type in ("INSERT","UPDATE","DELETE"):
        auto_extract_mapping_for_dml(conn, new_id, rule_data["RULE_SQL"], op_type)
        if check_has_mapping(conn, new_id):
            final_script = build_final_dml_script(conn, new_id)
            c2 = conn.cursor()
            c2.execute("UPDATE BRM_RULES SET RULE_SQL=? WHERE RULE_ID=?", (final_script, new_id))
            c2.connection.commit()

    notify_group(conn, rule_data["OWNER_GROUP"],
                 f"Rule Added: {rule_data['RULE_NAME']}",
                 f"New rule created by {created_by}.\nGroup: {rule_data['OWNER_GROUP']}")
    return new_id

def update_rule_sync(conn, rule_data, updated_by, user_group):
    c = conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?", (rule_data["RULE_ID"],))
    old = c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    old_data = dict(old)

    if rule_data["STATUS"]=="ACTIVE":
        if not check_has_mapping(conn, rule_data["RULE_ID"]):
            raise ValueError("Cannot set rule ACTIVE. No mapping found for DML rule.")

    c.execute("""
      UPDATE BRM_RULES
      SET
        PARENT_RULE_ID=?,
        RULE_TYPE_ID=?,
        RULE_NAME=?,
        RULE_SQL=?,
        EFFECTIVE_START_DATE=?,
        EFFECTIVE_END_DATE=?,
        STATUS=?,
        VERSION=VERSION+1,
        UPDATED_BY=?,
        DESCRIPTION=?,
        OPERATION_TYPE=?,
        BUSINESS_JUSTIFICATION=?,
        OWNER_GROUP=?
      WHERE RULE_ID=?
    """,(
      rule_data.get("PARENT_RULE_ID"),
      rule_data["RULE_TYPE_ID"],
      rule_data["RULE_NAME"],
      rule_data["RULE_SQL"],
      rule_data["EFFECTIVE_START_DATE"],
      rule_data.get("EFFECTIVE_END_DATE"),
      rule_data["STATUS"],
      updated_by,
      rule_data.get("DESCRIPTION"),
      rule_data.get("OPERATION_TYPE"),
      rule_data.get("BUSINESS_JUSTIFICATION",""),
      rule_data["OWNER_GROUP"],
      rule_data["RULE_ID"]
    ))

    c.execute("DELETE FROM BRM_RULE_TABLE_DEPENDENCIES WHERE RULE_ID=?", (rule_data["RULE_ID"],))
    deps = extract_tables(rule_data["RULE_SQL"])
    for db_name,tbl_name in deps:
        c.execute("""
          INSERT INTO BRM_RULE_TABLE_DEPENDENCIES(RULE_ID,DATABASE_NAME,TABLE_NAME)
          VALUES(?,?,?)
        """,(rule_data["RULE_ID"], db_name, tbl_name))

    new_data = dict(old_data)
    for k,v in rule_data.items():
        new_data[k]=v
    new_data["VERSION"] = old["VERSION"]+1

    add_audit_log(conn,"UPDATE","BRM_RULES", rule_data["RULE_ID"], updated_by, old_data, new_data)
    conn.commit()

    op_type = (rule_data.get("OPERATION_TYPE") or "").upper()
    if op_type in ("INSERT","UPDATE","DELETE"):
        auto_extract_mapping_for_dml(conn, rule_data["RULE_ID"], rule_data["RULE_SQL"], op_type)
        if check_has_mapping(conn, rule_data["RULE_ID"]):
            final_script = build_final_dml_script(conn, rule_data["RULE_ID"])
            c2 = conn.cursor()
            c2.execute("UPDATE BRM_RULES SET RULE_SQL=? WHERE RULE_ID=?", (final_script, rule_data["RULE_ID"]))
            c2.connection.commit()

    notify_group(conn, rule_data["OWNER_GROUP"],
                 f"Rule Updated: {rule_data['RULE_NAME']}",
                 f"Rule updated by {updated_by}.")

def deactivate_rule_sync(conn, rule_id, updated_by, user_group):
    c = conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?", (rule_id,))
    old = c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    old_data = dict(old)

    c.execute("SELECT * FROM BRM_RULES WHERE PARENT_RULE_ID=? AND STATUS='ACTIVE'", (rule_id,))
    kids = c.fetchall()
    if kids:
        raise ValueError("Cannot deactivate rule. Child rules must be deactivated first.")

    c.execute("""
      UPDATE BRM_RULES
      SET STATUS='INACTIVE', UPDATED_BY=?, VERSION=VERSION+1
      WHERE RULE_ID=?
    """,(updated_by, rule_id))

    new_data = dict(old_data)
    new_data["STATUS"]="INACTIVE"
    new_data["VERSION"] = old["VERSION"]+1
    add_audit_log(conn,"DEACTIVATE","BRM_RULES", rule_id, updated_by, old_data, new_data)
    conn.commit()
    notify_group(conn, old["OWNER_GROUP"],
                 f"Rule Deactivated: {old['RULE_NAME']}",
                 f"Rule ID {rule_id} deactivated by {updated_by}.")

def delete_rule_sync(conn, rule_id, action_by, user_group):
    c = conn.cursor()
    c.execute("SELECT * FROM BRM_RULES WHERE RULE_ID=?", (rule_id,))
    old = c.fetchone()
    if not old:
        raise ValueError("Rule not found.")
    if old["STATUS"]!="INACTIVE":
        raise ValueError("Cannot delete unless rule is INACTIVE.")

    c.execute("SELECT * FROM BRM_RULES WHERE PARENT_RULE_ID=?", (rule_id,))
    kids = c.fetchall()
    if kids:
        raise ValueError("Cannot delete rule. Child rules exist.")

    c.execute("SELECT * FROM BRM_RULE_TABLE_DEPENDENCIES WHERE RULE_ID=?", (rule_id,))
    dps = c.fetchall()
    if dps:
        raise ValueError("Remove table dependencies first before deleting rule.")

    old_data = dict(old)
    c.execute("DELETE FROM BRM_RULES WHERE RULE_ID=?", (rule_id,))
    add_audit_log(conn,"DELETE","BRM_RULES", rule_id, action_by, old_data,None)
    conn.commit()
    notify_group(conn, old["OWNER_GROUP"],
                 f"Rule Deleted: {old['RULE_NAME']}",
                 f"Rule ID {rule_id} deleted by {action_by}.")

##############################################################################
# Confirmation Dialog Classes
##############################################################################
class ConfirmAddDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Confirm Add")
        self.resize(300,150)
        layout = QVBoxLayout(self)
        lab = QLabel("Are you sure you want to add this rule?")
        layout.addWidget(lab)
        btn_layout = QHBoxLayout()
        yes = QPushButton("Yes")
        yes.clicked.connect(self.accept)
        no = QPushButton("No")
        no.clicked.connect(self.reject)
        btn_layout.addWidget(yes)
        btn_layout.addWidget(no)
        layout.addLayout(btn_layout)

class ConfirmUpdateDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Confirm Update")
        self.resize(300,150)
        layout = QVBoxLayout(self)
        lab = QLabel("Are you sure you want to update this rule?")
        layout.addWidget(lab)
        btn_layout = QHBoxLayout()
        yes = QPushButton("Yes")
        yes.clicked.connect(self.accept)
        no = QPushButton("No")
        no.clicked.connect(self.reject)
        btn_layout.addWidget(yes)
        btn_layout.addWidget(no)
        layout.addLayout(btn_layout)

class ConfirmDeactivateDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Confirm Deactivate")
        self.resize(300,150)
        layout = QVBoxLayout(self)
        lab = QLabel("Are you sure you want to deactivate this rule?")
        layout.addWidget(lab)
        btn_layout = QHBoxLayout()
        yes = QPushButton("Yes")
        yes.clicked.connect(self.accept)
        no = QPushButton("No")
        no.clicked.connect(self.reject)
        btn_layout.addWidget(yes)
        btn_layout.addWidget(no)
        layout.addLayout(btn_layout)

class ConfirmDeleteDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Confirm Delete")
        self.resize(300,150)
        layout = QVBoxLayout(self)
        lab = QLabel("Are you sure you want to delete this rule?")
        layout.addWidget(lab)
        btn_layout = QHBoxLayout()
        yes = QPushButton("Yes")
        yes.clicked.connect(self.accept)
        no = QPushButton("No")
        no.clicked.connect(self.reject)
        btn_layout.addWidget(yes)
        btn_layout.addWidget(no)
        layout.addLayout(btn_layout)

# END OF PART 1
################################################################################
# PART 2 of 6
# DESCRIPTION:
#   1) LoginDialog
#   2) RuleDetailPage
#   3) EnhancedAddRuleDialog
#   4) UpdateRuleDialog (with optional rename logic)
#   5) DeactivateRuleDialog, DeleteRuleDialog
################################################################################

class LoginDialog(QDialog):
    """
    Dialog for user login. Checks the USERS table for valid username/password.
    """
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.user_id = None
        self.user_group = None
        self.setWindowTitle("Login")
        self.setFixedSize(300, 200)

        layout = QVBoxLayout(self)
        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Username")
        layout.addWidget(QLabel("Username:"))
        layout.addWidget(self.username_edit)

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        layout.addWidget(QLabel("Password:"))
        layout.addWidget(self.password_edit)

        btn = QPushButton("Login")
        btn.clicked.connect(self.authenticate)
        layout.addWidget(btn)

        self.setLayout(layout)

    def authenticate(self):
        username = self.username_edit.text().strip()
        password = self.password_edit.text().strip()
        if not username or not password:
            QMessageBox.warning(self, "Input Error", "Please enter both username and password.")
            return
        try:
            c = self.connection.cursor()
            c.execute("SELECT USER_ID, USER_GROUP FROM USERS WHERE USERNAME=? AND PASSWORD=?",
                      (username, password))
            row = c.fetchone()
            if row:
                self.user_id = row["USER_ID"]
                self.user_group = row["USER_GROUP"]
                self.accept()
            else:
                QMessageBox.warning(self, "Login Failed", "Invalid username or password.")
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))


class RuleDetailPage(QGroupBox):
    """
    A reusable widget for capturing/editing rule details (except for the RULE_ID).
    """
    def __init__(self, rule_types=None, connection=None, parent=None):
        super().__init__("Rule Details", parent)
        self.rule_types = rule_types if rule_types else {}
        self.connection = connection
        self.initUI()

    def initUI(self):
        f = QFormLayout(self)

        # Parent Rule
        self.parent_rule_combo = QComboBox()
        self.parent_rule_combo.addItem("None", None)
        try:
            if self.connection:
                c = self.connection.cursor()
                c.execute("SELECT RULE_ID,RULE_NAME FROM BRM_RULES WHERE STATUS='ACTIVE'")
                rows = c.fetchall()
                for r in rows:
                    self.parent_rule_combo.addItem(f"{r['RULE_NAME']} (ID:{r['RULE_ID']})", r["RULE_ID"])
        except:
            pass
        f.addRow("Parent Rule:", self.parent_rule_combo)

        # Rule Name
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Enter rule name")
        f.addRow("Rule Name:", self.name_edit)

        # Rule Type
        self.type_combo = QComboBox()
        self.type_combo.addItems(self.rule_types.keys())
        self.type_combo.currentTextChanged.connect(self.on_rule_type_changed)
        f.addRow("Rule Type:", self.type_combo)

        # Status
        self.status_combo = QComboBox()
        self.status_combo.addItems(["ACTIVE","INACTIVE"])
        f.addRow("Status:", self.status_combo)

        # Operation Type
        self.op_combo = QComboBox()
        self.op_combo.addItems(["CREATE","INSERT","UPDATE","DELETE","OTHER"])
        f.addRow("Operation Type:", self.op_combo)

        # Effective Start/End
        self.start_dt = QDateTimeEdit(QDateTime.currentDateTime())
        self.start_dt.setCalendarPopup(True)
        self.start_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        f.addRow("Effective Start Date:", self.start_dt)

        self.end_dt = QDateTimeEdit(QDateTime.currentDateTime().addDays(30))
        self.end_dt.setCalendarPopup(True)
        self.end_dt.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        f.addRow("Effective End Date:", self.end_dt)

        # Description/SQL
        self.desc_edit = QTextEdit()
        self.desc_edit.setPlaceholderText("Enter entire SQL script or logic here (if known).")
        f.addRow("Description/SQL:", self.desc_edit)

        # Business Justification
        self.just_edit = QTextEdit()
        self.just_edit.setPlaceholderText("Enter business justification for this rule.")
        f.addRow("Business Justification:", self.just_edit)

        # Created By
        self.created_by_edit = QLineEdit()
        self.created_by_edit.setPlaceholderText("Enter your username")
        f.addRow("Created By:", self.created_by_edit)

        # Owner Group
        self.owner_grp_combo = QComboBox()
        try:
            if self.connection:
                c = self.connection.cursor()
                c.execute("SELECT DISTINCT GROUP_NAME FROM GROUP_PERMISSIONS")
                grps = c.fetchall()
                for g in grps:
                    self.owner_grp_combo.addItem(g["GROUP_NAME"], g["GROUP_NAME"])
        except:
            pass
        f.addRow("Owner Group:", self.owner_grp_combo)

        self.setLayout(f)

    def on_rule_type_changed(self, new_text):
        """
        If user picks "DM", then operation might be strictly 'INSERT'.
        If user picks "DQ", then operation might be 'UPDATE' or 'DELETE'.
        """
        if new_text == "DM":
            self.op_combo.clear()
            self.op_combo.addItem("INSERT")
        elif new_text == "DQ":
            self.op_combo.clear()
            self.op_combo.addItems(["UPDATE","DELETE","OTHER"])
        else:
            self.op_combo.clear()
            self.op_combo.addItems(["CREATE","INSERT","UPDATE","DELETE","OTHER"])

    # Basic getters
    def getParentRuleID(self):
        return self.parent_rule_combo.currentData()

    def getRuleName(self):
        return self.name_edit.text().strip()

    def getRuleTypeID(self):
        t = self.type_combo.currentText()
        return self.rule_types.get(t, None)

    def getRuleStatus(self):
        return self.status_combo.currentText()

    def getOperationType(self):
        return self.op_combo.currentText()

    def getStartDate(self):
        return self.start_dt.dateTime().toString("yyyy-MM-dd HH:mm:ss")

    def getEndDate(self):
        return self.end_dt.dateTime().toString("yyyy-MM-dd HH:mm:ss")

    def getRuleSQL(self):
        return self.desc_edit.toPlainText().strip()

    def getBusinessJustification(self):
        return self.just_edit.toPlainText().strip()

    def getCreatedBy(self):
        return self.created_by_edit.text().strip()

    def getDescription(self):
        return self.desc_edit.toPlainText().strip()

    def getOwnerGroup(self):
        return self.owner_grp_combo.currentData()


class EnhancedAddRuleDialog(QDialog):
    """
    Dialog for adding a new rule.
    """
    def __init__(self, main_app, connection, rule_types=None, user_group=None, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.rule_types = rule_types or {}
        self.user_group = user_group
        self.setWindowTitle("Add New Rule")
        self.resize(800, 600)

        self.detail_widget = RuleDetailPage(self.rule_types, self.connection)
        layout = QVBoxLayout(self)
        layout.addWidget(self.detail_widget)

        btn_layout = QHBoxLayout()
        self.save_btn = QPushButton("Add Rule")
        self.save_btn.clicked.connect(self.on_add_clicked)
        btn_layout.addWidget(self.save_btn)
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def on_add_clicked(self):
        rule_data = {
            "PARENT_RULE_ID": self.detail_widget.getParentRuleID(),
            "RULE_TYPE_ID": self.detail_widget.getRuleTypeID(),
            "RULE_NAME": self.detail_widget.getRuleName(),
            "RULE_SQL": self.detail_widget.getRuleSQL(),
            "EFFECTIVE_START_DATE": self.detail_widget.getStartDate(),
            "EFFECTIVE_END_DATE": self.detail_widget.getEndDate(),
            "STATUS": self.detail_widget.getRuleStatus(),
            "VERSION": 1,
            "DESCRIPTION": self.detail_widget.getDescription(),
            "OPERATION_TYPE": self.detail_widget.getOperationType(),
            "BUSINESS_JUSTIFICATION": self.detail_widget.getBusinessJustification(),
            "OWNER_GROUP": self.detail_widget.getOwnerGroup()
        }
        created_by = self.detail_widget.getCreatedBy() or "Unknown"

        # Validate
        if not rule_data["RULE_NAME"]:
            QMessageBox.warning(self, "Input Error", "Rule Name cannot be empty.")
            return
        if not rule_data["RULE_SQL"]:
            QMessageBox.warning(self, "Input Error", "SQL script/description cannot be empty.")
            return
        if rule_data["RULE_TYPE_ID"] is None:
            QMessageBox.warning(self, "Input Error", "Please select a valid Rule Type.")
            return
        if not rule_data["OWNER_GROUP"]:
            QMessageBox.warning(self, "Input Error", "Please select an Owner Group.")
            return

        confirm = ConfirmAddDialog(self)
        if confirm.exec_() != QDialog.Accepted:
            return

        prog = QProgressDialog("Adding rule...", None, 0, 0, self)
        prog.setWindowModality(Qt.WindowModal)
        prog.setCancelButton(None)
        prog.show()
        try:
            new_id = add_rule_sync(self.connection, rule_data, created_by, self.user_group)
            QCoreApplication.processEvents()
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.information(self, "Success", "Rule added successfully.")
            self.main_app.brm_tab.rule_dashboard.load_rules()
            self.accept()
        except Exception as e:
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.critical(self, "DB Error", str(e))


class UpdateRuleDialog(QDialog):
    """
    Dialog for updating an existing rule, with a button to rename a derived column
    (this demonstrates how to do cascade rename + impact check).
    """
    def __init__(self, main_app, connection, rule_types=None, user_group=None, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.rule_types = rule_types or {}
        self.user_group = user_group
        self.setWindowTitle("Update Rule")
        self.resize(800, 600)

        self.select_widget = SelectRuleWidget(self.connection,"Update",self.user_group)
        self.detail_widget = RuleDetailPage(self.rule_types,self.connection)
        layout = QVBoxLayout(self)
        layout.addWidget(self.select_widget)
        layout.addWidget(self.detail_widget)

        # Extra button to rename derived columns
        self.rename_btn = QPushButton("Rename Derived Column")
        self.rename_btn.clicked.connect(self.on_rename_column_clicked)
        layout.addWidget(self.rename_btn)

        self.save_btn = QPushButton("Save Changes")
        self.save_btn.clicked.connect(self.on_save_clicked)
        layout.addWidget(self.save_btn)

        self.setLayout(layout)
        self.select_widget.button_group.buttonClicked.connect(self.load_selected_rule)
        self.selected_rule_id = None
        self.selected_rule_data = None

    def load_selected_rule(self, btn):
        sr = self.select_widget.getSelectedRule()
        if sr:
            self.selected_rule_data = sr
            self.selected_rule_id = sr["RULE_ID"]
            self.detail_widget.name_edit.setText(sr["RULE_NAME"])

            # Populate rule type
            for nm,id_ in self.rule_types.items():
                if id_ == sr["RULE_TYPE_ID"]:
                    idx = self.detail_widget.type_combo.findText(nm)
                    if idx >= 0:
                        self.detail_widget.type_combo.setCurrentIndex(idx)
                    break

            # Status
            st = sr["STATUS"]
            idx2 = self.detail_widget.status_combo.findText(st)
            if idx2 >= 0:
                self.detail_widget.status_combo.setCurrentIndex(idx2)

            # Operation type based on rule type
            if sr["RULE_TYPE_ID"] == 2:  # DM
                self.detail_widget.op_combo.clear()
                self.detail_widget.op_combo.addItem("INSERT")
            elif sr["RULE_TYPE_ID"] == 1:  # DQ
                self.detail_widget.op_combo.clear()
                self.detail_widget.op_combo.addItems(["UPDATE","DELETE","OTHER"])

            op = sr.get("OPERATION_TYPE","OTHER")
            idx3 = self.detail_widget.op_combo.findText(op)
            if idx3 >= 0:
                self.detail_widget.op_combo.setCurrentIndex(idx3)

            # Start/End Dates
            try:
                start_dt = datetime.strptime(sr["EFFECTIVE_START_DATE"], "%Y-%m-%d %H:%M:%S")
                self.detail_widget.start_dt.setDateTime(QDateTime(start_dt))
            except:
                pass
            if sr["EFFECTIVE_END_DATE"]:
                try:
                    end_dt = datetime.strptime(sr["EFFECTIVE_END_DATE"], "%Y-%m-%d %H:%M:%S")
                    self.detail_widget.end_dt.setDateTime(QDateTime(end_dt))
                except:
                    pass
            else:
                self.detail_widget.end_dt.setDateTime(QDateTime.currentDateTime().addDays(30))

            self.detail_widget.desc_edit.setText(sr["RULE_SQL"])
            self.detail_widget.created_by_edit.setText(sr["CREATED_BY"])
            if sr.get("BUSINESS_JUSTIFICATION"):
                self.detail_widget.just_edit.setText(sr["BUSINESS_JUSTIFICATION"])

            og = sr["OWNER_GROUP"]
            idx4 = self.detail_widget.owner_grp_combo.findText(og)
            if idx4 >= 0:
                self.detail_widget.owner_grp_combo.setCurrentIndex(idx4)

    def find_impacted_children(self, parent_rule_id, old_col):
        """
        BFS or recursion to find child rules referencing old_col in SOURCE_COLUMN.
        """
        c = self.connection.cursor()
        impacted = []
        queue = [parent_rule_id]
        visited = set()

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            c.execute("SELECT RULE_ID,RULE_NAME FROM BRM_RULES WHERE PARENT_RULE_ID=?", (current,))
            kids = c.fetchall()
            for child in kids:
                child_id = child["RULE_ID"]
                c2 = self.connection.cursor()
                c2.execute("""
                  SELECT COUNT(*) as cnt
                  FROM BRM_COLUMN_MAPPING
                  WHERE RULE_ID=? AND SOURCE_COLUMN=?
                """,(child_id, old_col))
                row = c2.fetchone()
                if row and row["cnt"]>0:
                    impacted.append({"RULE_ID": child_id, "RULE_NAME": child["RULE_NAME"]})
                queue.append(child_id)

        return impacted

    def on_rename_column_clicked(self):
        """
        Let user rename a derived column from old->new and cascade to children.
        Show impacted child rules for confirmation.
        """
        if not self.selected_rule_data:
            QMessageBox.warning(self, "No Selection", "No rule is selected.")
            return

        parent_rule_id = self.selected_rule_data["RULE_ID"]
        old_col, ok1 = QInputDialog.getText(self, "Old Column Name", "Enter the existing column to rename:")
        if not ok1 or not old_col.strip():
            return
        new_col, ok2 = QInputDialog.getText(self, "New Column Name", "Enter the new column name:")
        if not ok2 or not new_col.strip():
            return

        old_col = old_col.strip()
        new_col = new_col.strip()

        # Find impacted children
        impacted = self.find_impacted_children(parent_rule_id, old_col)
        if impacted:
            msg = f"The following child rules reference '{old_col}':\n"
            for rinfo in impacted:
                msg += f"  - Rule ID {rinfo['RULE_ID']}: {rinfo['RULE_NAME']}\n"
            msg += f"\nThey will be updated to reference '{new_col}'. Proceed?"
            confirm = QMessageBox.question(self, "Cascade Rename Impact", msg)
            if confirm != QMessageBox.Yes:
                return
        else:
            QMessageBox.information(self, "No Impact", f"No child rules found referencing '{old_col}'.")

        # Do the rename
        try:
            rename_derived_column_in_children(
                self.connection,
                parent_rule_id,
                old_col,
                new_col,
                updated_by=self.selected_rule_data.get("UPDATED_BY","Unknown")
            )
            QMessageBox.information(
                self, "Success",
                f"Column rename from '{old_col}' to '{new_col}' completed.\n"
                f"Impacted child rules have been updated."
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def on_save_clicked(self):
        sr = self.select_widget.getSelectedRule()
        if not sr:
            QMessageBox.warning(self,"No selection","No rule selected.")
            return

        rule_data = {
            "RULE_ID": sr["RULE_ID"],
            "PARENT_RULE_ID": self.detail_widget.getParentRuleID(),
            "RULE_TYPE_ID": self.detail_widget.getRuleTypeID(),
            "RULE_NAME": self.detail_widget.getRuleName(),
            "RULE_SQL": self.detail_widget.getRuleSQL(),
            "EFFECTIVE_START_DATE": self.detail_widget.getStartDate(),
            "EFFECTIVE_END_DATE": self.detail_widget.getEndDate(),
            "STATUS": self.detail_widget.getRuleStatus(),
            "VERSION": sr["VERSION"],
            "DESCRIPTION": self.detail_widget.getDescription(),
            "OPERATION_TYPE": self.detail_widget.getOperationType(),
            "BUSINESS_JUSTIFICATION": self.detail_widget.getBusinessJustification(),
            "OWNER_GROUP": self.detail_widget.getOwnerGroup()
        }
        updated_by = self.detail_widget.getCreatedBy() or "Unknown"

        if not rule_data["RULE_NAME"]:
            QMessageBox.warning(self,"Input Error","Rule Name cannot be empty.")
            return
        if not rule_data["RULE_SQL"]:
            QMessageBox.warning(self,"Input Error","SQL cannot be empty.")
            return
        if rule_data["RULE_TYPE_ID"] is None:
            QMessageBox.warning(self,"Input Error","Please select a valid Rule Type.")
            return
        if not rule_data["OWNER_GROUP"]:
            QMessageBox.warning(self,"Input Error","Please select an Owner Group.")
            return

        confirm = ConfirmUpdateDialog(self)
        if confirm.exec_() != QDialog.Accepted:
            return

        prog = QProgressDialog("Updating rule...", None, 0, 0, self)
        prog.setWindowModality(Qt.WindowModal)
        prog.setCancelButton(None)
        prog.show()
        try:
            update_rule_sync(self.connection, rule_data, updated_by, self.user_group)
            QCoreApplication.processEvents()
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.information(self,"Success","Rule updated.")
            self.main_app.brm_tab.rule_dashboard.load_rules()
            self.accept()
        except Exception as e:
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.critical(self,"DB Error", str(e))


class DeactivateRuleDialog(QDialog):
    """
    Dialog to select a rule and deactivate it.
    """
    def __init__(self, main_app, connection, user_group=None, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.user_group = user_group
        self.setWindowTitle("Deactivate Rule")
        self.resize(600, 500)
        v = QVBoxLayout(self)

        self.select_widget = SelectRuleWidget(self.connection, "Deactivate", self.user_group)
        v.addWidget(self.select_widget)

        self.deactivate_btn = QPushButton("Deactivate Rule")
        self.deactivate_btn.clicked.connect(self.on_deactivate_clicked)
        v.addWidget(self.deactivate_btn)

        self.setLayout(v)

    def on_deactivate_clicked(self):
        sr = self.select_widget.getSelectedRule()
        if not sr:
            QMessageBox.warning(self, "No rule selected", "Please pick a rule.")
            return
        rule_id = sr["RULE_ID"]
        updated_by = sr.get("UPDATED_BY", sr["CREATED_BY"]) or "Unknown"

        c = ConfirmDeactivateDialog(self)
        if c.exec_() != QDialog.Accepted:
            return

        prog = QProgressDialog("Deactivating rule...", None, 0, 0, self)
        prog.setWindowModality(Qt.WindowModal)
        prog.setCancelButton(None)
        prog.show()
        try:
            deactivate_rule_sync(self.connection, rule_id, updated_by, self.user_group)
            QCoreApplication.processEvents()
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.information(self, "Success", "Rule deactivated.")
            self.main_app.brm_tab.rule_dashboard.load_rules()
            self.accept()
        except Exception as e:
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.critical(self, "DB Error", str(e))


class DeleteRuleDialog(QDialog):
    """
    Dialog to select an INACTIVE rule and delete it.
    """
    def __init__(self, main_app, connection, user_group=None, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.user_group = user_group
        self.setWindowTitle("Delete Rule")
        self.resize(600, 500)
        v = QVBoxLayout(self)

        self.select_widget = SelectRuleWidgetDelete(self.connection, "Delete", self.user_group)
        v.addWidget(self.select_widget)

        self.delete_btn = QPushButton("Delete Rule")
        self.delete_btn.clicked.connect(self.on_delete_clicked)
        v.addWidget(self.delete_btn)

        self.setLayout(v)

    def on_delete_clicked(self):
        sr = self.select_widget.getSelectedRule()
        if not sr:
            QMessageBox.warning(self, "No Selection", "No rule selected.")
            return
        rule_id = sr["RULE_ID"]
        action_by = sr.get("UPDATED_BY", sr["CREATED_BY"]) or "Unknown"

        c = ConfirmDeleteDialog(self)
        if c.exec_() != QDialog.Accepted:
            return

        prog = QProgressDialog("Deleting rule...", None, 0, 0, self)
        prog.setWindowModality(Qt.WindowModal)
        prog.setCancelButton(None)
        prog.show()
        try:
            delete_rule_sync(self.connection, rule_id, action_by, self.user_group)
            QCoreApplication.processEvents()
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.information(self, "Success", "Rule deleted.")
            self.main_app.brm_tab.rule_dashboard.load_rules()
            self.accept()
        except Exception as e:
            prog.close()
            QCoreApplication.processEvents()
            QMessageBox.critical(self, "DB Error", str(e))
################################################################################
# PART 3 of 6
# DESCRIPTION:
#   1) SelectRuleWidget & SelectRuleWidgetDelete
#   2) RuleDependencyViewer
#   3) AuditLogViewer
#   4) SearchRuleDialog
#   5) RuleDashboard
################################################################################

class SelectRuleWidget(QWidget):
    """
    Allows the user to pick a rule (via radio button) from a table,
    including a "View Dependencies" button.
    """
    def __init__(self, connection, action, user_group, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.action = action
        self.user_group = user_group

        v = QVBoxLayout(self)
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)

        self.rules_table = QTableWidget(0, 9)
        self.rules_table.setHorizontalHeaderLabels([
            "Select","Rule ID","Name","SQL","Status","Version","Created By","Created At","Owner Group"
        ])
        self.rules_table.horizontalHeader().setStretchLastSection(True)
        self.rules_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.rules_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.rules_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.rules_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        v.addWidget(self.rules_table)

        self.view_dep_btn = QPushButton("View Dependencies")
        self.view_dep_btn.clicked.connect(self.on_view_dependencies)
        v.addWidget(self.view_dep_btn)
        self.setLayout(v)

        self.load_rules()
        self.button_group.buttonClicked.connect(self.on_rule_selected)

    def load_rules(self):
        if not self.connection:
            self.rules_table.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            c.execute("""
            SELECT RULE_ID, RULE_TYPE_ID, RULE_NAME, RULE_SQL, STATUS, VERSION,
                   CREATED_BY, CREATED_TIMESTAMP, OWNER_GROUP, UPDATED_BY
            FROM BRM_RULES
            ORDER BY RULE_ID DESC
            """)
            rows = c.fetchall()

            self.rules_table.setRowCount(0)
            self.button_group = QButtonGroup(self)
            self.button_group.setExclusive(True)
            self.button_group.buttonClicked.connect(self.on_rule_selected)

            for rd in rows:
                r = self.rules_table.rowCount()
                self.rules_table.insertRow(r)

                rad = QRadioButton()
                self.button_group.addButton(rad, rd["RULE_ID"])
                self.rules_table.setCellWidget(r, 0, rad)
                self.rules_table.setItem(r, 1, QTableWidgetItem(str(rd["RULE_ID"])))
                self.rules_table.setItem(r, 2, QTableWidgetItem(rd["RULE_NAME"]))
                self.rules_table.setItem(r, 3, QTableWidgetItem(rd["RULE_SQL"]))

                sitem = QTableWidgetItem(rd["STATUS"])
                if rd["STATUS"].lower() == "active":
                    sitem.setBackground(QtGui.QColor(144,238,144))
                else:
                    sitem.setBackground(QtGui.QColor(255,182,193))
                self.rules_table.setItem(r, 4, sitem)

                self.rules_table.setItem(r, 5, QTableWidgetItem(str(rd["VERSION"])))
                self.rules_table.setItem(r, 6, QTableWidgetItem(rd["CREATED_BY"]))
                self.rules_table.setItem(r, 7, QTableWidgetItem(rd["CREATED_TIMESTAMP"]))
                self.rules_table.setItem(r, 8, QTableWidgetItem(rd["OWNER_GROUP"]))

        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def getSelectedRule(self):
        sb = self.button_group.checkedButton()
        if not sb:
            return None
        rid = self.button_group.id(sb)
        try:
            c = self.connection.cursor()
            c.execute("""
            SELECT RULE_ID, RULE_TYPE_ID, RULE_NAME, RULE_SQL, STATUS, VERSION,
                   CREATED_BY, CREATED_TIMESTAMP, OWNER_GROUP, UPDATED_BY,
                   OPERATION_TYPE, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE,
                   DESCRIPTION, BUSINESS_JUSTIFICATION
            FROM BRM_RULES
            WHERE RULE_ID=?
            """,(rid,))
            row = c.fetchone()
            if row:
                return dict(row)
            return None
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))
            return None

    def on_view_dependencies(self):
        sr = self.getSelectedRule()
        if not sr:
            QMessageBox.warning(self, "No Selection", "No rule selected.")
            return
        dlg = RuleDependencyViewer(self.connection, sr["RULE_ID"], self)
        dlg.exec_()

    def on_rule_selected(self, btn):
        pass


class SelectRuleWidgetDelete(SelectRuleWidget):
    """
    Specialized version that only shows INACTIVE rules for deletion.
    """
    def load_rules(self):
        if not self.connection:
            self.rules_table.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            c.execute("""
            SELECT RULE_ID, RULE_TYPE_ID, RULE_NAME, RULE_SQL, STATUS, VERSION,
                   CREATED_BY, CREATED_TIMESTAMP, OWNER_GROUP, UPDATED_BY
            FROM BRM_RULES
            WHERE STATUS='INACTIVE'
            ORDER BY RULE_ID DESC
            """)
            rows = c.fetchall()

            self.rules_table.setRowCount(0)
            self.button_group = QButtonGroup(self)
            self.button_group.setExclusive(True)
            self.button_group.buttonClicked.connect(self.on_rule_selected)

            for rd in rows:
                r = self.rules_table.rowCount()
                self.rules_table.insertRow(r)

                rad = QRadioButton()
                self.button_group.addButton(rad, rd["RULE_ID"])
                self.rules_table.setCellWidget(r, 0, rad)
                self.rules_table.setItem(r, 1, QTableWidgetItem(str(rd["RULE_ID"])))
                self.rules_table.setItem(r, 2, QTableWidgetItem(rd["RULE_NAME"]))
                self.rules_table.setItem(r, 3, QTableWidgetItem(rd["RULE_SQL"]))

                sitem = QTableWidgetItem(rd["STATUS"])
                sitem.setBackground(QtGui.QColor(255,182,193))
                self.rules_table.setItem(r, 4, sitem)

                self.rules_table.setItem(r, 5, QTableWidgetItem(str(rd["VERSION"])))
                self.rules_table.setItem(r, 6, QTableWidgetItem(rd["CREATED_BY"]))
                self.rules_table.setItem(r, 7, QTableWidgetItem(rd["CREATED_TIMESTAMP"]))
                self.rules_table.setItem(r, 8, QTableWidgetItem(rd["OWNER_GROUP"]))

        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))


class RuleDependencyViewer(QDialog):
    """
    Shows table dependencies (from BRM_RULE_TABLE_DEPENDENCIES) for a given rule.
    """
    def __init__(self, connection, rule_id, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Dependencies for Rule ID {rule_id}")
        self.resize(600, 400)
        self.connection = connection
        self.rule_id = rule_id

        v = QVBoxLayout(self)
        self.dep_table = QTableWidget(0, 3)
        self.dep_table.setHorizontalHeaderLabels(["Dependency ID", "Database Name", "Table Name"])
        self.dep_table.horizontalHeader().setStretchLastSection(True)
        self.dep_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.dep_table)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        v.addWidget(close_btn)

        self.setLayout(v)
        self.load_dependencies()

    def load_dependencies(self):
        if not self.connection:
            self.dep_table.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            c.execute("""
            SELECT DEPENDENCY_ID, DATABASE_NAME, TABLE_NAME
            FROM BRM_RULE_TABLE_DEPENDENCIES
            WHERE RULE_ID=?
            """,(self.rule_id,))
            rows = c.fetchall()
            self.dep_table.setRowCount(0)
            for rd in rows:
                rr = self.dep_table.rowCount()
                self.dep_table.insertRow(rr)
                self.dep_table.setItem(rr, 0, QTableWidgetItem(str(rd["DEPENDENCY_ID"])))
                self.dep_table.setItem(rr, 1, QTableWidgetItem(rd["DATABASE_NAME"]))
                self.dep_table.setItem(rr, 2, QTableWidgetItem(rd["TABLE_NAME"]))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))


class AuditLogViewer(QDialog):
    """
    Shows rows from BRM_AUDIT_LOG with basic search capability.
    """
    def __init__(self, connection, user_group, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Audit Logs")
        self.resize(800, 600)
        self.connection = connection
        self.user_group = user_group

        v = QVBoxLayout(self)
        hb = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search by Action, Table, or Action By...")
        self.search_edit.textChanged.connect(self.perform_search)
        hb.addWidget(QLabel("Search:"))
        hb.addWidget(self.search_edit)
        v.addLayout(hb)

        self.audit_table = QTableWidget(0, 8)
        self.audit_table.setHorizontalHeaderLabels([
            "Audit ID", "Action", "Table Name", "Record ID", "Action By", "Old Data", "New Data", "Timestamp"
        ])
        self.audit_table.horizontalHeader().setStretchLastSection(True)
        self.audit_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.audit_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.audit_table)

        rb = QPushButton("Refresh Logs")
        rb.clicked.connect(self.load_audit_logs)
        v.addWidget(rb)

        self.setLayout(v)
        self.load_audit_logs()

    def load_audit_logs(self):
        if not self.connection:
            self.audit_table.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            c.execute("""
                SELECT AUDIT_ID, ACTION, TABLE_NAME, RECORD_ID, ACTION_BY,
                       OLD_DATA, NEW_DATA, ACTION_TIMESTAMP
                FROM BRM_AUDIT_LOG
                ORDER BY ACTION_TIMESTAMP DESC
                LIMIT 1000
            """)
            rows = c.fetchall()
            self.audit_table.setRowCount(0)
            for row in rows:
                rr = self.audit_table.rowCount()
                self.audit_table.insertRow(rr)
                for i, val in enumerate(row):
                    if i in [5, 6]:  # OLD_DATA, NEW_DATA
                        if val:
                            try:
                                parsed = json.loads(val)
                                txt = json.dumps(parsed, indent=4)
                            except:
                                txt = str(val)
                            item = QTableWidgetItem(txt)
                        else:
                            item = QTableWidgetItem("None")
                    else:
                        item = QTableWidgetItem(str(val) if val is not None else "None")
                    self.audit_table.setItem(rr, i, item)
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def perform_search(self, text):
        for row in range(self.audit_table.rowCount()):
            match = False
            # search columns 1 (Action), 2 (Table Name), 4 (Action By)
            for col in [1,2,4]:
                it = self.audit_table.item(row, col)
                if it and text.lower() in it.text().lower():
                    match = True
                    break
            self.audit_table.setRowHidden(row, not match)


class SearchRuleDialog(QDialog):
    """
    Allows searching rules by RULE_NAME or RULE_SQL.
    """
    def __init__(self, connection, user_group, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Search Rules")
        self.resize(800, 600)
        self.connection = connection
        self.user_group = user_group

        v = QVBoxLayout(self)
        hb = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Enter rule name or SQL snippet...")
        self.search_edit.textChanged.connect(self.load_search_results)
        hb.addWidget(QLabel("Search:"))
        hb.addWidget(self.search_edit)
        v.addLayout(hb)

        self.results_view = QTableWidget(0, 6)
        self.results_view.setHorizontalHeaderLabels([
            "Rule ID","Name","SQL","Status","Version","Created By"
        ])
        self.results_view.horizontalHeader().setStretchLastSection(True)
        self.results_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        v.addWidget(self.results_view)

        rb = QPushButton("Refresh Results")
        rb.clicked.connect(self.load_search_results)
        v.addWidget(rb)

        self.setLayout(v)
        self.load_search_results()

    def load_search_results(self):
        query = self.search_edit.text().strip()
        if not self.connection:
            self.results_view.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            if query:
                c.execute("""
                SELECT RULE_ID,RULE_NAME,RULE_SQL,STATUS,VERSION,CREATED_BY
                FROM BRM_RULES
                WHERE (RULE_NAME LIKE ? OR RULE_SQL LIKE ?)
                ORDER BY RULE_ID DESC
                LIMIT 1000
                """,(f"%{query}%", f"%{query}%"))
            else:
                c.execute("""
                SELECT RULE_ID,RULE_NAME,RULE_SQL,STATUS,VERSION,CREATED_BY
                FROM BRM_RULES
                ORDER BY RULE_ID DESC
                LIMIT 1000
                """)
            rows = c.fetchall()
            self.results_view.setRowCount(0)
            for rd in rows:
                r = self.results_view.rowCount()
                self.results_view.insertRow(r)
                self.results_view.setItem(r, 0, QTableWidgetItem(str(rd["RULE_ID"])))
                self.results_view.setItem(r, 1, QTableWidgetItem(rd["RULE_NAME"]))
                self.results_view.setItem(r, 2, QTableWidgetItem(rd["RULE_SQL"]))
                self.results_view.setItem(r, 3, QTableWidgetItem(rd["STATUS"]))
                self.results_view.setItem(r, 4, QTableWidgetItem(str(rd["VERSION"])))
                self.results_view.setItem(r, 5, QTableWidgetItem(rd["CREATED_BY"]))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

class RuleDashboard(QGroupBox):
    """
    A dashboard-like view that lists rules, supports searching, pagination,
    and also provides some charting (bar + pie) with PyQtGraph.
    """
    def __init__(self, connection, user_id, user_group, parent=None):
        super().__init__("Rule Dashboard", parent)
        self.connection = connection
        self.user_id = user_id
        self.user_group = user_group
        self.selected_rule_id = None
        self.current_page = 1
        self.records_per_page = 50
        self.total_pages = 1
        self.main_app = None  # reference if needed

        layout = QVBoxLayout(self)
        self.welcome_label = QLabel(f"Welcome, {self.user_group}!")
        self.welcome_label.setStyleSheet("font-size:16px;font-weight:bold;")
        layout.addWidget(self.welcome_label)

        # Filter Row
        fl = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search rules by name or SQL...")
        self.search_edit.textChanged.connect(self.load_rules)
        fl.addWidget(QLabel("Search:"))
        fl.addWidget(self.search_edit)

        self.status_filter = QComboBox()
        self.status_filter.addItem("All Statuses", None)
        self.status_filter.addItem("ACTIVE", "ACTIVE")
        self.status_filter.addItem("INACTIVE", "INACTIVE")
        self.status_filter.addItem("DELETED", "DELETED")
        self.status_filter.currentIndexChanged.connect(self.load_rules)
        fl.addWidget(QLabel("Status:"))
        fl.addWidget(self.status_filter)

        self.creator_filter = QComboBox()
        self.creator_filter.addItem("All Creators", None)
        try:
            c = self.connection.cursor()
            c.execute("SELECT DISTINCT CREATED_BY FROM BRM_RULES")
            crows = c.fetchall()
            for rr in crows:
                self.creator_filter.addItem(rr["CREATED_BY"], rr["CREATED_BY"])
        except:
            pass
        self.creator_filter.currentIndexChanged.connect(self.load_rules)
        fl.addWidget(QLabel("Creator:"))
        fl.addWidget(self.creator_filter)

        self.start_date_filter = QDateTimeEdit()
        self.start_date_filter.setCalendarPopup(True)
        self.start_date_filter.setDisplayFormat("yyyy-MM-dd")
        self.start_date_filter.setDate(QDate.currentDate().addMonths(-6))
        self.start_date_filter.dateChanged.connect(self.load_rules)

        self.end_date_filter = QDateTimeEdit()
        self.end_date_filter.setCalendarPopup(True)
        self.end_date_filter.setDisplayFormat("yyyy-MM-dd")
        self.end_date_filter.setDate(QDate.currentDate())
        self.end_date_filter.dateChanged.connect(self.load_rules)

        fl.addWidget(QLabel("Start Date:"))
        fl.addWidget(self.start_date_filter)
        fl.addWidget(QLabel("End Date:"))
        fl.addWidget(self.end_date_filter)
        layout.addLayout(fl)

        # Table of rules
        self.rules_table = QTableWidget(0, 7)
        self.rules_table.setHorizontalHeaderLabels([
            "Select","Rule ID","Name","SQL","Status","Version","Owner Group"
        ])
        self.rules_table.horizontalHeader().setStretchLastSection(True)
        self.rules_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.rules_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.rules_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.rules_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        layout.addWidget(self.rules_table)

        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        self.button_group.buttonClicked.connect(self.on_rule_selected_dashboard)

        # Charts
        chart_box = QHBoxLayout()
        self.bar_chart = pg.PlotWidget(title="Number of Rules Created by Each User")
        self.bar_chart.setBackground('w')
        chart_box.addWidget(self.bar_chart)

        self.pie_chart = pg.PlotWidget(title="Rule Status Distribution")
        self.pie_chart.setBackground('w')
        chart_box.addWidget(self.pie_chart)
        layout.addLayout(chart_box)

        # Pagination
        nav_box = QHBoxLayout()
        self.prev_page_btn = QPushButton("Previous")
        self.prev_page_btn.clicked.connect(self.prev_page)
        self.page_label = QLabel("Page 1/1")
        self.next_page_btn = QPushButton("Next")
        self.next_page_btn.clicked.connect(self.next_page)
        nav_box.addWidget(self.prev_page_btn)
        nav_box.addWidget(self.page_label)
        nav_box.addWidget(self.next_page_btn)
        layout.addLayout(nav_box)

        ref_btn = QPushButton("Refresh")
        ref_btn.clicked.connect(self.load_rules)
        layout.addWidget(ref_btn)

        self.setLayout(layout)
        self.load_rules()

    def build_filter_query(self):
        filters = []
        params = []
        txt = self.search_edit.text().strip()
        if txt:
            filters.append("(RULE_NAME LIKE ? OR RULE_SQL LIKE ?)")
            params.extend([f"%{txt}%", f"%{txt}%"])

        st = self.status_filter.currentData()
        if st:
            if st.upper() == "DELETED":
                # "DELETED" is not a real status in the table, so we exclude everything
                filters.append("1=0")
            else:
                filters.append("STATUS=?")
                params.append(st)

        cr = self.creator_filter.currentData()
        if cr:
            filters.append("CREATED_BY=?")
            params.append(cr)

        sd = self.start_date_filter.date().toString("yyyy-MM-dd")
        ed = self.end_date_filter.date().toString("yyyy-MM-dd")
        filters.append("DATE(CREATED_TIMESTAMP) BETWEEN ? AND ?")
        params.extend([sd, ed])

        if filters:
            clause = " AND ".join(filters)
        else:
            clause = "1"
        return clause, params

    def load_rules(self):
        if not self.connection:
            self.rules_table.setRowCount(0)
            return
        try:
            c = self.connection.cursor()
            clause, params = self.build_filter_query()
            # Count total
            c.execute(f"SELECT COUNT(*) as count FROM BRM_RULES WHERE {clause}", params)
            total_row = c.fetchone()
            total = total_row["count"] if total_row else 0
            self.total_pages = max(1, math.ceil(total / self.records_per_page))

            if self.current_page > self.total_pages:
                self.current_page = self.total_pages
            elif self.current_page < 1:
                self.current_page = 1
            self.page_label.setText(f"Page {self.current_page}/{self.total_pages}")

            offset = (self.current_page - 1)* self.records_per_page
            c.execute(f"""
              SELECT RULE_ID, RULE_NAME, RULE_SQL, STATUS, VERSION,
                     CREATED_BY, OWNER_GROUP, CREATED_TIMESTAMP, RULE_TYPE_ID
              FROM BRM_RULES
              WHERE {clause}
              ORDER BY RULE_ID DESC
              LIMIT ? OFFSET ?
            """, (*params, self.records_per_page, offset))
            rows = c.fetchall()

            self.rules_table.setRowCount(0)
            self.button_group = QButtonGroup(self)
            self.button_group.setExclusive(True)
            self.button_group.buttonClicked.connect(self.on_rule_selected_dashboard)

            creators = {}
            status_counts = {"ACTIVE":0, "INACTIVE":0, "DELETED":0}

            for rd in rows:
                rr = self.rules_table.rowCount()
                self.rules_table.insertRow(rr)
                rad = QRadioButton()
                self.button_group.addButton(rad, rd["RULE_ID"])
                self.rules_table.setCellWidget(rr,0,rad)
                self.rules_table.setItem(rr,1,QTableWidgetItem(str(rd["RULE_ID"])))
                self.rules_table.setItem(rr,2,QTableWidgetItem(rd["RULE_NAME"]))
                self.rules_table.setItem(rr,3,QTableWidgetItem(rd["RULE_SQL"]))

                sitem = QTableWidgetItem(rd["STATUS"])
                if rd["STATUS"].lower() == "active":
                    sitem.setBackground(QColor(144,238,144))
                else:
                    sitem.setBackground(QColor(255,182,193))
                self.rules_table.setItem(rr,4,sitem)

                self.rules_table.setItem(rr,5,QTableWidgetItem(str(rd["VERSION"])))
                self.rules_table.setItem(rr,6,QTableWidgetItem(rd["OWNER_GROUP"]))

                ckey = rd["CREATED_BY"]
                creators[ckey] = creators.get(ckey, 0) + 1

                stkey = rd["STATUS"].upper()
                if stkey in status_counts:
                    status_counts[stkey] += 1
                else:
                    status_counts[stkey] = 1

            # Check how many "deleted" from logs
            c.execute("SELECT COUNT(*) as deleted_count FROM BRM_AUDIT_LOG WHERE ACTION='DELETE'")
            row2 = c.fetchone()
            if row2:
                dcount = row2["deleted_count"]
                status_counts["DELETED"] = dcount

            self.update_charts(creators, status_counts)

        except Exception as e:
            QMessageBox.critical(self,"DB Error", str(e))

    def update_charts(self, creators, status_counts):
        self.bar_chart.clear()
        # Bar chart: number of rules per creator
        if creators:
            sorted_creators = sorted(creators.items(), key=lambda x: x[1], reverse=True)
            c_names = [x[0] for x in sorted_creators]
            c_vals = [x[1] for x in sorted_creators]
            bg = pg.BarGraphItem(x=range(len(c_names)), height=c_vals, width=0.6, brush="skyblue")
            self.bar_chart.addItem(bg)
            ax = self.bar_chart.getAxis("bottom")
            ax.setTicks([list(zip(range(len(c_names)), c_names))])
            self.bar_chart.setLabel("left","Number of Rules")
            self.bar_chart.setLabel("bottom","Created By")
            self.bar_chart.showGrid(x=True, y=True)

        # Pie chart
        self.pie_chart.clear()
        total = sum(status_counts.values())
        if total > 0:
            angles = [360*(v/total) for v in status_counts.values()]
            start = 90
            color_map = {"ACTIVE":"green","INACTIVE":"red","DELETED":"gray"}
            for (k,v),ang in zip(status_counts.items(), angles):
                if ang>0:
                    wedge = pg.QtGui.QPainterPath()
                    wedge.moveTo(0,0)
                    wedge.arcTo(-100,-100,200,200,start,ang)
                    wedge.closeSubpath()
                    brush = QtGui.QBrush(QtGui.QColor(color_map.get(k,"blue")))
                    pi = pg.QtWidgets.QGraphicsPathItem(wedge)
                    pi.setBrush(brush)
                    pi.setPen(pg.mkPen("black"))
                    self.pie_chart.addItem(pi)

                    mid = start + (ang/2)
                    import math
                    rad = (mid*math.pi)/180
                    xx = 50*math.cos(rad)
                    yy = 50*math.sin(rad)
                    perc = math.floor((ang/360)*100)
                    lab = pg.TextItem(f"{k} ({perc}%)", anchor=(0.5,0.5))
                    lab.setPos(xx,yy)
                    self.pie_chart.addItem(lab)
                    start += ang
            self.pie_chart.setAspectLocked(True)

    def on_rule_selected_dashboard(self, btn):
        self.selected_rule_id = self.button_group.id(btn)

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_rules()

    def next_page(self):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_rules()
################################################################################
# PART 4 of 6
# DESCRIPTION:
#   1) BusinessRuleManagementTab
#   2) GroupManagementTab (with rename, backup, restore)
################################################################################

class BusinessRuleManagementTab(QWidget):
    """
    Main tab for business rule management, hosting the RuleDashboard and
    CRUD action buttons (Add, Update, Deactivate, Delete, etc.).
    """
    def __init__(self, main_app, connection, user_id, user_group, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.user_id = user_id
        self.user_group = user_group

        v = QVBoxLayout(self)

        crud_box = QHBoxLayout()

        addb = QPushButton("Add Rule")
        addb.clicked.connect(self.main_app.launch_add_rule_dialog)
        crud_box.addWidget(addb)

        upb = QPushButton("Update Rule")
        upb.clicked.connect(self.main_app.launch_update_rule_dialog)
        crud_box.addWidget(upb)

        dab = QPushButton("Deactivate Rule")
        dab.clicked.connect(self.main_app.launch_deactivate_rule_dialog)
        crud_box.addWidget(dab)

        deb = QPushButton("Delete Rule")
        deb.clicked.connect(self.main_app.launch_delete_rule_dialog)
        crud_box.addWidget(deb)

        adb = QPushButton("View Audit Logs")
        adb.clicked.connect(self.main_app.launch_audit_log_viewer)
        crud_box.addWidget(adb)

        srb = QPushButton("Search Rules")
        srb.clicked.connect(self.main_app.launch_search_rule_dialog)
        crud_box.addWidget(srb)

        crud_box.addStretch()
        v.addLayout(crud_box)

        # The Rule Dashboard
        self.rule_dashboard = RuleDashboard(self.connection, self.user_id, self.user_group)
        self.rule_dashboard.main_app = self.main_app  # so it can call reload or other functions if needed
        v.addWidget(self.rule_dashboard)
        v.addStretch()

        self.setLayout(v)


class GroupManagementTab(QWidget):
    """
    Admin-only tab for managing groups, users, and permissions,
    with the ability to rename a group (updating BRM_RULES.OWNER_GROUP),
    and backup/restore the entire group's set of rules.
    """
    def __init__(self, main_app, connection, user_id, user_group, parent=None):
        super().__init__(parent)
        self.main_app = main_app
        self.connection = connection
        self.user_id = user_id
        self.user_group = user_group

        if user_group != "Admin":
            layout = QVBoxLayout(self)
            layout.addWidget(QLabel("Access Denied: Only Admin can manage groups."))
            self.setLayout(layout)
            return

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Tab 1: Groups & membership
        groups_membership_tab = QWidget()
        gm_layout = QVBoxLayout(groups_membership_tab)

        group_details_box = QGroupBox("Group Details")
        group_details_layout = QVBoxLayout(group_details_box)
        self.groups_table = QTableWidget()
        self.groups_table.setColumnCount(3)
        self.groups_table.setHorizontalHeaderLabels(["Group Name", "Description", "Email"])
        self.groups_table.setSortingEnabled(True)
        self.groups_table.horizontalHeader().setStretchLastSection(True)
        group_details_layout.addWidget(self.groups_table)

        group_btn_layout = QHBoxLayout()
        add_group_btn = QPushButton("Add Group")
        add_group_btn.clicked.connect(self.on_add_group)
        group_btn_layout.addWidget(add_group_btn)

        rename_group_btn = QPushButton("Rename Group")
        rename_group_btn.clicked.connect(self.on_rename_group)
        group_btn_layout.addWidget(rename_group_btn)

        del_group_btn = QPushButton("Delete Group")
        del_group_btn.clicked.connect(self.on_delete_group)
        group_btn_layout.addWidget(del_group_btn)

        # Buttons for backup/restore
        backup_group_btn = QPushButton("Backup Group")
        backup_group_btn.clicked.connect(self.on_backup_group)
        group_btn_layout.addWidget(backup_group_btn)

        restore_group_btn = QPushButton("Restore Group")
        restore_group_btn.clicked.connect(self.on_restore_group)
        group_btn_layout.addWidget(restore_group_btn)

        group_btn_layout.addStretch()
        group_details_layout.addLayout(group_btn_layout)
        gm_layout.addWidget(group_details_box)

        membership_box = QGroupBox("Membership Management")
        membership_layout = QVBoxLayout(membership_box)
        self.users_table = QTableWidget()
        self.users_table.setColumnCount(3)
        self.users_table.setHorizontalHeaderLabels(["User ID", "Username", "Group"])
        self.users_table.setSortingEnabled(True)
        self.users_table.horizontalHeader().setStretchLastSection(True)
        membership_layout.addWidget(self.users_table)

        membership_btn_layout = QHBoxLayout()
        add_user_btn = QPushButton("Add User to Group")
        add_user_btn.clicked.connect(self.on_add_user_to_group)
        membership_btn_layout.addWidget(add_user_btn)

        remove_user_btn = QPushButton("Remove User from Group")
        remove_user_btn.clicked.connect(self.on_remove_user_from_group)
        membership_btn_layout.addWidget(remove_user_btn)

        membership_btn_layout.addStretch()
        membership_layout.addLayout(membership_btn_layout)
        gm_layout.addWidget(membership_box)

        self.tabs.addTab(groups_membership_tab, "Groups & Membership")

        # Tab 2: Group Permissions
        perm_tab = QWidget()
        perm_layout = QVBoxLayout(perm_tab)
        perm_box = QGroupBox("Group Permissions")
        perm_box_layout = QVBoxLayout(perm_box)

        group_dropdown_layout = QHBoxLayout()
        group_dropdown_layout.addWidget(QLabel("Select Group:"))
        self.perm_group_combo = QComboBox()
        group_dropdown_layout.addWidget(self.perm_group_combo)
        group_dropdown_layout.addStretch()
        perm_box_layout.addLayout(group_dropdown_layout)

        self.perm_table = QTableWidget()
        self.perm_table.setColumnCount(1)
        self.perm_table.setHorizontalHeaderLabels(["Target Table"])
        self.perm_table.setSortingEnabled(True)
        self.perm_table.horizontalHeader().setStretchLastSection(True)
        perm_box_layout.addWidget(self.perm_table)

        perm_btn_layout = QHBoxLayout()
        add_perm_btn = QPushButton("Add Permission")
        add_perm_btn.clicked.connect(self.on_add_permission)
        perm_btn_layout.addWidget(add_perm_btn)

        remove_perm_btn = QPushButton("Remove Permission")
        remove_perm_btn.clicked.connect(self.on_remove_permission)
        perm_btn_layout.addWidget(remove_perm_btn)

        perm_btn_layout.addStretch()
        perm_box_layout.addLayout(perm_btn_layout)
        perm_layout.addWidget(perm_box)
        self.tabs.addTab(perm_tab, "Group Permissions")

        # Refresh button
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.load_data)
        main_layout.addWidget(refresh_btn)

        self.setLayout(main_layout)
        self.load_data()
        self.perm_group_combo.currentIndexChanged.connect(self.load_permissions)

    def load_data(self):
        self.load_groups()
        self.load_users()
        self.load_group_combo()

    def load_groups(self):
        try:
            c = self.connection.cursor()
            c.execute("SELECT GROUP_NAME, DESCRIPTION, EMAIL FROM BUSINESS_GROUPS ORDER BY GROUP_NAME")
            rows = c.fetchall()
            self.groups_table.setRowCount(0)
            for row in rows:
                r = self.groups_table.rowCount()
                self.groups_table.insertRow(r)
                self.groups_table.setItem(r,0,QTableWidgetItem(row["GROUP_NAME"]))
                self.groups_table.setItem(r,1,QTableWidgetItem(row["DESCRIPTION"] or ""))
                self.groups_table.setItem(r,2,QTableWidgetItem(row["EMAIL"] or ""))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def load_users(self):
        try:
            c = self.connection.cursor()
            c.execute("SELECT USER_ID, USERNAME, USER_GROUP FROM USERS ORDER BY USER_ID")
            rows = c.fetchall()
            self.users_table.setRowCount(0)
            for row in rows:
                rr = self.users_table.rowCount()
                self.users_table.insertRow(rr)
                self.users_table.setItem(rr, 0, QTableWidgetItem(str(row["USER_ID"])))
                self.users_table.setItem(rr, 1, QTableWidgetItem(row["USERNAME"]))
                self.users_table.setItem(rr, 2, QTableWidgetItem(row["USER_GROUP"]))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def load_group_combo(self):
        try:
            c = self.connection.cursor()
            c.execute("SELECT GROUP_NAME FROM BUSINESS_GROUPS ORDER BY GROUP_NAME")
            rows = c.fetchall()
            self.perm_group_combo.clear()
            for row in rows:
                self.perm_group_combo.addItem(row["GROUP_NAME"], row["GROUP_NAME"])
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def load_permissions(self):
        group = self.perm_group_combo.currentText().strip()
        try:
            c = self.connection.cursor()
            c.execute("SELECT TARGET_TABLE FROM GROUP_PERMISSIONS WHERE GROUP_NAME=?", (group,))
            rows = c.fetchall()
            self.perm_table.setRowCount(0)
            for row in rows:
                rr = self.perm_table.rowCount()
                self.perm_table.insertRow(rr)
                self.perm_table.setItem(rr, 0, QTableWidgetItem(row["TARGET_TABLE"]))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def get_selected_group(self):
        idx = self.groups_table.currentRow()
        if idx < 0:
            return None
        item = self.groups_table.item(idx, 0)
        if not item:
            return None
        return item.text().strip()

    def on_add_group(self):
        name, ok = QInputDialog.getText(self, "Add Group", "Group Name:")
        if not ok or not name.strip():
            return
        desc, ok2 = QInputDialog.getText(self, "Add Group", "Description:")
        if not ok2:
            desc = ""
        email, ok3 = QInputDialog.getText(self, "Add Group", "Email:")
        if not ok3:
            email = ""
        name = name.strip()
        if not name:
            return
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?", (name,))
            if c.fetchone():
                QMessageBox.warning(self, "Error", "Group already exists.")
                return
            c.execute("INSERT INTO BUSINESS_GROUPS(GROUP_NAME,DESCRIPTION,EMAIL) VALUES(?,?,?)",
                      (name, desc.strip(), email.strip()))
            c.connection.commit()
            QMessageBox.information(self, "Success", "Group added.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_rename_group(self):
        grp = self.get_selected_group()
        if not grp:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return
        new_name, ok = QInputDialog.getText(self, "Rename Group", "New group name:")
        if not ok or not new_name.strip():
            return
        new_name = new_name.strip()
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?", (new_name,))
            if c.fetchone():
                QMessageBox.warning(self, "Error", "New group name already exists.")
                return

            c.execute("BEGIN")
            c.execute("UPDATE BUSINESS_GROUPS SET GROUP_NAME=? WHERE GROUP_NAME=?", (new_name, grp))
            c.execute("UPDATE BRM_RULES SET OWNER_GROUP=? WHERE OWNER_GROUP=?", (new_name, grp))
            c.execute("COMMIT")

            add_audit_log(self.connection,
                          action="RENAME_GROUP",
                          table_name="BUSINESS_GROUPS",
                          record_id=grp,
                          action_by="Admin",
                          old_data={"old_group_name": grp},
                          new_data={"new_group_name": new_name})

            QMessageBox.information(self, "Success", f"Group renamed to {new_name}.")
            self.load_data()
        except Exception as e:
            c.execute("ROLLBACK")
            QMessageBox.critical(self, "DB Error", str(e))

    def on_delete_group(self):
        grp = self.get_selected_group()
        if not grp:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return
        confirm = QMessageBox.question(self, "Confirm", f"Delete group '{grp}'?")
        if confirm != QMessageBox.Yes:
            return
        try:
            c = self.connection.cursor()
            c.execute("DELETE FROM BUSINESS_GROUPS WHERE GROUP_NAME=?", (grp,))
            c.connection.commit()
            QMessageBox.information(self, "Success", "Group deleted.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_backup_group(self):
        grp = self.get_selected_group()
        if not grp:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return
        try:
            version = backup_group(self.connection, grp, "Admin")
            QMessageBox.information(self, "Backup Created",
                                    f"Group '{grp}' backed up as version {version}.")
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_restore_group(self):
        grp = self.get_selected_group()
        if not grp:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return

        c = self.connection.cursor()
        c.execute("""
        SELECT BACKUP_VERSION, BACKUP_TIMESTAMP
        FROM BRM_GROUP_BACKUPS
        WHERE GROUP_NAME=?
        ORDER BY BACKUP_VERSION DESC
        """,(grp,))
        rows = c.fetchall()
        if not rows:
            QMessageBox.information(self, "No Backups", f"No backups exist for group '{grp}'.")
            return

        items = []
        for row in rows:
            version = row["BACKUP_VERSION"]
            stamp = row["BACKUP_TIMESTAMP"]
            items.append(f"Version {version} (created {stamp})")

        sel, ok = QInputDialog.getItem(self, "Restore Group",
                                       "Choose backup version:", items, 0, False)
        if not ok:
            return

        import re
        match = re.search(r"Version\s+(\d+)", sel)
        if not match:
            return
        chosen_ver = int(match.group(1))

        confirm = QMessageBox.question(self,
            "Restore",
            f"Are you sure you want to restore group '{grp}' to version {chosen_ver}?\n"
            "This will overwrite current rules for that group!"
        )
        if confirm != QMessageBox.Yes:
            return

        try:
            restore_group(self.connection, grp, chosen_ver, "Admin")
            QMessageBox.information(self, "Restored",
                                    f"Group '{grp}' restored to version {chosen_ver}.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_add_user_to_group(self):
        uid = self.get_selected_user()
        if not uid:
            QMessageBox.warning(self, "No selection", "No user selected.")
            return
        grp, ok = QInputDialog.getText(self, "Add to Group", "Enter group name:")
        if not ok or not grp.strip():
            return
        try:
            c = self.connection.cursor()
            c.execute("SELECT * FROM BUSINESS_GROUPS WHERE GROUP_NAME=?", (grp.strip(),))
            if not c.fetchone():
                QMessageBox.warning(self, "Error", "Group not found.")
                return
            c.execute("SELECT * FROM USERS WHERE USER_ID=?", (uid,))
            user_data = c.fetchone()
            if not user_data:
                QMessageBox.warning(self, "Error", "User not found.")
                return
            if user_data["USER_GROUP"] == grp.strip():
                QMessageBox.warning(self, "Error", "User already in that group.")
                return
            c.execute("UPDATE USERS SET USER_GROUP=? WHERE USER_ID=?", (grp.strip(), uid))
            c.connection.commit()
            QMessageBox.information(self, "Success", "User added to group.")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_remove_user_from_group(self):
        uid = self.get_selected_user()
        if not uid:
            QMessageBox.warning(self, "No selection", "No user selected.")
            return
        confirm = QMessageBox.question(self, "Confirm", "Remove user from group?")
        if confirm != QMessageBox.Yes:
            return
        try:
            c = self.connection.cursor()
            # Naive approach: default them to BG1
            c.execute("UPDATE USERS SET USER_GROUP='BG1' WHERE USER_ID=?", (uid,))
            c.connection.commit()
            QMessageBox.information(self, "Success", "User removed from group (now BG1).")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def get_selected_user(self):
        idx = self.users_table.currentRow()
        if idx < 0:
            return None
        item = self.users_table.item(idx, 0)
        if not item:
            return None
        try:
            return int(item.text())
        except:
            return None

    def on_add_permission(self):
        group = self.perm_group_combo.currentText().strip()
        if not group:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return
        table, ok = QInputDialog.getText(self, "Add Permission", "Enter target table for group:")
        if not ok or not table.strip():
            return
        try:
            c = self.connection.cursor()
            c.execute("INSERT OR IGNORE INTO GROUP_PERMISSIONS(GROUP_NAME,TARGET_TABLE) VALUES(?,?)",
                      (group, table.strip()))
            c.connection.commit()
            QMessageBox.information(self, "Success",
                                    f"Permission for '{table.strip()}' added to '{group}'.")
            self.load_permissions()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_remove_permission(self):
        group = self.perm_group_combo.currentText().strip()
        if not group:
            QMessageBox.warning(self, "No selection", "No group selected.")
            return
        row = self.perm_table.currentRow()
        if row < 0:
            QMessageBox.warning(self, "No selection", "No permission row selected.")
            return
        item = self.perm_table.item(row, 0)
        if not item:
            QMessageBox.warning(self, "No selection", "No table in selected row.")
            return
        tbl = item.text().strip()
        confirm = QMessageBox.question(self, "Confirm",
                                       f"Remove permission '{tbl}' from '{group}'?")
        if confirm != QMessageBox.Yes:
            return
        try:
            c = self.connection.cursor()
            c.execute("DELETE FROM GROUP_PERMISSIONS WHERE GROUP_NAME=? AND TARGET_TABLE=?",
                      (group, tbl))
            c.connection.commit()
            QMessageBox.information(self, "Success",
                                    f"Permission for '{tbl}' removed from '{group}'.")
            self.load_permissions()
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))
################################################################################
# PART 5 of 6
# DESCRIPTION:
#   1) CtrlTablesTab
#   2) LineageGraphWidget (enhanced to show a simple parent->child rule graph)
#   3) LineageVisualizationTab (uses the new populate_graph() logic)
################################################################################

class CtrlTablesTab(QWidget):
    """
    Allows an Admin to view data in control tables (or any table).
    """
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection = connection
        layout = QVBoxLayout(self)

        self.table_list = [
            "USERS",
            "BUSINESS_GROUPS",
            "GROUP_PERMISSIONS",
            "BRM_RULE_TYPES",
            "BRM_RULES",
            "BRM_RULE_TABLE_DEPENDENCIES",
            "BRM_AUDIT_LOG",
            "BRM_COLUMN_MAPPING",
            "BRM_RULE_LINEAGE",
            "BRM_GROUP_BACKUPS"
        ]
        self.table_combo = QComboBox()
        for t in self.table_list:
            self.table_combo.addItem(t)
        layout.addWidget(QLabel("Select Table:"))
        layout.addWidget(self.table_combo)

        self.load_btn = QPushButton("Load Data")
        self.load_btn.clicked.connect(self.on_load_data)
        layout.addWidget(self.load_btn)

        self.table_view = QTableWidget(0, 0)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table_view)

        self.setLayout(layout)

    def on_load_data(self):
        tbl = self.table_combo.currentText()
        if not tbl:
            return
        try:
            c = self.connection.cursor()
            c.execute(f"PRAGMA table_info({tbl})")
            info = c.fetchall()
            col_names = [col["name"] for col in info]
            if not col_names:
                # fallback if PRAGMA didn't give columns
                c.execute(f"SELECT * FROM {tbl} LIMIT 1")
                col_names = [desc[0] for desc in c.description]
            c.execute(f"SELECT * FROM {tbl}")
            rows = c.fetchall()

            self.table_view.setRowCount(0)
            self.table_view.setColumnCount(len(col_names))
            self.table_view.setHorizontalHeaderLabels(col_names)

            for rd in rows:
                rr = self.table_view.rowCount()
                self.table_view.insertRow(rr)
                for j, cn in enumerate(col_names):
                    val = rd[cn]
                    self.table_view.setItem(rr, j, QTableWidgetItem(str(val) if val is not None else ""))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))


class LineageGraphWidget(QGraphicsView):
    """
    Displays a simple node-link diagram for rule parent->child relationships.
    """
    CLUSTER_THRESHOLD = 50

    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection = connection
        self.scene = QtWidgets.QGraphicsScene(self)
        self.setScene(self.scene)
        self.setRenderHint(QtGui.QPainter.Antialiasing)
        self.setDragMode(QtWidgets.QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QtWidgets.QGraphicsView.AnchorUnderMouse)

    def resetView(self):
        if self.scene and self.scene.sceneRect().isValid():
            self.fitInView(self.scene.sceneRect(), Qt.KeepAspectRatio)

    def populate_graph(self):
        """
        Build a simple node-link diagram of rules:
          - Each rule is a node
          - There's an edge from PARENT_RULE_ID -> RULE_ID
        """
        self.scene.clear()

        c = self.connection.cursor()
        c.execute("""
            SELECT RULE_ID, RULE_NAME, PARENT_RULE_ID
            FROM BRM_RULES
            ORDER BY RULE_ID
        """)
        rules = c.fetchall()
        if not rules:
            txt = QtWidgets.QGraphicsTextItem("No rules found.")
            self.scene.addItem(txt)
            return

        # We'll store a QGraphicsEllipseItem for each rule
        # and keep a map from RULE_ID -> item
        node_map = {}
        x_offset = 200
        y_offset = 80

        # A naive approach: place them in vertical columns based on PARENT_RULE_ID = None or not
        # We'll do a BFS layout from "root" rules (where PARENT_RULE_ID is null).
        roots = [r for r in rules if not r["PARENT_RULE_ID"]]
        if not roots:
            # fallback: treat all as "top-level"
            roots = rules

        # We'll keep a queue for BFS
        from collections import deque
        queue = deque()
        for r in roots:
            queue.append((r["RULE_ID"], 0, 0))  # (rule_id, depth, index_in_level)

        visited = set()
        level_map = {}  # depth -> how many so far at this depth

        while queue:
            rule_id, depth, index_in_level = queue.popleft()
            if rule_id in visited:
                continue
            visited.add(rule_id)

            # find the rule info
            rinfo = None
            for rr in rules:
                if rr["RULE_ID"] == rule_id:
                    rinfo = rr
                    break
            if not rinfo:
                continue

            # figure out how many we've already placed at 'depth'
            cur_count = level_map.get(depth, 0)
            level_map[depth] = cur_count + 1

            # draw a rectangle or ellipse
            node_item = QtWidgets.QGraphicsEllipseItem(0, 0, 120, 50)
            node_item.setBrush(QtGui.QBrush(QtCore.Qt.lightGray))
            node_item.setPen(QtGui.QPen(QtCore.Qt.black, 2))

            text_item = QtWidgets.QGraphicsTextItem(f"ID:{rinfo['RULE_ID']}\n{rinfo['RULE_NAME']}")
            text_item.setParentItem(node_item)

            # position
            x = depth * x_offset + 40
            y = (cur_count * (y_offset + 60)) + 50
            node_item.setPos(x, y)
            self.scene.addItem(node_item)
            node_map[rule_id] = node_item

            # find children
            children = [x for x in rules if x["PARENT_RULE_ID"] == rule_id]
            for ch in children:
                queue.append((ch["RULE_ID"], depth + 1, 0))

        # Now draw edges
        for r in rules:
            if r["PARENT_RULE_ID"]:
                parent_id = r["PARENT_RULE_ID"]
                child_id = r["RULE_ID"]
                if parent_id in node_map and child_id in node_map:
                    pitem = node_map[parent_id]
                    citem = node_map[child_id]
                    self.draw_edge(pitem, citem)

        # adjust scene rect
        self.scene.setSceneRect(self.scene.itemsBoundingRect())

    def draw_edge(self, itemA, itemB):
        # We'll connect the center of itemA to the center of itemB
        a_rect = itemA.sceneBoundingRect()
        b_rect = itemB.sceneBoundingRect()
        a_center = a_rect.center()
        b_center = b_rect.center()

        line = QtWidgets.QGraphicsLineItem(a_center.x(), a_center.y(), b_center.x(), b_center.y())
        line.setPen(QtGui.QPen(QtGui.QColor("darkblue"), 2))
        self.scene.addItem(line)


class LineageVisualizationTab(QWidget):
    """
    A tab for lineage visualization using the improved LineageGraphWidget.
    """
    def __init__(self, connection, parent=None):
        super().__init__(parent)
        self.connection = connection
        layout = QVBoxLayout(self)

        lbl = QLabel("Lineage Visualization (Parent->Child)")
        lbl.setStyleSheet("font-weight:bold;font-size:14px;")
        layout.addWidget(lbl)

        self.graph_widget = LineageGraphWidget(self.connection)
        layout.addWidget(self.graph_widget)

        btn_layout = QHBoxLayout()
        self.reset_btn = QPushButton("Reset View")
        self.reset_btn.clicked.connect(self.graph_widget.resetView)
        btn_layout.addWidget(self.reset_btn)

        self.refresh_btn = QPushButton("Refresh Graph")
        self.refresh_btn.clicked.connect(self.graph_widget.populate_graph)
        btn_layout.addWidget(self.refresh_btn)

        layout.addLayout(btn_layout)

        self.setLayout(layout)
        self.graph_widget.populate_graph()  # initial populate
################################################################################
# PART 6 of 6
# DESCRIPTION:
#   1) The BRMTool Main Window
#   2) The main() function
#   3) The __main__ guard
################################################################################

class BRMTool(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Business Rule Manager - Enhanced Implementation")
        self.resize(1200, 800)

        # Initialize in-memory DB
        self.connection = setup_in_memory_db()

        # Show a login dialog
        self.login_dialog = LoginDialog(self.connection)
        if self.login_dialog.exec_() == QDialog.Accepted:
            self.user_id = self.login_dialog.user_id
            self.user_group = self.login_dialog.user_group
            self.init_ui()
        else:
            sys.exit()

    def init_ui(self):
        cw = QWidget()
        self.setCentralWidget(cw)
        layout = QVBoxLayout(cw)

        # If Admin, provide "Switch User" top bar
        top_bar_layout = QHBoxLayout()
        if self.user_group == "Admin":
            self.switch_user_combo = QComboBox()
            self.populate_switch_user_combo()
            self.switch_user_button = QPushButton("Switch User")
            self.switch_user_button.clicked.connect(self.on_switch_user_click)

            top_bar_layout.addWidget(QLabel("Impersonate:"))
            top_bar_layout.addWidget(self.switch_user_combo)
            top_bar_layout.addWidget(self.switch_user_button)
            top_bar_layout.addStretch()
        layout.addLayout(top_bar_layout)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Main BRM Tab
        self.brm_tab = BusinessRuleManagementTab(self, self.connection, self.user_id, self.user_group)
        self.tabs.addTab(self.brm_tab, "Business Rule Management")

        # Admin tabs
        if self.user_group == "Admin":
            self.group_tab = GroupManagementTab(self, self.connection, self.user_id, self.user_group)
            self.tabs.addTab(self.group_tab, "Group Management")

            self.ctrl_tables_tab = CtrlTablesTab(self.connection)
            self.tabs.addTab(self.ctrl_tables_tab, "CTRL_TBLS")

        # Lineage Visualization tab
        self.lineage_tab = LineageVisualizationTab(self.connection)
        self.tabs.addTab(self.lineage_tab, "Lineage Visualization")

        self.init_real_time_updates()
        self.show()

    def populate_switch_user_combo(self):
        self.switch_user_combo.clear()
        try:
            c = self.connection.cursor()
            c.execute("SELECT USER_ID, USERNAME, USER_GROUP FROM USERS ORDER BY USER_ID")
            rows = c.fetchall()
            for row in rows:
                display_text = f"{row['USERNAME']} ({row['USER_GROUP']})"
                self.switch_user_combo.addItem(display_text, (row["USER_ID"], row["USER_GROUP"]))
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))

    def on_switch_user_click(self):
        if not self.switch_user_combo.count():
            return
        new_data = self.switch_user_combo.currentData()
        if not new_data:
            return
        new_user_id, new_user_group = new_data
        if new_user_id == self.user_id and new_user_group == self.user_group:
            return
        self.user_id = new_user_id
        self.user_group = new_user_group
        self.reinit_main_tabs()

    def reinit_main_tabs(self):
        self.tabs.clear()

        self.brm_tab = BusinessRuleManagementTab(self, self.connection, self.user_id, self.user_group)
        self.tabs.addTab(self.brm_tab, "Business Rule Management")

        if self.user_group == "Admin":
            self.group_tab = GroupManagementTab(self, self.connection, self.user_id, self.user_group)
            self.tabs.addTab(self.group_tab, "Group Management")

            self.ctrl_tables_tab = CtrlTablesTab(self.connection)
            self.tabs.addTab(self.ctrl_tables_tab, "CTRL_TBLS")

        self.lineage_tab = LineageVisualizationTab(self.connection)
        self.tabs.addTab(self.lineage_tab, "Lineage Visualization")

    def init_real_time_updates(self):
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh_dashboard)
        self.timer.start(5000)  # every 5 seconds

    def refresh_dashboard(self):
        if hasattr(self.brm_tab, 'rule_dashboard'):
            self.brm_tab.rule_dashboard.load_rules()

    # CRUD Launchers
    def launch_add_rule_dialog(self):
        rtypes = self.get_rule_types()
        dlg = EnhancedAddRuleDialog(self, self.connection, rtypes, self.user_group, self)
        dlg.exec_()

    def launch_update_rule_dialog(self):
        rtypes = self.get_rule_types()
        dlg = UpdateRuleDialog(self, self.connection, rtypes, self.user_group, self)
        dlg.exec_()

    def launch_deactivate_rule_dialog(self):
        dlg = DeactivateRuleDialog(self, self.connection, self.user_group, self)
        dlg.exec_()

    def launch_delete_rule_dialog(self):
        dlg = DeleteRuleDialog(self, self.connection, self.user_group, self)
        dlg.exec_()

    def launch_audit_log_viewer(self):
        dlg = AuditLogViewer(self.connection, self.user_group, self)
        dlg.exec_()

    def launch_search_rule_dialog(self):
        dlg = SearchRuleDialog(self.connection, self.user_group, self)
        dlg.exec_()

    def get_rule_types(self):
        try:
            c = self.connection.cursor()
            c.execute("SELECT RULE_TYPE_NAME,RULE_TYPE_ID FROM BRM_RULE_TYPES")
            rows = c.fetchall()
            return {row["RULE_TYPE_NAME"]: row["RULE_TYPE_ID"] for row in rows}
        except Exception as e:
            QMessageBox.critical(self, "DB Error", str(e))
            return {}

    def closeEvent(self, event):
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Windows")
    w = BRMTool()
    w.showMaximized()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
