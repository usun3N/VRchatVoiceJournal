import sys
import multiprocessing
import sqlite3
import json
from queue import Empty
from enum import IntEnum, auto
from typing import Dict, Any, Optional, Tuple
import uuid
import datetime
import pyaudio
import re
import os
import setup as setup_module
import subprocess

from PySide6.QtCore import QThread, Signal, QObject, Qt, QSize, Slot
from PySide6.QtGui import QIcon, QAction
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QPushButton, QTextBrowser, QLabel, QGroupBox,
                               QMenuBar, QSplitter, QSystemTrayIcon, QMenu, QStyle, QDialog, QComboBox, QMessageBox, QLineEdit, QSpinBox, QCheckBox, QFileDialog, QFormLayout)

# --- ワーカーのインポート ---
from workers.record_worker import record_worker
from workers.transcribe_worker import transcribe_worker
from workers.metagen_worker import metagen_worker

# --- モダンなダークテーマのスタイルシート ---
MODERN_STYLESHEET = """
QWidget {
    background-color: #2c313c;
    color: #f0f0f0;
    font-family: "Segoe UI", "Meiryo", sans-serif;
}
QMainWindow {
    background-color: #2c313c;
}
QMenuBar {
    background-color: #383c4a;
    color: #f0f0f0;
}
QMenuBar::item:selected {
    background-color: #5d6d7e;
}
QMenu {
    background-color: #383c4a;
    color: #f0f0f0;
    border: 1px solid #21252b;
}
QMenu::item:selected {
    background-color: #5d6d7e;
}
QGroupBox {
    background-color: #383c4a;
    border-radius: 8px;
    border: 1px solid #21252b;
    margin-top: 10px;
    padding: 10px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 2px 8px;
    background-color: #5d6d7e;
    border-radius: 4px;
}
QPushButton {
    background-color: #5d6d7e;
    color: #f0f0f0;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #7f8c8d;
}
QPushButton:pressed {
    background-color: #95a5a6;
}
QPushButton:checked {
    background-color: #3498db;
    color: white;
}
QTextBrowser {
    background-color: #21252b;
    border: 1px solid #383c4a;
    border-radius: 4px;
    color: #f0f0f0;
}
QLabel {
    color: #f0f0f0;
    padding: 2px;
}
QSplitter::handle {
    background-color: #5d6d7e;
}
QSplitter::handle:hover {
    background-color: #7f8c8d;
}
"""

# --- ワーカー状態管理用 Enum と定数 ---
class WorkerStatus(IntEnum):
    IDLE = 0
    RUNNING = 1
    PAUSED = 2
    ERROR = 3

WORKER_STATUS_TEXT = {
    WorkerStatus.IDLE: "🟢 待機中",
    WorkerStatus.RUNNING: "🟡 処理中",
    WorkerStatus.PAUSED: "⏸️ 停止中",
    WorkerStatus.ERROR: "🚨 エラー",
}
WORKER_STATUS_COLOR = {
    WorkerStatus.IDLE: "green",
    WorkerStatus.RUNNING: "orange",
    WorkerStatus.PAUSED: "gray",
    WorkerStatus.ERROR: "red",
}

def validate_config(config: Dict[str, Any]):
    """設定ファイルに必要なキーが存在するかを検証する。"""
    if not all(k in config for k in ["db_path", "base_dir", "record_worker", "transcribe_worker", "metagen_worker"]):
        raise KeyError("トップレベルの必須キーが不足しています。")

class Status(IntEnum):
    """各セッションの処理状態を示すEnum。"""
    ERROR = -1; PENDING = 0; TRANSCRIBE_DONE = 1; META_DONE = 2

class DatabaseManager:
    """DB接続と操作をカプセル化するクラス"""
    def __init__(self, db_path: str):
        try:
            self.conn: sqlite3.Connection = sqlite3.connect(db_path, check_same_thread=False)
            self._init_db()
        except (PermissionError, OSError) as e:
            from PySide6.QtWidgets import QApplication, QMessageBox
            import sys
            app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(None, "DBファイルロック", f"DBファイルがロックされているか、アクセスできません:\n{e}\n他のプロセスで開いていないか確認してください。")
            sys.exit(1)
        except Exception as e:
            from PySide6.QtWidgets import QApplication, QMessageBox
            import sys, shutil, os
            app = QApplication.instance() or QApplication(sys.argv)
            msg = QMessageBox()
            msg.setWindowTitle("DBエラー")
            msg.setText(f"DBファイルのオープンに失敗しました: {e}\n修復または初期化しますか？")
            repair_btn = msg.addButton("修復(バックアップ後新規作成)", QMessageBox.ButtonRole.AcceptRole)
            init_btn = msg.addButton("初期化(新規作成)", QMessageBox.ButtonRole.DestructiveRole)
            quit_btn = msg.addButton("終了", QMessageBox.ButtonRole.RejectRole)
            msg.setDefaultButton(repair_btn)
            msg.exec()
            if msg.clickedButton() == repair_btn:
                if os.path.exists(db_path):
                    shutil.copy2(db_path, db_path+".bak")
                    os.remove(db_path)
                self.conn = sqlite3.connect(db_path, check_same_thread=False)
                self._init_db()
            elif msg.clickedButton() == init_btn:
                if os.path.exists(db_path):
                    os.remove(db_path)
                self.conn = sqlite3.connect(db_path, check_same_thread=False)
                self._init_db()
            else:
                sys.exit(1)
    def _init_db(self):
        self.conn.execute("PRAGMA journal_mode=WAL;"); cursor = self.conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS recordings (id TEXT PRIMARY KEY, start_time TEXT, length REAL, file_path TEXT, status INTEGER);")
        cursor.execute("CREATE TABLE IF NOT EXISTS transcribes (id TEXT PRIMARY KEY, segments_json TEXT, FOREIGN KEY (id) REFERENCES recordings (id));")
        cursor.execute("CREATE TABLE IF NOT EXISTS markers (id TEXT PRIMARY KEY, session_id TEXT, timestamp REAL, label TEXT, FOREIGN KEY (session_id) REFERENCES recordings (id));")
        cursor.execute("CREATE TABLE IF NOT EXISTS tags (id TEXT PRIMARY KEY, session_id TEXT, tag TEXT, FOREIGN KEY (session_id) REFERENCES recordings (id));")
        self.conn.commit()
    def close(self):
        if self.conn: self.conn.close()
    def _update_status(self, session_id: str, status: Status):
        with self.conn: self.conn.execute("UPDATE recordings SET status = ? WHERE id = ?", (status.value, session_id))
    def find_pending_transcribe_job(self) -> Optional[Tuple[str, str]]:
        cursor = self.conn.cursor(); cursor.execute("SELECT id, file_path FROM recordings WHERE status = ? ORDER BY start_time ASC LIMIT 1", (Status.PENDING.value,)); return cursor.fetchone()
    def find_pending_meta_job(self) -> Optional[Tuple[str, str]]:
        cursor = self.conn.cursor(); cursor.execute("SELECT r.id, t.segments_json FROM recordings r JOIN transcribes t ON r.id = t.id WHERE r.status = ? ORDER BY r.start_time ASC LIMIT 1", (Status.TRANSCRIBE_DONE.value,)); return cursor.fetchone()
    def handle_record_done(self, message: Dict[str, Any]):
        p = message.get('payload', {}); sid = p.get('session_id')
        with self.conn: self.conn.execute("INSERT OR REPLACE INTO recordings VALUES (?, ?, ?, ?, ?)", (sid, p['start_time'], p['length'], p['file_path'], Status.PENDING.value))
    def handle_transcribe_done(self, message: Dict[str, Any]):
        p = message.get('payload', {}); sid = p.get('session_id', "")
        with self.conn: self.conn.execute("INSERT OR REPLACE INTO transcribes VALUES (?, ?)", (sid, json.dumps(p['segments_json']))); self._update_status(sid, Status.TRANSCRIBE_DONE)
    def handle_meta_done(self, message: Dict[str, Any]):
        p = message.get('payload', {}); sid = p.get('session_id', "")
        if not sid: return
        with self.conn:
            for marker in p.get('markers', []): self.conn.execute("INSERT INTO markers VALUES (?, ?, ?, ?)", (str(uuid.uuid4()), sid, marker.get('time'), marker.get('content')))
            for tag in p.get('tags', []): self.conn.execute("INSERT INTO tags VALUES (?, ?, ?)", (str(uuid.uuid4()), sid, tag))
            self._update_status(sid, Status.META_DONE)
    def handle_error(self, message: Dict[str, Any]):
        p = message.get('payload', {}); sid = p.get('session_id')
        if sid: self._update_status(sid, Status.ERROR)

class EventListener:
    """ワーカーからのイベントを処理し、GUIに通知するクラス"""
    def __init__(self, db_manager: DatabaseManager, result_queue: multiprocessing.Queue, command_queues: Dict[str, multiprocessing.Queue]):
        self.db_manager = db_manager; self.result_queue = result_queue; self.command_queues = command_queues
        self.is_running = True; self.signals: Optional[BackendSignals] = None
        self.ai_processing_paused = False
        self.event_handlers = {
            'record_started': self.handle_record_started,
            'record_done': self.db_manager.handle_record_done,
            'record_paused': self.handle_record_paused,
            'record_resumed': self.handle_record_resumed,
            'record_idle': self.handle_record_idle,
            'transcribe_started': self.handle_transcribe_started,
            'transcribe_done': self.db_manager.handle_transcribe_done,
            'transcribe_idle': self.handle_transcribe_idle,
            'meta_started': self.handle_meta_started,
            'meta_done': self.db_manager.handle_meta_done,
            'meta_idle': self.handle_meta_idle,
            'error': self.handle_error,
            'request_transcribe_job': self.handle_transcribe_job_request,
            'request_metagen_job': self.handle_meta_job_request,
            'toggle_ai_pause': self.toggle_ai_pause,
        }
    def log(self, message: str):
        now = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        msg = f"{now} {message}"
        if self.signals:
            self.signals.log_message.emit(msg)
        else:
            print(msg)
    def update_status(self, worker_name: str, status: WorkerStatus):
        if self.signals:
            self.signals.worker_status_changed.emit(worker_name, status.value)
        # ログ出力はここで行わない

    def handle_record_started(self, message: Dict[str, Any]):
        self.update_status("RecordWorker-1", WorkerStatus.RUNNING)
        self.log("録音ワーカーが開始されました。")

    def toggle_ai_pause(self, message: Dict[str, Any]):
        self.ai_processing_paused = not self.ai_processing_paused
        state = "一時停止中" if self.ai_processing_paused else "再開"
        self.log(f"AI処理を {state} にしました。")
        self.update_status("TranscribeWorker-1", WorkerStatus.PAUSED)
        self.update_status("MetaGenWorker-1", WorkerStatus.PAUSED)
    def handle_transcribe_job_request(self, message: Dict[str, Any]):
        worker_name = message.get("worker", ""); command_queue = self.command_queues.get(worker_name)
        if not command_queue: return
        if self.ai_processing_paused:
            command_queue.put({"task": "standby"}); return
        job = self.db_manager.find_pending_transcribe_job()
        if job:
            session_id, _ = job
            self.update_status("TranscribeWorker-1", WorkerStatus.RUNNING)
            command_queue.put({"task": "transcribe", "payload": {"session_id": session_id, "file_path": _}})
        else:
            command_queue.put({"task": "standby"})

    def handle_meta_job_request(self, message: Dict[str, Any]):
        worker_name = message.get("worker", ""); command_queue = self.command_queues.get(worker_name)
        if not command_queue: return
        if self.ai_processing_paused:
            command_queue.put({"task": "standby"}); return
        job = self.db_manager.find_pending_meta_job()
        if job:
            session_id, segments_json_str = job
            self.update_status("MetaGenWorker-1", WorkerStatus.RUNNING)
            command_queue.put({"task": "generate_meta", "payload": {"session_id": session_id, "segments_json": json.loads(segments_json_str)}})
        else:
            command_queue.put({"task": "standby"})
    def listen(self):
        self.log("🎧 Event listener started...")
        while self.is_running:
            try:
                message = self.result_queue.get(timeout=1)
                event = message.get('event', "")
                worker = message.get('worker', '')
                # 新しいイベント名に対応
                if event in ("record_started", "transcribe_started", "meta_started"):
                    self.update_status(worker, WorkerStatus.RUNNING)
                elif event in ("record_paused",):
                    self.update_status(worker, WorkerStatus.PAUSED)
                elif event in ("record_resumed",):
                    self.update_status(worker, WorkerStatus.RUNNING)
                elif event in ("record_done", "transcribe_done", "meta_done"):
                    self.update_status(worker, WorkerStatus.IDLE)
                elif event in ("record_idle", "transcribe_idle", "meta_idle"):
                    self.update_status(worker, WorkerStatus.IDLE)
                elif event == "error":
                    self.update_status(worker, WorkerStatus.ERROR)
                # 既存のイベントハンドラも呼ぶ
                handler = self.event_handlers.get(event)
                if handler:
                    try: handler(message)
                    except Exception as e: self.log(f"🚨 [ERROR] while handling '{event}': {e}")
                else:
                    self.log(f"🤔 [WARNING] Unknown event: '{event}'")
            except Empty: continue
        self.log("Listener loop finished.")

    def handle_record_paused(self, message: Dict[str, Any]):
        self.update_status("RecordWorker-1", WorkerStatus.PAUSED)
        self.log("録音ワーカーが一時停止しました。")

    def handle_record_resumed(self, message: Dict[str, Any]):
        self.update_status("RecordWorker-1", WorkerStatus.RUNNING)
        self.log("録音ワーカーが再開しました。")

    def handle_record_idle(self, message: Dict[str, Any]):
        self.update_status("RecordWorker-1", WorkerStatus.IDLE)
        self.log("録音ワーカーが待機状態になりました。")

    def handle_transcribe_started(self, message: Dict[str, Any]):
        self.update_status("TranscribeWorker-1", WorkerStatus.RUNNING)
        self.log("文字起こしワーカーが開始されました。")

    def handle_transcribe_idle(self, message: Dict[str, Any]):
        self.update_status("TranscribeWorker-1", WorkerStatus.IDLE)
        self.log("文字起こしワーカーが待機状態になりました。")

    def handle_meta_started(self, message: Dict[str, Any]):
        self.update_status("MetaGenWorker-1", WorkerStatus.RUNNING)
        self.log("メタデータ生成ワーカーが開始されました。")

    def handle_meta_idle(self, message: Dict[str, Any]):
        self.update_status("MetaGenWorker-1", WorkerStatus.IDLE)
        self.log("メタデータ生成ワーカーが待機状態になりました。")

    def handle_error(self, message: Dict[str, Any]):
        worker = message.get('worker', '不明なワーカー')
        error_info = message.get('payload', {}).get('error_message', '詳細不明')
        self.log(f"🚨 {worker} でエラー発生: {error_info}")
        self.db_manager.handle_error(message)

class DeviceSelectDialog(QDialog):
    def __init__(self, pa, parent=None):
        super().__init__(parent)
        self.setWindowTitle("デバイス選択")
        self.setModal(True)
        layout = QVBoxLayout(self)
        self.vc_combo = QComboBox(self)
        self.mic_combo = QComboBox(self)
        self.device_list = []
        for i in range(pa.get_device_count()):
            info = pa.get_device_info_by_index(i)
            name = fix_encoding(info['name'])
            self.device_list.append((i, name))
            self.vc_combo.addItem(f"{i}: {name}", i)
            self.mic_combo.addItem(f"{i}: {name}", i)
        layout.addWidget(QLabel("VCデバイスを選択:"))
        layout.addWidget(self.vc_combo)
        layout.addWidget(QLabel("マイクデバイスを選択:"))
        layout.addWidget(self.mic_combo)
        ok_btn = QPushButton("OK", self)
        ok_btn.clicked.connect(self.accept)
        layout.addWidget(ok_btn)
    def get_selected_indices(self):
        return self.vc_combo.currentData(), self.mic_combo.currentData()

class SettingsDialog(QDialog):
    def __init__(self, config, pa, parent=None):
        super().__init__(parent)
        self.setWindowTitle("設定")
        self.setModal(True)
        self.config = json.loads(json.dumps(config))  # ディープコピー
        self.pa = pa
        layout = QVBoxLayout(self)
        self.form_layouts = {}
        # record_worker
        record_group = QGroupBox("録音設定 (record_worker)")
        record_form = QFormLayout()
        # VCデバイス
        self.vc_combo = QComboBox(self)
        for i in range(pa.get_device_count()):
            info = pa.get_device_info_by_index(i)
            name = fix_encoding(info['name'])
            self.vc_combo.addItem(f"{i}: {name}", i)
            if i == self.config['record_worker'].get('vc_device_index', -1):
                self.vc_combo.setCurrentIndex(self.vc_combo.count()-1)
        record_form.addRow("VCデバイス", self.vc_combo)
        # マイクデバイス
        self.mic_combo = QComboBox(self)
        for i in range(pa.get_device_count()):
            info = pa.get_device_info_by_index(i)
            name = fix_encoding(info['name'])
            self.mic_combo.addItem(f"{i}: {name}", i)
            if i == self.config['record_worker'].get('mic_device_index', -1):
                self.mic_combo.setCurrentIndex(self.mic_combo.count()-1)
        record_form.addRow("マイクデバイス", self.mic_combo)
        # その他パラメータ
        self.monoral_mic = QCheckBox()
        self.monoral_mic.setChecked(self.config['record_worker'].get('monoral_mic', False))
        record_form.addRow("モノラルマイク", self.monoral_mic)
        self.rate = QSpinBox(); self.rate.setRange(8000, 192000)
        self.rate.setValue(self.config['record_worker'].get('rate', 48000))
        record_form.addRow("サンプリングレート", self.rate)
        self.chunk = QSpinBox(); self.chunk.setRange(64, 8192)
        self.chunk.setValue(self.config['record_worker'].get('chunk', 1024))
        record_form.addRow("チャンクサイズ", self.chunk)
        self.record_seconds = QSpinBox(); self.record_seconds.setRange(1, 3600)
        self.record_seconds.setValue(self.config['record_worker'].get('record_seconds', 300))
        record_form.addRow("録音秒数", self.record_seconds)
        record_group.setLayout(record_form)
        layout.addWidget(record_group)
        # transcribe_worker
        transcribe_group = QGroupBox("文字起こし設定 (transcribe_worker)")
        transcribe_form = QFormLayout()
        self.model_size = QLineEdit(self.config['transcribe_worker'].get('model_size', ''))
        transcribe_form.addRow("モデルサイズ", self.model_size)
        self.device = QLineEdit(self.config['transcribe_worker'].get('device', ''))
        transcribe_form.addRow("デバイス", self.device)
        self.compute_type = QLineEdit(self.config['transcribe_worker'].get('compute_type', ''))
        transcribe_form.addRow("compute_type", self.compute_type)
        self.wait_seconds = QSpinBox(); self.wait_seconds.setRange(0, 600)
        self.wait_seconds.setValue(self.config['transcribe_worker'].get('wait_seconds_if_no_job', 5))
        transcribe_form.addRow("待機秒数", self.wait_seconds)
        transcribe_group.setLayout(transcribe_form)
        layout.addWidget(transcribe_group)
        # metagen_worker
        meta_group = QGroupBox("メタデータ生成設定 (metagen_worker)")
        meta_form = QFormLayout()
        self.api_key = QLineEdit(self.config['metagen_worker'].get('api_key', ''))
        meta_form.addRow("APIキー", self.api_key)
        self.model_name = QLineEdit(self.config['metagen_worker'].get('model_name', ''))
        meta_form.addRow("モデル名", self.model_name)
        self.meta_wait_seconds = QSpinBox(); self.meta_wait_seconds.setRange(0, 600)
        self.meta_wait_seconds.setValue(self.config['metagen_worker'].get('wait_seconds_if_no_job', 5))
        meta_form.addRow("待機秒数", self.meta_wait_seconds)
        meta_group.setLayout(meta_form)
        layout.addWidget(meta_group)
        # 共通
        common_group = QGroupBox("共通設定")
        common_form = QFormLayout()
        self.db_path = QLineEdit(self.config.get('db_path', ''))
        common_form.addRow("DBパス", self.db_path)
        self.base_dir = QLineEdit(self.config.get('base_dir', ''))
        common_form.addRow("ベースディレクトリ", self.base_dir)
        common_group.setLayout(common_form)
        layout.addWidget(common_group)
        # ボタン
        btn_layout = QHBoxLayout()
        save_btn = QPushButton("保存", self)
        save_btn.clicked.connect(self.on_save_clicked)
        cancel_btn = QPushButton("キャンセル", self)
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(save_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
    def validate(self):
        # metagen_worker
        if not self.api_key.text().strip():
            QMessageBox.warning(self, "入力エラー", "APIキーは必須です。")
            return False
        if not self.model_name.text().strip():
            QMessageBox.warning(self, "入力エラー", "モデル名は必須です。")
            return False
        # 他にも必要なら追加
        return True
    def on_save_clicked(self):
        if self.validate():
            self.accept()
    def get_config(self):
        # record_worker
        self.config['record_worker']['vc_device_index'] = self.vc_combo.currentData()
        self.config['record_worker']['vc_device_name'] = self.vc_combo.currentText().split(': ',1)[1] if ': ' in self.vc_combo.currentText() else self.vc_combo.currentText()
        self.config['record_worker']['mic_device_index'] = self.mic_combo.currentData()
        self.config['record_worker']['mic_device_name'] = self.mic_combo.currentText().split(': ',1)[1] if ': ' in self.mic_combo.currentText() else self.mic_combo.currentText()
        self.config['record_worker']['monoral_mic'] = self.monoral_mic.isChecked()
        self.config['record_worker']['rate'] = self.rate.value()
        self.config['record_worker']['chunk'] = self.chunk.value()
        self.config['record_worker']['record_seconds'] = self.record_seconds.value()
        # transcribe_worker
        self.config['transcribe_worker']['model_size'] = self.model_size.text()
        self.config['transcribe_worker']['device'] = self.device.text()
        self.config['transcribe_worker']['compute_type'] = self.compute_type.text()
        self.config['transcribe_worker']['wait_seconds_if_no_job'] = self.wait_seconds.value()
        # metagen_worker
        self.config['metagen_worker']['api_key'] = self.api_key.text()
        self.config['metagen_worker']['model_name'] = self.model_name.text()
        self.config['metagen_worker']['wait_seconds_if_no_job'] = self.meta_wait_seconds.value()
        # 共通
        self.config['db_path'] = self.db_path.text()
        self.config['base_dir'] = self.base_dir.text()
        return self.config

class BackendApp:
    def __init__(self):
        config_path = 'config.json'
        try:
            with open(config_path, 'r', encoding='utf-8') as f: self.config = json.load(f)
            validate_config(self.config)
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            app = QApplication.instance() or QApplication(sys.argv)
            msg = QMessageBox()
            msg.setWindowTitle("設定ファイルエラー")
            msg.setText(f"config.jsonの読み込みに失敗しました: {e}\n修復または初期化しますか？")
            repair_btn = msg.addButton("修復(バックアップ後初期化)", QMessageBox.ButtonRole.AcceptRole)
            init_btn = msg.addButton("初期化(新規作成)", QMessageBox.ButtonRole.DestructiveRole)
            quit_btn = msg.addButton("終了", QMessageBox.ButtonRole.RejectRole)
            msg.setDefaultButton(repair_btn)
            msg.exec()
            if msg.clickedButton() == repair_btn:
                # バックアップして初期化
                if os.path.exists(config_path):
                    import shutil
                    shutil.copy2(config_path, config_path+".bak")
                self.config = setup_module.SetupWizard().collect_config() if hasattr(setup_module, 'SetupWizard') else {}
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, ensure_ascii=False, indent=2)
            elif msg.clickedButton() == init_btn:
                self.config = setup_module.SetupWizard().collect_config() if hasattr(setup_module, 'SetupWizard') else {}
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, ensure_ascii=False, indent=2)
            else:
                sys.exit(1)
        except (PermissionError, OSError) as e:
            app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(None, "設定ファイルロック", f"config.jsonがロックされているか、アクセスできません:\n{e}\n他のプロセスで開いていないか確認してください。")
            sys.exit(1)

        # --- デバイスindexとnameの整合性チェック ---
        pa = pyaudio.PyAudio()
        changed = False
        need_select = False
        for key in ["vc_device", "mic_device"]:
            idx_key = f"{key}_index"
            name_key = f"{key}_name"
            if idx_key in self.config["record_worker"] and name_key in self.config["record_worker"]:
                saved_index = self.config["record_worker"][idx_key]
                saved_name = self.config["record_worker"][name_key]
                resolved_index = device_index_resolver(pa, saved_index, saved_name)
                if resolved_index is not None and resolved_index != saved_index:
                    self.config["record_worker"][idx_key] = resolved_index
                    changed = True
                elif resolved_index is None:
                    print(f"警告: {key} のデバイスが見つかりません: {saved_name}")
                    need_select = True
        if changed:
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
        if need_select:
            app = QApplication.instance() or QApplication(sys.argv)
            while True:
                QMessageBox.warning(None, "デバイス警告", "デバイスが見つかりません。選択してください。")
                dlg = DeviceSelectDialog(pa)
                if dlg.exec() == QDialog.DialogCode.Accepted:
                    vc_idx, mic_idx = dlg.get_selected_indices()
                    self.config["record_worker"]["vc_device_index"] = vc_idx
                    self.config["record_worker"]["vc_device_name"] = pa.get_device_info_by_index(vc_idx)["name"]
                    self.config["record_worker"]["mic_device_index"] = mic_idx
                    self.config["record_worker"]["mic_device_name"] = pa.get_device_info_by_index(mic_idx)["name"]
                    with open('config.json', 'w', encoding='utf-8') as f:
                        json.dump(self.config, f, ensure_ascii=False, indent=2)
                    break
                else:
                    msg = QMessageBox()
                    msg.setWindowTitle("デバイス未選択")
                    msg.setText("デバイスが選択されませんでした。どうしますか？")
                    retry_btn = msg.addButton("再選択", QMessageBox.ButtonRole.AcceptRole)
                    init_btn = msg.addButton("初期化", QMessageBox.ButtonRole.DestructiveRole)
                    quit_btn = msg.addButton("終了", QMessageBox.ButtonRole.RejectRole)
                    msg.setDefaultButton(retry_btn)
                    msg.exec()
                    if msg.clickedButton() == retry_btn:
                        continue
                    elif msg.clickedButton() == init_btn:
                        # config初期化
                        self.config = setup_module.SetupWizard().collect_config() if hasattr(setup_module, 'SetupWizard') else {}
                        with open('config.json', 'w', encoding='utf-8') as f:
                            json.dump(self.config, f, ensure_ascii=False, indent=2)
                        break
                    else:
                        print("デバイス選択がキャンセルされました。終了します。", file=sys.stderr)
                        sys.exit(1)

        self.result_queue = multiprocessing.Queue(); self.command_queues = {}; self.workers = {}
        self.db_manager = DatabaseManager(self.config['db_path'])
        self.listener = EventListener(self.db_manager, self.result_queue, self.command_queues)
    def run(self):
        worker_defs = {
            "RecordWorker-1": (record_worker, self.config['record_worker']),
            "TranscribeWorker-1": (transcribe_worker, self.config['transcribe_worker']),
            "MetaGenWorker-1": (metagen_worker, self.config['metagen_worker']),
        }
        for name, (target, cfg) in worker_defs.items():
            cmd_q = multiprocessing.Queue(); self.command_queues[name] = cmd_q
            if name == "RecordWorker-1":
                # record_worker: (result_queue, command_queue, base_dir, vc_device_index, mic_device_index, monoral_mic, rate, chunk, record_seconds, audio_format, timezone_str)
                args = (self.result_queue, cmd_q, self.config['base_dir'], 
                       cfg['vc_device_index'], cfg['mic_device_index'], cfg['monoral_mic'], 
                       cfg['rate'], cfg['chunk'], cfg['record_seconds'])
            elif name == "TranscribeWorker-1":
                # transcribe_worker: (result_queue, command_queue, model_size, device, compute_type, wait_seconds)
                args = (self.result_queue, cmd_q, cfg['model_size'], cfg['device'], 
                       cfg['compute_type'], cfg['wait_seconds_if_no_job'])
            elif name == "MetaGenWorker-1":
                # metagen_worker: (result_queue, command_queue, api_key, model_name, wait_seconds)
                args = (self.result_queue, cmd_q, cfg['api_key'], cfg['model_name'], 
                       cfg['wait_seconds_if_no_job'])
            else:
                continue
            process = multiprocessing.Process(target=target, args=args, name=name); process.start(); self.workers[name] = process
        try: self.listener.listen()
        finally: self.stop()
    def stop(self):
        self.listener.log("\n🧹 Cleaning up resources...")
        self.listener.is_running = False
        for name, q in self.command_queues.items():
            try: q.put({"task": "stop"})
            except Exception: pass
        for name, process in self.workers.items():
            if name == "RecordWorker-1":
                process.join(timeout=10)
                if process.is_alive(): process.terminate(); process.join()
            else:
                if process.is_alive(): process.terminate(); process.join()
        if self.db_manager: self.db_manager.close()

# -----------------------------------------------------------------------------
# ▲▲▲ GUI関連クラス ▲▲▲
# -----------------------------------------------------------------------------

class BackendSignals(QObject):
    log_message = Signal(str)
    worker_status_changed = Signal(str, int)  # worker_name, WorkerStatusの値

class BackendThread(QThread):
    def __init__(self, signals: BackendSignals):
        super().__init__(); self.backend_app = BackendApp(); self.backend_app.listener.signals = signals
    def run(self): self.backend_app.run()
    def stop(self): self.backend_app.stop(); self.quit(); self.wait()

class QuitThread(QThread):
    finished_signal = Signal()
    def __init__(self, backend_thread):
        super().__init__()
        self.backend_thread = backend_thread
    def run(self):
        self.backend_thread.stop()
        self.finished_signal.emit()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VRChatVoiceJournal")
        self.setGeometry(100, 100, 800, 600)
        self.setWindowIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay))

        self.is_mic_muted = False; self.is_recording_paused = False
        self.is_ai_paused = False
        self.is_quitting = False  # 終了中フラグ

        self.signals = BackendSignals()
        self.backend_thread = BackendThread(self.signals)
        self.signals.log_message.connect(self.update_log)
        self.signals.worker_status_changed.connect(self.update_worker_status)

        self.init_ui()
        self.init_tray_icon()
        self.backend_thread.start()

    def init_ui(self):
        self.setStyleSheet(MODERN_STYLESHEET)
        menu_bar = self.menuBar()
        # ファイル・表示メニューは削除、設定アクションのみ
        settings_action = QAction("設定", self)
        settings_action.triggered.connect(self.open_settings_dialog)
        menu_bar.addAction(settings_action)
        # 終了ボタン
        quit_action = QAction("終了", self)
        quit_action.triggered.connect(self.quit_application)
        menu_bar.addAction(quit_action)
        
        central_widget = QWidget(); self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget); splitter = QSplitter(Qt.Orientation.Horizontal)
        
        left_pane = QWidget(); left_layout = QVBoxLayout(left_pane); left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        control_group = QGroupBox("操作盤")
        control_v_layout = QVBoxLayout()
        self.record_button = QPushButton("録音 一時停止"); self.record_button.setCheckable(True)
        self.mute_button = QPushButton("マイク ミュート"); self.mute_button.setCheckable(True)
        self.ai_pause_button = QPushButton("AI処理 一時停止"); self.ai_pause_button.setCheckable(True)
        
        # アイコンを設定
        self.record_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPause))
        self.mute_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaVolumeMuted))
        self.ai_pause_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserStop))
        
        control_v_layout.addWidget(self.record_button); control_v_layout.addWidget(self.mute_button); control_v_layout.addWidget(self.ai_pause_button)
        control_group.setLayout(control_v_layout)
        
        self.record_button.clicked.connect(self.toggle_recording_pause)
        self.mute_button.clicked.connect(self.toggle_mic_mute)
        self.ai_pause_button.clicked.connect(self.toggle_ai_pause)
        
        left_layout.addWidget(control_group)
        
        right_pane = QWidget(); right_layout = QVBoxLayout(right_pane)
        status_group = QGroupBox("ワーカー状態"); status_layout = QHBoxLayout()
        self.status_labels = {
            "RecordWorker-1": QLabel(), "TranscribeWorker-1": QLabel(), "MetaGenWorker-1": QLabel()
        }
        self.restart_buttons = {}
        for worker in self.status_labels:
            vbox = QVBoxLayout()
            vbox.addWidget(self.status_labels[worker])
            btn = QPushButton("再起動")
            btn.clicked.connect(lambda _, w=worker: self.restart_worker(w))
            vbox.addWidget(btn)
            status_layout.addLayout(vbox)
            self.restart_buttons[worker] = btn
        status_group.setLayout(status_layout)
        self.update_worker_status("RecordWorker-1", WorkerStatus.IDLE.value)
        self.update_worker_status("TranscribeWorker-1", WorkerStatus.IDLE.value)
        self.update_worker_status("MetaGenWorker-1", WorkerStatus.IDLE.value)

        log_group = QGroupBox("詳細ログ"); log_layout = QVBoxLayout()
        self.log_browser = QTextBrowser(); log_layout.addWidget(self.log_browser)
        log_group.setLayout(log_layout)
        right_layout.addWidget(status_group); right_layout.addWidget(log_group)
        splitter.addWidget(left_pane); splitter.addWidget(right_pane)
        splitter.setSizes([250, 550]); main_layout.addWidget(splitter)

    def init_tray_icon(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay))
        self.tray_icon.setToolTip("VRChatVoiceJournal")
        tray_menu = QMenu()
        show_action = QAction("表示", self)
        show_action.triggered.connect(self.show)
        quit_action = QAction("終了", self)
        quit_action.triggered.connect(self.quit_application)
        tray_menu.addAction(show_action)
        tray_menu.addAction(quit_action)
        self.tray_icon.setContextMenu(tray_menu); self.tray_icon.show()

    def update_log(self, message: str):
        self.log_browser.append(message)

    def update_worker_status(self, worker_name: str, status_value: int):
        status = WorkerStatus(status_value)
        if worker_name in self.status_labels:
            label = self.status_labels[worker_name]
            label.setText(f'<b>{worker_name.split("-")[0]}:</b> <b style="color:{WORKER_STATUS_COLOR[status]};">{WORKER_STATUS_TEXT[status]}</b>')

    def toggle_recording_pause(self):
        self.is_recording_paused = not self.is_recording_paused
        command = {"task": "pause"} if self.is_recording_paused else {"task": "resume"}
        self.record_button.setText("録音 再開" if self.is_recording_paused else "録音 一時停止")
        self.record_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay if self.is_recording_paused else QStyle.StandardPixmap.SP_MediaPause))
        self.backend_thread.backend_app.command_queues["RecordWorker-1"].put(command)

    def toggle_mic_mute(self):
        self.is_mic_muted = not self.is_mic_muted
        command = {"task": "mic_mute"} if self.is_mic_muted else {"task": "mic_unmute"}
        self.mute_button.setText("マイク ミュート 解除" if self.is_mic_muted else "マイク ミュート")
        self.mute_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MediaVolume if self.is_mic_muted else QStyle.StandardPixmap.SP_MediaVolumeMuted))
        self.backend_thread.backend_app.command_queues["RecordWorker-1"].put(command)

    def toggle_ai_pause(self):
        self.is_ai_paused = not self.is_ai_paused
        self.ai_pause_button.setText("AI処理 再開" if self.is_ai_paused else "AI処理 一時停止")
        self.ai_pause_button.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_BrowserReload if self.is_ai_paused else QStyle.StandardPixmap.SP_BrowserStop))
        self.backend_thread.backend_app.result_queue.put({"event": "toggle_ai_pause"})

    def open_settings_dialog(self):
        pa = pyaudio.PyAudio()
        dlg = SettingsDialog(self.backend_thread.backend_app.config, pa, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            new_config = dlg.get_config()
            old_config = self.backend_thread.backend_app.config
            # 差分検出
            changed_workers = set()
            if old_config['record_worker'] != new_config['record_worker']:
                changed_workers.add('RecordWorker-1')
            if old_config['transcribe_worker'] != new_config['transcribe_worker']:
                changed_workers.add('TranscribeWorker-1')
            if old_config['metagen_worker'] != new_config['metagen_worker']:
                changed_workers.add('MetaGenWorker-1')
            # config保存
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(new_config, f, ensure_ascii=False, indent=2)
            self.backend_thread.backend_app.config = new_config
            # ワーカー再起動
            for worker in changed_workers:
                self.restart_worker(worker)
            if changed_workers:
                QMessageBox.information(self, "再起動", f"{', '.join(changed_workers)} を再起動しました。")
    def restart_worker(self, worker_name):
        # ワーカーを停止して再起動
        app = self.backend_thread.backend_app
        if worker_name in app.workers:
            # 停止
            app.command_queues[worker_name].put({"task": "stop"})
            app.workers[worker_name].join(timeout=10)
            if app.workers[worker_name].is_alive():
                app.workers[worker_name].terminate()
                app.workers[worker_name].join(timeout=5)
            if app.workers[worker_name].is_alive():
                import signal
                try:
                    app.workers[worker_name].kill()
                except Exception:
                    pass
                app.workers[worker_name].join(timeout=2)

    def closeEvent(self, event):
        if self.is_quitting:
            event.accept()
        else:
            event.ignore(); self.hide()
            self.tray_icon.showMessage("収納しました", "アプリはバックグラウンドで実行中です。", QSystemTrayIcon.MessageIcon.Information, 2000)

    def quit_application(self):
        msg = QMessageBox(self)
        msg.setWindowTitle("本当に終了しますか？")
        msg.setText("本当に終了しますか？\nバックグラウンドの処理も停止します。")
        yes_btn = msg.addButton("はい", QMessageBox.ButtonRole.AcceptRole)
        minimize_btn = msg.addButton("トレイに収納", QMessageBox.ButtonRole.DestructiveRole)
        cancel_btn = msg.addButton("キャンセル", QMessageBox.ButtonRole.RejectRole)
        msg.setDefaultButton(yes_btn)
        msg.exec()
        if msg.clickedButton() == yes_btn:
            self.is_quitting = True
            self.quit_thread = QuitThread(self.backend_thread)
            self.quit_thread.finished_signal.connect(QApplication.quit)
            self.quit_thread.start()
        elif msg.clickedButton() == minimize_btn:
            self.hide()

def fix_encoding(name):
    m = re.match(r"^(.*?)(\((.*?)\))?$", name)
    if m:
        outer = m.group(1)
        paren = m.group(2)
        inner = m.group(3)
        try:
            outer_fixed = outer.encode("cp932").decode("utf-8")
        except Exception:
            outer_fixed = outer
        if inner is not None:
            try:
                inner_fixed = inner.encode("cp932").decode("utf-8")
            except Exception:
                inner_fixed = inner
            paren_fixed = f"({inner_fixed})"
        else:
            paren_fixed = ""
        return f"{outer_fixed}{paren_fixed}"
    else:
        try:
            return name.encode("cp932").decode("utf-8")
        except Exception:
            return name

def device_index_resolver(pa, saved_index, saved_name):
    try:
        info = pa.get_device_info_by_index(saved_index)
        if info["name"] == saved_name:
            return saved_index
    except Exception:
        pass
    for i in range(pa.get_device_count()):
        info = pa.get_device_info_by_index(i)
        if info["name"] == saved_name:
            return i
    return None

if __name__ == "__main__":
    multiprocessing.freeze_support()
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)
    config_path = 'config.json'
    if not os.path.exists(config_path):
        # サブプロセスでsetup.pyをウィンドウ非表示で実行
        kwargs = {}
        if sys.platform == 'win32':
            import subprocess
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
        ret = subprocess.run([sys.executable, 'setup.py'], **kwargs)
        if not os.path.exists(config_path):
            print('config.jsonの生成に失敗しました。', file=sys.stderr)
            sys.exit(1)
    try:
        import pyaudio
    except ImportError:
        from PySide6.QtWidgets import QApplication, QMessageBox
        import sys
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "PyAudio未導入", "PyAudioライブラリがインストールされていません。\npip install pyaudio を実行してください。")
        sys.exit(1)
    try:
        import setup as setup_module
    except ImportError:
        from PySide6.QtWidgets import QApplication, QMessageBox
        import sys
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "setup.py未導入", "setup.pyが見つかりません。初期セットアップをやり直してください。")
        sys.exit(1)
    try:
        from workers.record_worker import record_worker
        from workers.transcribe_worker import transcribe_worker
        from workers.metagen_worker import metagen_worker
    except ImportError as e:
        from PySide6.QtWidgets import QApplication, QMessageBox
        import sys
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "ワーカー未導入", f"ワーカーモジュールのインポートに失敗しました:\n{e}\n必要なファイルが揃っているか確認してください。")
        sys.exit(1)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
