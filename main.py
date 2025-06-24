#!/usr/bin/env python3
import sqlite3
import json
import multiprocessing
import sys
from queue import Empty
from enum import IntEnum
from typing import Dict, Any, Optional, Tuple

# 外部ワーカーのインポート
from workers.record_worker import record_worker
from workers.transcribe_worker import transcribe_worker

# --- 設定ファイル検証 ---
def validate_config(config: Dict[str, Any]):
    """設定ファイルに必要なキーが存在するかを検証する。"""
    if not all(k in config for k in ["db_path", "base_dir", "record_worker", "transcribe_worker"]):
        raise KeyError("トップレベルの必須キーが不足しています。")
    if not all(k in config["record_worker"] for k in ["vc_device_index", "mic_device_index"]):
        raise KeyError("record_workerの必須キーが不足しています。")
    if not all(k in config["transcribe_worker"] for k in ["model_size", "device", "wait_seconds_if_no_job"]):
        raise KeyError("transcribe_workerの必須キーが不足しています。")

# --- アプリケーションの状態を定義するEnum ---
class Status(IntEnum):
    """各セッションの処理状態を示すEnum。"""
    ERROR = -1
    PENDING = 0
    TRANSCRIBE_DONE = 1 # ★★★ 命名を統一 ★★★
    MARKER_DONE = 2
    TAG_DONE = 3

# --- データベース管理クラス ---
class DatabaseManager:
    def __init__(self, db_path: str):
        self.conn: sqlite3.Connection = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        print("Initializing database...")
        self.conn.execute("PRAGMA journal_mode=WAL;")
        cursor = self.conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS recordings (
            id TEXT PRIMARY KEY, start_time TEXT NOT NULL, length REAL,
            file_path TEXT, status INTEGER NOT NULL);
        """)
        # ★★★ テーブル名を統一 ★★★
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transcribes (
            id TEXT PRIMARY KEY, segments_json TEXT,
            FOREIGN KEY (id) REFERENCES recordings (id));
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS markers (
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, timestamp REAL NOT NULL,
            label TEXT, category TEXT, FOREIGN KEY (session_id) REFERENCES recordings (id));
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, tag TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES recordings (id));
        """)
        self.conn.commit()
        print("Database initialized.")

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def _update_status(self, session_id: str, status: Status):
        with self.conn:
            self.conn.execute("UPDATE recordings SET status = ? WHERE id = ?", (status.value, session_id))

    def find_pending_transcribe_job(self) -> Optional[Tuple[str, str]]:
        """
        ステータスがPENDINGの文字起こしジョブを1件探して返す。
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, file_path FROM recordings WHERE status = ? ORDER BY start_time ASC LIMIT 1",
            (Status.PENDING.value,)
        )
        return cursor.fetchone()

    # --- イベントハンドラ（DB操作メソッド） ---
    def handle_record_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id')
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO recordings (id, start_time, length, file_path, status) VALUES (?, ?, ?, ?, ?)",
                (session_id, payload['start_time'], payload['length'], payload['file_path'], Status.PENDING.value)
            )

    # ★★★ メソッド名、テーブル名、ステータス名を統一 ★★★
    def handle_transcribe_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id', "")
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO transcribes (id, segments_json) VALUES (?, ?)",
                              (session_id, json.dumps(payload['segments_json'])))
            self._update_status(session_id, Status.TRANSCRIBE_DONE)

    def handle_marker_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id', "")
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO markers (id, session_id, timestamp, label, category) VALUES (?, ?, ?, ?, ?)",
                (payload['id'], session_id, payload['timestamp'], payload['label'], payload['category'])
            )
            self._update_status(session_id, Status.MARKER_DONE)

    def handle_tag_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id', "")
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO tags (id, session_id, tag) VALUES (?, ?, ?)",
                (payload['id'], session_id, payload['tag'])
            )
            self._update_status(session_id, Status.TAG_DONE)

    def handle_error(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id')
        if session_id: self._update_status(session_id, Status.ERROR)
        print(f"[ERROR] from '{payload.get('worker')}': {payload.get('error_message')}")

# --- イベントリスナークラス ---
class EventListener:
    def __init__(self, db_manager: DatabaseManager, result_queue: multiprocessing.Queue, command_queues: Dict[str, multiprocessing.Queue]):
        self.db_manager = db_manager
        self.result_queue = result_queue
        self.command_queues = command_queues
        self.event_handlers = {
            'record_done': self.db_manager.handle_record_done,
            'transcribe_done': self.db_manager.handle_transcribe_done, # ★★★ イベント名とハンドラ名を統一 ★★★
            'marker_done': self.db_manager.handle_marker_done,
            'tag_done': self.db_manager.handle_tag_done,
            'error': self.db_manager.handle_error,
            'request_transcribe_job': self.handle_transcribe_job_request,
        }

    def handle_transcribe_job_request(self, payload: Dict[str, Any]):
        worker_name = payload.get("worker", "")
        command_queue = self.command_queues.get(worker_name)
        if not command_queue:
            return

        job = self.db_manager.find_pending_transcribe_job() # ★★★ メソッド名を統一 ★★★
        if job:
            session_id, file_path = job
            command_queue.put({
                "task": "transcribe",
                "payload": {"session_id": session_id, "file_path": file_path}
            })
        else:
            command_queue.put({"task": "standby"})

    def listen(self):
        print("Event listener started...")
        try:
            while True:
                try:
                    message = self.result_queue.get(timeout=5)
                    self._process_message(message)
                except Empty:
                    continue
        except KeyboardInterrupt:
            print("\nShutdown signal received...")

    def _process_message(self, message: Dict[str, Any]):
        event = message.get('event', "")
        handler = self.event_handlers.get(event)
        if handler:
            try:
                handler(message.get('payload', {}))
            except Exception as e:
                print(f"[ERROR] while handling '{event}': {e}")
        else:
            print(f"[WARNING] Unknown event: '{event}'")


# --- メイン実行ブロック ---
def main():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        validate_config(config)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"[FATAL] 設定ファイルに問題があります: {e}", file=sys.stderr)
        sys.exit(1)

    result_queue = multiprocessing.Queue()
    command_queues = {}
    db_manager = DatabaseManager(config['db_path'])
    workers = {}

    try:
        # レコードワーカーを起動
        rec_worker_name = "RecordWorker-1"
        rec_worker_cfg = config['record_worker']
        rec_cmd_q = multiprocessing.Queue()
        command_queues[rec_worker_name] = rec_cmd_q
        rec_process = multiprocessing.Process(
            target=record_worker, args=(result_queue, rec_cmd_q, config['base_dir'], *rec_worker_cfg.values()),
            name=rec_worker_name)
        rec_process.start()
        workers[rec_worker_name] = rec_process
        print(f"Worker '{rec_worker_name}' started.")

        # 文字起こしワーカーを起動
        ts_worker_name = "TranscribeWorker-1"
        ts_worker_cfg = config['transcribe_worker']
        ts_cmd_q = multiprocessing.Queue()
        command_queues[ts_worker_name] = ts_cmd_q
        ts_process = multiprocessing.Process(
            target=transcribe_worker, args=(result_queue, ts_cmd_q, *ts_worker_cfg.values()),
            name=ts_worker_name)
        ts_process.start()
        workers[ts_worker_name] = ts_process
        print(f"Worker '{ts_worker_name}' started.")

        # イベントリスナーを起動
        listener = EventListener(db_manager, result_queue, command_queues)
        listener.listen()

    finally:
        print("\nCleaning up resources...")
        for name, process in workers.items():
            if process.is_alive():
                print(f"Terminating worker: {name}...")
                process.terminate()
                process.join(timeout=5)
        print("All workers terminated.")
        if db_manager:
            db_manager.close()
        print("Shutdown complete.")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
