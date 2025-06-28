#!/usr/bin/env python3
import sqlite3
import json
import multiprocessing
import sys
from queue import Empty
from enum import IntEnum
from typing import Dict, Any, Optional, Tuple
import uuid

# 外部ワーカーのインポート
from workers.record_worker import record_worker
from workers.transcribe_worker import transcribe_worker
from workers.metagen_worker import metagen_worker

# --- 設定ファイル検証 ---
def validate_config(config: Dict[str, Any]):
    """設定ファイルに必要なキーが存在するかを検証する。"""
    if not all(k in config for k in ["db_path", "base_dir", "record_worker", "transcribe_worker", "metagen_worker"]):
        raise KeyError("トップレベルの必須キーが不足しています。")
    if not all(k in config["record_worker"] for k in ["vc_device_index", "mic_device_index"]):
        raise KeyError("record_workerの必須キーが不足しています。")
    if not all(k in config["transcribe_worker"] for k in ["model_size", "device", "wait_seconds_if_no_job"]):
        raise KeyError("transcribe_workerの必須キーが不足しています。")
    if not all(k in config["metagen_worker"] for k in ["api_key", "model_name", "wait_seconds_if_no_job"]):
        raise KeyError("metagen_workerの必須キーが不足しています。")

# --- アプリケーションの状態を定義するEnum ---
class Status(IntEnum):
    """各セッションの処理状態を示すEnum。"""
    ERROR = -1
    PENDING = 0
    TRANSCRIBE_DONE = 1
    META_DONE = 2

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
        """ステータスがPENDINGの文字起こしジョブを1件探して返す。"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, file_path FROM recordings WHERE status = ? ORDER BY start_time ASC LIMIT 1",
            (Status.PENDING.value,)
        )
        return cursor.fetchone()
    
    def find_pending_meta_job(self) -> Optional[Tuple[str, str]]:
        """ステータスがTRANSCRIBE_DONEのメタデータ生成ジョブを探して返す。"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT r.id, t.segments_json
            FROM recordings r
            JOIN transcribes t ON r.id = t.id
            WHERE r.status = ? 
            ORDER BY r.start_time ASC 
            LIMIT 1
        """, (Status.TRANSCRIBE_DONE.value,))
        return cursor.fetchone()

    def handle_record_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id')
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO recordings (id, start_time, length, file_path, status) VALUES (?, ?, ?, ?, ?)",
                (session_id, payload['start_time'], payload['length'], payload['file_path'], Status.PENDING.value)
            )

    def handle_transcribe_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id', "")
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO transcribes (id, segments_json) VALUES (?, ?)",
                              (session_id, json.dumps(payload['segments_json'])))
            self._update_status(session_id, Status.TRANSCRIBE_DONE)

    def handle_meta_done(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id', "")
        markers = payload.get('markers', [])
        tags = payload.get('tags', [])
        
        if not session_id:
            print("[ERROR] session_id is missing in meta_done payload.")
            return

        with self.conn:
            for marker in markers:
                self.conn.execute(
                    "INSERT INTO markers (id, session_id, timestamp, label, category) VALUES (?, ?, ?, ?, ?)",
                    (str(uuid.uuid4()), session_id, marker.get('time'), marker.get('content'), None)
                )
            for tag_text in tags:
                self.conn.execute(
                    "INSERT INTO tags (id, session_id, tag) VALUES (?, ?, ?)",
                    (str(uuid.uuid4()), session_id, tag_text)
                )
            self._update_status(session_id, Status.META_DONE)
            print(f"✅ Metadata stored for session {session_id}.")

    def handle_error(self, payload: Dict[str, Any]):
        session_id = payload.get('session_id')
        if session_id: self._update_status(session_id, Status.ERROR)
        print(f"🚨 [ERROR] from '{payload.get('worker')}': {payload.get('error_message')}")

# --- イベントリスナークラス ---
class EventListener:
    def __init__(self, db_manager: DatabaseManager, result_queue: multiprocessing.Queue, command_queues: Dict[str, multiprocessing.Queue]):
        self.db_manager = db_manager
        self.result_queue = result_queue
        self.command_queues = command_queues
        self.event_handlers = {
            'record_done': self.db_manager.handle_record_done,
            'transcribe_done': self.db_manager.handle_transcribe_done,
            'meta_done': self.db_manager.handle_meta_done,
            'error': self.db_manager.handle_error,
            'request_transcribe_job': self.handle_transcribe_job_request,
            'request_metagen_job': self.handle_meta_job_request,
        }

    def handle_transcribe_job_request(self, payload: Dict[str, Any]):
        worker_name = payload.get("worker", "")
        command_queue = self.command_queues.get(worker_name)
        if not command_queue: return

        job = self.db_manager.find_pending_transcribe_job()
        if job:
            session_id, file_path = job
            print(f"🚚 Assigning transcribe job {session_id} to {worker_name}")
            command_queue.put({
                "task": "transcribe",
                "payload": {"session_id": session_id, "file_path": file_path}
            })
        else:
            command_queue.put({"task": "standby"})

    def handle_meta_job_request(self, payload: Dict[str, Any]):
        worker_name = payload.get("worker", "")
        command_queue = self.command_queues.get(worker_name)
        if not command_queue: return

        job = self.db_manager.find_pending_meta_job()
        if job:
            session_id, segments_json_str = job
            try:
                segments = json.loads(segments_json_str)
                print(f"🚚 Assigning metagen job {session_id} to {worker_name}")
                command_queue.put({
                    "task": "generate_meta",
                    "payload": {"session_id": session_id, "segments_json": segments}
                })
            except json.JSONDecodeError as e:
                print(f"🚨 [ERROR] Failed to decode segments_json for session {session_id}: {e}")
                self.db_manager.handle_error({
                    'worker': 'EventListener', 'session_id': session_id,
                    'error_message': f"segments_jsonのデコードに失敗: {e}"
                })
        else:
            command_queue.put({"task": "standby"})

    def listen(self):
        print("🎧 Event listener started...")
        try:
            while True:
                try:
                    message = self.result_queue.get(timeout=5)
                    event = message.get('event', "")
                    handler = self.event_handlers.get(event)
                    if handler:
                        try:
                            handler(message.get('payload', {}))
                        except Exception as e:
                            print(f"🚨 [ERROR] while handling '{event}': {e}")
                    else:
                        print(f"🤔 [WARNING] Unknown event: '{event}'")
                except Empty:
                    continue
        except KeyboardInterrupt:
            print("\nShutdown signal received...")

# --- メイン実行ブロック ---
def main():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        validate_config(config)
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"🚨 [FATAL] 設定ファイルに問題があります: {e}", file=sys.stderr)
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
        print(f"🚀 Worker '{rec_worker_name}' started.")

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
        print(f"🚀 Worker '{ts_worker_name}' started.")

        # メタデータ生成ワーカーを起動
        meta_worker_name = "MetaGenWorker-1"
        meta_worker_cfg = config['metagen_worker']
        meta_cmd_q = multiprocessing.Queue()
        command_queues[meta_worker_name] = meta_cmd_q
        meta_process = multiprocessing.Process(
            target=metagen_worker, args=(result_queue, meta_cmd_q, *meta_worker_cfg.values()),
            name=meta_worker_name)
        meta_process.start()
        workers[meta_worker_name] = meta_process
        print(f"🚀 Worker '{meta_worker_name}' started.")

        # イベントリスナーを起動
        listener = EventListener(db_manager, result_queue, command_queues)
        listener.listen()

    finally:
        print("\n🧹 Cleaning up resources...")
        for name, q in command_queues.items():
            try:
                print(f"👋 Sending stop command to {name}...")
                q.put({"task": "stop"})
            except Exception as e:
                print(f"🚨 Error sending stop command to {name}: {e}")

        for name, process in workers.items():
            if process.is_alive():
                process.join(timeout=10)
                if process.is_alive():
                    print(f"😡 Worker '{name}' did not terminate, forcing it.")
                    process.terminate()
                    process.join()

        print("All workers terminated.")
        if db_manager:
            db_manager.close()
        print("Shutdown complete. Bye! 👋")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()