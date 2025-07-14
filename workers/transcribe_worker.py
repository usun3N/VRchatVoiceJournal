import multiprocessing
import time
from faster_whisper import WhisperModel

def transcribe_worker(result_queue: multiprocessing.Queue,
                      command_queue: multiprocessing.Queue,
                      model_size: str,
                      device: str,
                      compute_type: str,
                      wait_seconds: int):
    """
    文字起こしタスクを処理するワーカープロセス。(faster-whisper版)
    """
    print(f"Transcribe worker started. Model: {model_size}, Device: {device}, ComputeType: {compute_type}")
    
    worker_name = "TranscribeWorker-1"
    try:
        # compute_type を指定して、最適化されたモデルをロードする
        model = WhisperModel(model_size, device=device, compute_type=compute_type)
        print("Faster-Whisper model loaded successfully.")
    except Exception as e:
        print(f"[FATAL] Failed to load Faster-Whisper model: {e}")
        result_queue.put({
            "event": "error",
            "worker": worker_name,
            "payload": {"error_message": f"Faster-Whisperモデルのロードに失敗: {e}"}
        })
        return

    while True:
        try:
            # 1. メインプロセスに仕事があるか問い合わせる
            result_queue.put({
                "event": "request_transcribe_job",
                "worker": worker_name,
                "payload": {}
            })

            # 2. メインプロセスからの指令を待つ
            command = command_queue.get()
            task = command.get("task")
            payload = command.get("payload", {})

            # 3. 指令に応じて処理を分岐
            if task == "transcribe":
                session_id = payload['session_id']
                file_path = payload['file_path']
                print(f"Received job: Transcribing {file_path}")
                result_queue.put({"event": "transcribe_started", "worker": worker_name, "payload": {"session_id": session_id}})

                # ▼▼▼ 文字起こし部分を faster-whisper に変更 ▼▼▼
                segments_generator, info = model.transcribe(file_path, language="ja")
                print(f"Detected language '{info.language}' with probability {info.language_probability}")

                clean_segments = []
                # ジェネレータからセグメントを一つずつ取り出して処理する
                for segment in segments_generator:
                    clean_segments.append({
                        "start": segment.start,
                        "end": segment.end,
                        "text": segment.text.strip()
                    })
                
                print(f"Transcription finished for {session_id}.")
                
                # 4. 完了報告をメインプロセスに送る
                result_queue.put({
                    "event": "transcribe_done",
                    "worker": worker_name,
                    "payload": {
                        "session_id": session_id,
                        "segments_json": clean_segments
                    }
                })
                result_queue.put({"event": "transcribe_idle", "worker": worker_name, "payload": {}})

            elif task == "standby":
                print(f"No job found. Standing by for {wait_seconds} seconds...")
                result_queue.put({"event": "transcribe_idle", "worker": worker_name, "payload": {}})
                time.sleep(wait_seconds)
            
            elif task == "stop":
                print("Stop command received. Exiting worker.")
                result_queue.put({"event": "transcribe_idle", "worker": worker_name, "payload": {}})
                break

            else:
                print(f"[WARNING] Unknown command received: {task}")
                result_queue.put({"event": "transcribe_idle", "worker": worker_name, "payload": {}})

        except Exception as e:
            print(f"[ERROR] An unexpected error occurred in transcribe_worker: {e}")
            result_queue.put({
                "event": "error",
                "worker": worker_name,
                "payload": {"error_message": str(e)}
            })
            time.sleep(10)