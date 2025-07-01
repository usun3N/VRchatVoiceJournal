import multiprocessing
import time
from queue import Empty
import whisper

def transcribe_worker(result_queue: multiprocessing.Queue,
                      command_queue: multiprocessing.Queue,
                      model_size: str,
                      device: str,
                      wait_seconds: int):
    """
    文字起こしタスクを処理するワーカープロセス。
    """
    print(f"Transcribe worker started. Model: {model_size}, Device: {device}")
    
    # Whisperモデルを一度だけロードする
    try:
        model = whisper.load_model(model_size, device=device)
        print("Whisper model loaded successfully.")
    except Exception as e:
        print(f"[FATAL] Failed to load Whisper model: {e}")
        # モデルロード失敗は致命的なので、エラーを通知して終了
        result_queue.put({
            "event": "error",
            "worker": "TranscribeWorker",
            "payload": {"error_message": f"Whisperモデルのロードに失敗: {e}"}
        })
        return

    worker_name = multiprocessing.current_process().name

    while True:
        try:
            # 1. メインプロセスに仕事があるか問い合わせる
            result_queue.put({
                "event": "request_transcribe_job",
                "worker": worker_name,
                "payload": {}
            })

            # 2. メインプロセスからの指令を待つ
            command = command_queue.get() # ここではブロックして待つ
            task = command.get("task")
            payload = command.get("payload", {})

            # 3. 指令に応じて処理を分岐
            if task == "transcribe":
                session_id = payload['session_id']
                file_path = payload['file_path']
                print(f"Received job: Transcribing {file_path}")

                # Whisperで文字起こしを実行
                result = model.transcribe(file_path, language="ja", fp16=False) # fp16は環境に応じて調整
                print(f"Transcription finished for {session_id}.")
                
                raw_segments: dict = result.get("segments", []) #type: ignore
                
                clean_segments = []
                # 型を教えてあげた変数を使えば、VSCodeはもう文句を言わない
                for segment in raw_segments:
                    clean_segments.append({
                        "start": segment.get("start", 0.0),
                        "end": segment.get("end", 0.0),
                        "text": segment.get("text", "").strip()
                    })
                
                # 4. 完了報告をメインプロセスに送る
                result_queue.put({
                    "event": "transcribe_done",
                    "worker": worker_name,
                    "payload": {
                        "session_id": session_id,
                        "segments_json": clean_segments
                    }
                })

            elif task == "standby":
                # 仕事がなかった場合、指定された時間だけ待機
                print(f"No job found. Standing by for {wait_seconds} seconds...")
                time.sleep(wait_seconds)
            
            elif task == "stop":
                print("Stop command received. Exiting worker.")
                break

            else:
                print(f"[WARNING] Unknown command received: {task}")

        except Exception as e:
            print(f"[ERROR] An unexpected error occurred in transcribe_worker: {e}")
            result_queue.put({
                "event": "error",
                "worker": worker_name,
                "payload": {"error_message": str(e)}
            })
            # エラー発生後、少し待ってから次の仕事を探しに行く
            time.sleep(10)

