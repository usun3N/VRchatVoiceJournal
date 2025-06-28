import multiprocessing
import time
from google import genai
from typing import List
from pydantic import BaseModel

# 注意！ライブラリのインストールコマンドが変わりました！
# pip install google-genai

class Markers(BaseModel):
    time: float
    content: str
class MetaResponse(BaseModel):
    markers: List[Markers]
    tags: List[str]

def generate_prompt(transcript_text: str) -> str:
    """
    Geminiに投げるためのプロンプトを生成する。
    test_metagen.pyを参考に、JSONモードでの出力に適した指示に修正。
    """
    prompt = f"""
あなたは、会話の文字起こしデータを分析し、構造化されたメタデータを抽出する専門家です。
Aiを使用した文字起こしデータのため、ミスが含まれる場合があります。
マーカーはその時の話題をできるだけ言い換えずに一言でまとめてください。
タグは、この文字起こしデータに関連する普遍的な単語を上げてください。
以下の【文字起こしデータ】を分析し、スキーマのとおりに会話のマーカーとタグを数個抽出してください。

【文字起こしデータ】
{transcript_text}
"""
    return prompt

def format_transcript(segments: list) -> str:
    """
    DBから取得したセグメント情報を、プロンプト用のテキスト形式に変換する。
    """
    lines = []
    for segment in segments:
        start = segment.get('start', 0)
        end = segment.get('end', 0)
        text = segment.get('text', '')
        lines.append(f"[{start:.2f}s -> {end:.2f}s] {text}")
    return "\n".join(lines)

def metagen_worker(result_queue: multiprocessing.Queue,
                     command_queue: multiprocessing.Queue,
                     api_key: str,
                     model_name: str,
                     wait_seconds: int):
    """
    メタデータ（マーカーとタグ）を生成するワーカープロセス。
    """
    print(f"Metagen worker started. Model: {model_name}")

    try:
        client = genai.Client(api_key=api_key)

    except Exception as e:
        print(f"[FATAL] Failed to configure Gemini model: {e}")
        result_queue.put({"event": "error", "worker": "MetagenWorker", "payload": {"error_message": f"Geminiの設定に失敗: {e}"}})
        return

    worker_name = multiprocessing.current_process().name

    while True:
        try:
            result_queue.put({"event": "request_metagen_job", "worker": worker_name, "payload": {}})

            command: dict = command_queue.get()
            task = command.get("task")
            payload = command.get("payload", {})

            if task == "generate_meta":
                session_id = payload['session_id']
                segments = payload['segments_json']
                print(f"Received job: Generating metadata for {session_id}")
                
                transcript_text = format_transcript(segments)
                prompt = generate_prompt(transcript_text)

                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config={
                        "response_mime_type": "application/json",
                        "response_schema": MetaResponse
                    }
                )

                meta_data: MetaResponse = response.parsed # type: ignore

                result_queue.put({
                    "event": "meta_done",
                    "worker": worker_name,
                    "payload": {
                        "session_id": session_id,
                        "markers": [marker.model_dump() for marker in meta_data.markers], # Pydanticモデルを辞書に変換
                        "tags": meta_data.tags
                    }
                })

                print(f"Metadata generation finished for {session_id}.")

            elif task == "standby":
                print(f"No job for metadata. Standing by for {wait_seconds} seconds...")
                time.sleep(wait_seconds)

            elif task == "stop":
                print("Stop command received. Exiting worker.")
                break

        except Exception as e:
            print(f"[ERROR] An unexpected error occurred in metagen_worker: {e}")
            result_queue.put({"event": "error", "worker": worker_name, "payload": {"error_message": str(e)}})
            time.sleep(10)