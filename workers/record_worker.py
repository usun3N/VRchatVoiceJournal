import pyaudio
import wave
import queue
import numpy as np
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import multiprocessing
import uuid
from pathlib import Path

def record_worker(result_queue: multiprocessing.Queue,
                  command_queue: multiprocessing.Queue,
                  base_dir: str,
                  vc_device_index: int,
                  mic_device_index: int,
                  monoral_mic: bool = True,
                  rate: int = 44100,
                  chunk: int = 1024,
                  record_seconds: int = 600,
                  audio_format=pyaudio.paInt16,
                  timezone_str: str = "Asia/Tokyo"):
    
    FORMAT = audio_format
    RATE = rate
    CHUNK = chunk
    RECORD_SECONDS = record_seconds

    pa = pyaudio.PyAudio()
    mic_name = pa.get_device_info_by_index(mic_device_index).get('name')
    mic_channels = int(pa.get_device_info_by_index(mic_device_index).get('maxInputChannels', 1))
    vc_name = pa.get_device_info_by_index(vc_device_index).get('name')
    vc_channels = int(pa.get_device_info_by_index(vc_device_index).get('maxInputChannels', 1))
    output_channels = 2  # 出力は必ずステレオ
    print(f"[RecordWorker] Recording started. Mic: {mic_name}, VC: {vc_name}")
    mic_stream = pa.open(format=FORMAT,
                         channels=mic_channels,
                         rate=RATE,
                         input=True,
                         frames_per_buffer=CHUNK,
                         input_device_index=mic_device_index)

    vc_stream = pa.open(format=FORMAT,
                        channels=vc_channels,
                        rate=RATE,
                        input=True,
                        frames_per_buffer=CHUNK,
                        input_device_index=vc_device_index)
    
    bytes_per_sample = pa.get_sample_size(FORMAT)
    mute_bytes = b"\x00" * (CHUNK * output_channels * bytes_per_sample)
    pause = False
    recording = True
    mic_mute = False
    vc_mute = False
    worker_name = "RecordWorker-1"
    try:
        while recording:
            if pause:
                result_queue.put({"event": "record_paused", "worker": worker_name, "payload": {}})
                cmd_raw = command_queue.get()
                cmd = cmd_raw.get("task")
                print(f"[RecordWorker] Received command: {cmd}")
                if cmd == "resume":
                    pause = False
                    result_queue.put({"event": "record_resumed", "worker": worker_name, "payload": {}})
                elif cmd == "stop":
                    recording = False
                    result_queue.put({"event": "record_idle", "worker": worker_name, "payload": {}})
                    break
                elif cmd == "mic_mute":
                    mic_mute = True
                    continue
                elif cmd == "mic_unmute":
                    mic_mute = False
                    continue
                elif cmd == "vc_mute":
                    vc_mute = True
                    continue
                elif cmd == "vc_unmute":
                    vc_mute = False
                    continue
                else:
                    continue

            buffer = []
            session_id = uuid.uuid4()
            now = datetime.now(tz=ZoneInfo(timezone_str))
            date = now.strftime("%Y-%m-%d")
            file_name = now.strftime("%Y-%m-%d_%H-%M-%S")
            import re
            timestamp = re.sub(r"\+\d{2}:\d{2}$", "Z", now.isoformat(timespec="milliseconds"))
            base_path = Path(base_dir)
            dir_path = base_path / "data" / "audio" / date
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{file_name}.wav"
            result_queue.put({"event": "record_started", "worker": worker_name, "payload": {"session_id": str(session_id)}})
            for _ in range(int(RATE / CHUNK * RECORD_SECONDS)):
                mic_data = mic_stream.read(CHUNK, exception_on_overflow=False) if not mic_mute else mute_bytes
                vc_data = vc_stream.read(CHUNK, exception_on_overflow=False) if not vc_mute else mute_bytes
                
                mic_np = np.frombuffer(mic_data, dtype=np.int16).astype(np.int32)
                vc_np = np.frombuffer(vc_data, dtype=np.int16).astype(np.int32)

                # 各チャンネルのサンプル数を計算
                mic_samples_per_channel = len(mic_np) // mic_channels
                vc_samples_per_channel = len(vc_np) // vc_channels
                
                # 最小のサンプル数に合わせる
                min_samples = min(mic_samples_per_channel, vc_samples_per_channel)
                
                # データを適切な形状にリシェイプ
                if mic_channels == 1:
                    mic_np = mic_np[:min_samples]
                    mic_np = np.repeat(mic_np, 2)
                else:
                    mic_np = mic_np[:min_samples * mic_channels].reshape(-1, mic_channels)
                    if mic_channels == 1:
                        mic_np = np.repeat(mic_np, 2, axis=1)
                    elif mic_channels > 2:
                        # 2チャンネル以上の場合、最初の2チャンネルを使用
                        mic_np = mic_np[:, :2]
                
                if vc_channels == 1:
                    vc_np = vc_np[:min_samples]
                    vc_np = np.repeat(vc_np, 2)
                else:
                    vc_np = vc_np[:min_samples * vc_channels].reshape(-1, vc_channels)
                    if vc_channels == 1:
                        vc_np = np.repeat(vc_np, 2, axis=1)
                    elif vc_channels > 2:
                        # 2チャンネル以上の場合、最初の2チャンネルを使用
                        vc_np = vc_np[:, :2]

                # monoral_mic処理
                if monoral_mic:
                    mic_mono = mic_np.mean(axis=1)
                    mic_np = np.repeat(mic_mono[:, np.newaxis], 2, axis=1)
                
                # 最終的な形状を確認してから混合
                if mic_np.shape != vc_np.shape:
                    # 形状が一致しない場合、小さい方に合わせる
                    min_rows = min(mic_np.shape[0], vc_np.shape[0])
                    mic_np = mic_np[:min_rows]
                    vc_np = vc_np[:min_rows]
                
                gain = 0.8
                mixed = (mic_np + vc_np) * gain
                mixed = np.clip(mixed, -32768, 32767).astype(np.int16)
                buffer.append(mixed.tobytes())

                try:
                    cmd_raw = command_queue.get_nowait()
                    cmd = cmd_raw.get("task")
                    print(f"[RecordWorker] Received command: {cmd}")
                    if cmd == "pause":
                        pause = True
                        break
                    elif cmd == "stop":
                        recording = False
                        result_queue.put({"event": "record_idle", "worker": worker_name, "payload": {}})
                        break
                    elif cmd == "mic_mute":
                        mic_mute = True
                    elif cmd == "mic_unmute":
                        mic_mute = False
                    elif cmd == "vc_mute":
                        vc_mute = True
                    elif cmd == "vc_unmute":
                        vc_mute = False
                    
                except queue.Empty:
                    pass
            
            with wave.open(str(file_path), "wb") as wf:
                wf.setnchannels(output_channels)
                wf.setsampwidth(pa.get_sample_size(FORMAT))
                wf.setframerate(RATE)
                wf.writeframes(b"".join(buffer))

            length = len(buffer) * CHUNK / RATE
            result_queue.put({
                "event": "record_done",
                "worker": worker_name,
                "payload": {
                    "session_id": str(session_id),
                    "start_time": timestamp,
                    "length": length,
                    "file_path": str(file_path)
                }
            })
            result_queue.put({"event": "record_idle", "worker": worker_name, "payload": {}})
    except Exception as e:
        result_queue.put({
            "event": "error",
            "worker": worker_name,
            "payload": {"error_message": str(e)}
        })
    finally:
        mic_stream.stop_stream()
        mic_stream.close()
        vc_stream.stop_stream()
        vc_stream.close()
        pa.terminate()