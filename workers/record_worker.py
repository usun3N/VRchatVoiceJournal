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
                  channels: int = 2,
                  rate: int = 44100,
                  chunk: int = 1024,
                  record_seconds: int = 600,
                  audio_format=pyaudio.paInt16,
                  timezone_str: str = "Asia/Tokyo"):
    
    FORMAT = audio_format
    CHANNELS = channels
    RATE = rate
    CHUNK = chunk
    RECORD_SECONDS = record_seconds

    pa = pyaudio.PyAudio()
    mic_name = pa.get_device_info_by_index(mic_device_index).get('name')
    vc_name = pa.get_device_info_by_index(vc_device_index).get('name')
    print(f"[RecordWorker] Recording started. Mic: {mic_name}, VC: {vc_name}")
    mic_stream = pa.open(format=FORMAT,
                         channels=CHANNELS,
                         rate=RATE,
                         input=True,
                         frames_per_buffer=CHUNK,
                         input_device_index=mic_device_index)

    vc_stream = pa.open(format=FORMAT,
                        channels=CHANNELS,
                        rate=RATE,
                        input=True,
                        frames_per_buffer=CHUNK,
                        input_device_index=vc_device_index)
    
    pause = False
    recording = True
    mic_mute = False
    vc_mute = False
    try:
        while recording:
            if pause:
                cmd_raw = command_queue.get()
                cmd = cmd_raw.get("task")
                print(f"[RecordWorker] Received command: {cmd}")
                if cmd == "resume":
                    pause = False
                elif cmd == "stop":
                    recording = False
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
            timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            base_path = Path(base_dir)
            dir_path = base_path / "data" / "audio" / date
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{file_name}.wav"
            result_queue.put({"event": "record_started", "worker": "record_worker", "payload": {"session_id": str(session_id)}})
            for _ in range(int(RATE / CHUNK * RECORD_SECONDS)):
                mic_data = mic_stream.read(CHUNK, exception_on_overflow=False) if not mic_mute else b"\x00" * CHUNK
                vc_data = vc_stream.read(CHUNK, exception_on_overflow=False) if not vc_mute else b"\x00" * CHUNK
                
                mic_np = np.frombuffer(mic_data, dtype=np.int16).astype(np.int32)
                vc_np = np.frombuffer(vc_data, dtype=np.int16).astype(np.int32)
                
                if monoral_mic:
                    mic_stereo = mic_np.reshape(-1, 2)
                    mic_mono = mic_stereo.mean(axis=1)
                    mic_np = np.repeat(mic_mono, 2).astype(np.int32)
                
                gain = 0.8
                mixed = (mic_np + vc_np) * gain
                mixed = np.clip(mixed, -32768, 32767).astype(np.int16)
                buffer.append(mixed.tobytes())

                try:
                    cmd_raw = command_queue.get_nowait()
                    cmd = cmd_raw.get("task")
                    print(f"[RecordWorker] Received command: {cmd}")
                    if cmd == "pause":
                        result_queue.put({"event": "record_paused", "worker": "record_worker", "payload": {}})
                        pause = True
                        break
                    elif cmd == "stop":
                        recording = False
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
                wf.setnchannels(CHANNELS)
                wf.setsampwidth(pa.get_sample_size(FORMAT))
                wf.setframerate(RATE)
                wf.writeframes(b"".join(buffer))

            length = len(buffer) * CHUNK / RATE
            result_queue.put({
                "event": "record_done",
                "worker": "record_worker",
                "payload": {
                    "session_id": str(session_id),
                    "start_time": timestamp,
                    "length": length,
                    "file_path": str(file_path)
                }
            })
    except Exception as e:
        result_queue.put({
            "event": "error",
            "worker": "record_worker",
            "payload": {"error_message": str(e)}
        })
    finally:
        mic_stream.stop_stream()
        mic_stream.close()
        vc_stream.stop_stream()
        vc_stream.close()
        pa.terminate()