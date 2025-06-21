import pyaudio
import wave
import queue
import numpy as np
from datetime import datetime, timezone
import multiprocessing
import os
import uuid

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
                  audio_format=pyaudio.paInt16):
    
    FORMAT = audio_format
    CHANNELS = channels
    RATE = rate
    CHUNK = chunk
    RECORD_SECONDS = record_seconds

    pa = pyaudio.PyAudio()

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
    try:
        while recording:
            if pause:
                cmd = command_queue.get()
                if cmd == "resume":
                    result_queue.put({"event": "resume", "worker": "record_worker", "payload": {}})
                    pause = False
                elif cmd == "stop":
                    recording = False
                    result_queue.put({"event": "stop", "worker": "record_worker", "payload": {}})
                    break
                else:
                    continue

            buffer = []
            session_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            date = now.strftime("%Y-%m-%d")
            file_name = now.strftime("%Y-%m-%d_%H-%M-%S")
            timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            dir_path = os.path.join(base_dir, "data/audio", date)
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, f"{file_name}.wav")

            for _ in range(int(RATE / CHUNK * RECORD_SECONDS)):
                mic_data = mic_stream.read(CHUNK)
                vc_data = vc_stream.read(CHUNK)
                
                mic_np = np.frombuffer(mic_data, dtype=np.int16).astype(np.int32)
                vc_np = np.frombuffer(vc_data, dtype=np.int16).astype(np.int32)
                
                if monoral_mic:
                    mic_stereo = mic_np.reshape(-1, 2)
                    mic_mono = mic_stereo.mean(axis=1)
                    mic_np = np.repeat(mic_mono, 2).astype(np.int32)
                    

                # 平和的ミキシング
                mixed = (mic_np + vc_np) // 2
                mixed = np.clip(mixed, -32768, 32767).astype(np.int16)
                buffer.append(mixed.tobytes())

                # (Command checkは変更なし)
                try:
                    cmd = command_queue.get_nowait()
                    if cmd == "pause":
                        pause = True
                        result_queue.put({"event": "pause", "worker": "record_worker", "payload": {}})
                        break
                    elif cmd == "stop":
                        recording = False
                        result_queue.put({"event": "stop", "worker": "record_worker", "payload": {}})
                        break
                except queue.Empty:
                    pass
            
            # (WAV書き込み、result_queueへの送信は変更なし)
            with wave.open(file_path, "wb") as wf:
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
                    "file_path": file_path
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