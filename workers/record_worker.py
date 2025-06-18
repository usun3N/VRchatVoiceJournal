import pyaudio
import wave
import queue
from datetime import datetime, timezone
import multiprocessing
import os
import uuid

def resolve_stereo_mix_index(pa: pyaudio.PyAudio):
    for i in range(pa.get_device_count()):
        info = pa.get_device_info_by_index(i)
        if "ステレオ ミキサー" in str(info["name"]):
            return i
    return None

def record_worker(result_queue: multiprocessing.Queue, command_queue: multiprocessing.Queue, base_dir: str):
    FORMAT = pyaudio.paInt16
    CHANNELS = 2
    RATE = 44100
    CHUNK = 1024
    RECORD_SECONDS = 600

    pa = pyaudio.PyAudio()

    device_index = resolve_stereo_mix_index(pa)
    if device_index is None:
        result_queue.put({
            "event": "error",
            "worker": "record_worker",
            "payload": {
                "error_message": "stereo mix not found"
            }
        })
        raise Exception("stereo mix not found")

    stream = pa.open(format=FORMAT,
                    channels=CHANNELS,
                    rate=RATE,
                    input=True,
                    frames_per_buffer=CHUNK,
                    input_device_index=device_index)
    
    pause = False
    recording = True
    print(f"record worker start")
    try:
        while recording:
            if pause:
                print("pause")
                command = command_queue.get()
                if command == "resume":
                    result_queue.put({
                        "event": "resume",
                        "worker": "record_worker",
                        "payload": {}
                    })
                    pause = False
                
                elif command == "stop":
                    recording = False
                    result_queue.put({
                        "event": "stop",
                        "worker": "record_worker",
                        "payload": {}
                    })
                    break
                else:
                    continue

            
            buffer = []
            session_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            date = now.strftime("%Y-%m-%d")
            file_name = now.strftime("%Y-%m-%d_%H-%M-%S")
            timestamp = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            dir_path = f"{base_dir}/data/audio/{date}"
            file_path = f"{dir_path}/{file_name}.wav"
            print(f"Recording to {file_path}")
            for _ in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
                data = stream.read(CHUNK)
                buffer.append(data)
                print(".", end="", flush=True)
                try:
                    command = command_queue.get_nowait()
                    if command == "pause":
                        print("pause command received", flush=True)
                        pause = True
                        result_queue.put({
                            "event": "pause",
                            "worker": "record_worker",
                            "payload": {}
                        })
                        break
                    elif command == "stop":
                        print("stop command received", flush=True)
                        recording = False
                        result_queue.put({
                            "event": "stop",
                            "worker": "record_worker",
                            "payload": {}
                        })
                        break
                except queue.Empty:
                    pass
            print(f"Finished recording to {file_path}", flush=True)
            print(len(buffer), flush=True)
            os.makedirs(dir_path, exist_ok=True)
            with wave.open(str(file_path), "wb") as wf:
                wf.setnchannels(CHANNELS)
                wf.setsampwidth(pa.get_sample_size(FORMAT))
                wf.setframerate(RATE)
                wf.writeframes(b"".join(buffer))
            print(f"Recorded to {file_path}", flush=True)
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
            "payload": {
                "error_message": str(e)
            }
        })
    finally:
        stream.stop_stream()
        stream.close()
        pa.terminate()
