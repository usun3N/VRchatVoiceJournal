import sys
import json
import os
import pyaudio
import re
from typing import Dict, Any, List, Optional
from PySide6.QtCore import Qt, QThread, Signal, QObject
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                               QHBoxLayout, QPushButton, QLabel, QComboBox, 
                               QLineEdit, QGroupBox, QTextEdit, QProgressBar,
                               QStackedWidget, QMessageBox, QSpinBox, QCheckBox)

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
    min-height: 30px;
}
QPushButton:hover {
    background-color: #7f8c8d;
}
QPushButton:pressed {
    background-color: #95a5a6;
}
QPushButton:disabled {
    background-color: #4a4a4a;
    color: #888888;
}
QComboBox {
    background-color: #383c4a;
    border: 1px solid #21252b;
    border-radius: 4px;
    padding: 5px;
    min-height: 25px;
}
QComboBox::drop-down {
    border: none;
}
QComboBox::down-arrow {
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 5px solid #f0f0f0;
}
QLineEdit {
    background-color: #383c4a;
    border: 1px solid #21252b;
    border-radius: 4px;
    padding: 5px;
    min-height: 25px;
}
QSpinBox {
    background-color: #383c4a;
    border: 1px solid #21252b;
    border-radius: 4px;
    padding: 5px;
    min-height: 25px;
}
QCheckBox {
    spacing: 8px;
}
QCheckBox::indicator {
    width: 18px;
    height: 18px;
    border: 2px solid #21252b;
    border-radius: 3px;
    background-color: #383c4a;
}
QCheckBox::indicator:checked {
    background-color: #3498db;
    border-color: #3498db;
}
QLabel {
    color: #f0f0f0;
    padding: 2px;
}
QProgressBar {
    border: 1px solid #21252b;
    border-radius: 4px;
    text-align: center;
    background-color: #383c4a;
}
QProgressBar::chunk {
    background-color: #3498db;
    border-radius: 3px;
}
"""

def fix_encoding(name):
    """デバイス名の文字化けを修正する関数"""
    # 正規表現でカッコの外側と内側を分離
    m = re.match(r"^(.*?)(\((.*?)\))?$", name)
    if m:
        outer = m.group(1)
        paren = m.group(2)  # "(xxx)" もしくは None
        inner = m.group(3)  # xxx もしくは None

        # 外側のエンコーディング修正
        try:
            outer_fixed = outer.encode("cp932").decode("utf-8")
        except Exception:
            outer_fixed = outer

        # 内側のエンコーディング修正
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
        # 分離できなければ従来通り
        try:
            return name.encode("cp932").decode("utf-8")
        except Exception:
            return name

class AudioDeviceScanner(QThread):
    """オーディオデバイスをスキャンするワーカースレッド"""
    devices_ready = Signal(dict)
    
    def run(self):
        try:
            p = pyaudio.PyAudio()
            devices = {
                'input': []
            }
            
            for i in range(p.get_device_count()):
                device_info = p.get_device_info_by_index(i)
                if int(device_info['maxInputChannels']) > 0:
                    devices['input'].append({
                        'index': i,
                        'name': fix_encoding(device_info['name']),
                        'channels': int(device_info['maxInputChannels'])
                    })
            
            p.terminate()
            self.devices_ready.emit(devices)
        except Exception as e:
            self.devices_ready.emit({'error': str(e)})

try:
    from google import genai
except ImportError:
    genai = None

class SetupWizard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VRChatVoiceJournal - 初回セットアップ")
        self.setGeometry(100, 100, 800, 600)
        self.setStyleSheet(MODERN_STYLESHEET)
        
        self.current_step = 0
        self.config = {}
        self.audio_devices = {}
        
        self.init_ui()
        self.scan_audio_devices()
    
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # ステップ表示
        self.step_label = QLabel("ステップ 1/4: VirtualCableの確認")
        self.step_label.setStyleSheet("font-size: 16px; font-weight: bold; margin: 10px;")
        main_layout.addWidget(self.step_label)
        
        # プログレスバー
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 4)
        self.progress_bar.setValue(1)
        main_layout.addWidget(self.progress_bar)
        
        # スタックウィジェット
        self.stacked_widget = QStackedWidget()
        main_layout.addWidget(self.stacked_widget)
        
        # 各ステップのウィジェットを作成
        self.create_step1_widget()  # VirtualCable確認
        self.create_step2_widget()  # 録音設定
        self.create_step3_widget()  # 文字起こし設定
        self.create_step4_widget()  # メタデータ生成設定
        
        # ナビゲーションボタン
        button_layout = QHBoxLayout()
        self.prev_button = QPushButton("前へ")
        self.next_button = QPushButton("次へ")
        self.finish_button = QPushButton("完了")
        
        self.prev_button.clicked.connect(self.prev_step)
        self.next_button.clicked.connect(self.next_step)
        self.finish_button.clicked.connect(self.finish_setup)
        
        button_layout.addWidget(self.prev_button)
        button_layout.addStretch()
        button_layout.addWidget(self.next_button)
        button_layout.addWidget(self.finish_button)
        
        main_layout.addLayout(button_layout)
        
        # 初期状態設定
        self.update_navigation_buttons()
    
    def create_step1_widget(self):
        """ステップ1: VirtualCable確認"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("VirtualCableの確認")
        group_layout = QVBoxLayout()
        
        # ステータス表示
        self.vc_status_label = QLabel("VirtualCableの確認中...")
        self.vc_status_label.setStyleSheet("font-weight: bold; color: #3498db;")
        group_layout.addWidget(self.vc_status_label)
        
        # VirtualCableの説明
        self.vc_info_text = QTextEdit()
        self.vc_info_text.setMaximumHeight(300)
        self.vc_info_text.setReadOnly(True)
        group_layout.addWidget(self.vc_info_text)
        
        # インストールボタン
        self.install_button = QPushButton("VirtualCableをインストール")
        self.install_button.clicked.connect(self.open_vc_install_page)
        self.install_button.setVisible(False)
        group_layout.addWidget(self.install_button)
        
        group.setLayout(group_layout)
        layout.addWidget(group)
        layout.addStretch()
        
        self.stacked_widget.addWidget(widget)
    
    def create_step2_widget(self):
        """ステップ2: 録音設定"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # 録音デバイス設定
        record_group = QGroupBox("録音デバイス設定")
        record_layout = QVBoxLayout()
        record_layout.addWidget(QLabel("VRChatの音声を録音するデバイスを選択してください:"))
        self.record_device_combo = QComboBox()
        record_layout.addWidget(self.record_device_combo)
        record_group.setLayout(record_layout)
        
        # マイク設定
        mic_group = QGroupBox("マイク設定")
        mic_layout = QVBoxLayout()
        mic_layout.addWidget(QLabel("録音に使用するマイクを選択してください:"))
        self.mic_device_combo = QComboBox()
        mic_layout.addWidget(self.mic_device_combo)
        self.monoral_checkbox = QCheckBox("モノラル録音を使用")
        self.monoral_checkbox.setChecked(True)
        mic_layout.addWidget(self.monoral_checkbox)
        mic_group.setLayout(mic_layout)
        
        # 詳細設定（折りたたみ）
        self.detail_record_checkbox = QCheckBox("詳細設定を表示")
        self.detail_record_checkbox.setChecked(False)
        self.detail_record_checkbox.toggled.connect(lambda checked: self.detail_record_widget.setVisible(checked))
        layout.addWidget(self.detail_record_checkbox)
        
        self.detail_record_widget = QWidget()
        detail_record_layout = QVBoxLayout(self.detail_record_widget)
        # 録音時間
        record_settings_layout = QHBoxLayout()
        record_settings_layout.addWidget(QLabel("録音時間 (秒):"))
        self.record_seconds_spin = QSpinBox()
        self.record_seconds_spin.setRange(60, 3600)
        self.record_seconds_spin.setValue(600)
        record_settings_layout.addWidget(self.record_seconds_spin)
        record_settings_layout.addStretch()
        detail_record_layout.addLayout(record_settings_layout)
        self.detail_record_widget.setVisible(False)
        
        layout.addWidget(record_group)
        layout.addWidget(mic_group)
        layout.addWidget(self.detail_record_widget)
        layout.addStretch()
        self.stacked_widget.addWidget(widget)

    def create_step3_widget(self):
        """ステップ3: 文字起こし設定"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("文字起こし設定")
        group_layout = QVBoxLayout()
        # モデルサイズ
        group_layout.addWidget(QLabel("Whisperモデルサイズ:"))
        self.model_size_combo = QComboBox()
        self.model_size_combo.addItems(["tiny", "base", "small", "medium", "large"])
        self.model_size_combo.setCurrentText("small")
        group_layout.addWidget(self.model_size_combo)
        
        # 詳細設定（折りたたみ）
        self.detail_transcribe_checkbox = QCheckBox("詳細設定を表示")
        self.detail_transcribe_checkbox.setChecked(False)
        self.detail_transcribe_checkbox.toggled.connect(lambda checked: self.detail_transcribe_widget.setVisible(checked))
        group_layout.addWidget(self.detail_transcribe_checkbox)
        
        self.detail_transcribe_widget = QWidget()
        detail_transcribe_layout = QVBoxLayout(self.detail_transcribe_widget)
        # デバイス
        detail_transcribe_layout.addWidget(QLabel("処理デバイス:"))
        self.device_combo = QComboBox()
        self.device_combo.addItems(["cpu", "cuda"])
        self.device_combo.setCurrentText("cpu")
        detail_transcribe_layout.addWidget(self.device_combo)
        # 計算タイプ
        detail_transcribe_layout.addWidget(QLabel("計算タイプ:"))
        self.compute_type_combo = QComboBox()
        self.compute_type_combo.addItems(["int8", "float16", "float32"])
        self.compute_type_combo.setCurrentText("int8")
        detail_transcribe_layout.addWidget(self.compute_type_combo)
        # 待機時間
        detail_transcribe_layout.addWidget(QLabel("待機時間 (秒):"))
        self.transcribe_wait_spin = QSpinBox()
        self.transcribe_wait_spin.setRange(10, 120)
        self.transcribe_wait_spin.setValue(30)
        detail_transcribe_layout.addWidget(self.transcribe_wait_spin)
        self.detail_transcribe_widget.setVisible(False)
        group_layout.addWidget(self.detail_transcribe_widget)
        
        group.setLayout(group_layout)
        layout.addWidget(group)
        layout.addStretch()
        self.stacked_widget.addWidget(widget)

    def create_step4_widget(self):
        """ステップ4: メタデータ生成設定"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("メタデータ生成設定")
        group_layout = QVBoxLayout()
        # APIキー
        api_key_layout = QHBoxLayout()
        api_key_layout.addWidget(QLabel("Gemini APIキー:"))
        self.api_key_help_button = QPushButton("取得方法")
        self.api_key_help_button.clicked.connect(self.show_gemini_api_help)
        api_key_layout.addWidget(self.api_key_help_button)
        group_layout.addLayout(api_key_layout)
        self.api_key_edit = QLineEdit()
        self.api_key_edit.setPlaceholderText("AIzaSy...")
        self.api_key_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_edit.editingFinished.connect(self.update_model_list_from_api)
        group_layout.addWidget(self.api_key_edit)
        
        # モデル名
        group_layout.addWidget(QLabel("モデル名:"))
        self.model_name_combo = QComboBox()
        # 2024年6月時点の主要Geminiモデルを追加
        model_list = [
            "gemini-1.0-pro",
            "gemini-1.5-pro",
            "gemini-2.0-pro",
            "gemini-2.0-flash",
            "gemini-2.5-pro",
            "gemini-2.5-flash"
        ]
        self.model_name_combo.addItems(model_list)
        self.model_name_combo.setCurrentText("gemini-2.5-flash")
        group_layout.addWidget(self.model_name_combo)
        
        # 詳細設定（折りたたみ）
        self.detail_meta_checkbox = QCheckBox("詳細設定を表示")
        self.detail_meta_checkbox.setChecked(False)
        self.detail_meta_checkbox.toggled.connect(lambda checked: self.detail_meta_widget.setVisible(checked))
        group_layout.addWidget(self.detail_meta_checkbox)
        
        self.detail_meta_widget = QWidget()
        detail_meta_layout = QVBoxLayout(self.detail_meta_widget)
        # 待機時間
        detail_meta_layout.addWidget(QLabel("待機時間 (秒):"))
        self.meta_wait_spin = QSpinBox()
        self.meta_wait_spin.setRange(30, 300)
        self.meta_wait_spin.setValue(60)
        detail_meta_layout.addWidget(self.meta_wait_spin)
        self.detail_meta_widget.setVisible(False)
        group_layout.addWidget(self.detail_meta_widget)
        
        group.setLayout(group_layout)
        layout.addWidget(group)
        layout.addStretch()
        self.stacked_widget.addWidget(widget)

    def show_gemini_api_help(self):
        msg = ("Gemini APIキーの取得方法:\n"
               "1. Google Cloud Platformにアクセスし、プロジェクトを作成\n"
               "2. Gemini APIを有効化\n"
               "3. 認証情報からAPIキーを作成\n"
               "4. 取得したAPIキーをここに貼り付けてください\n\n"
               "公式ドキュメント: https://ai.google.dev/gemini-api/docs/get-started")
        QMessageBox.information(self, "Gemini APIキーの取得方法", msg)

    def scan_audio_devices(self):
        """オーディオデバイスをスキャン"""
        self.scanner = AudioDeviceScanner()
        self.scanner.devices_ready.connect(self.on_devices_ready)
        self.scanner.start()
    
    def open_vc_install_page(self):
        """VirtualCableのインストールページを開く"""
        import webbrowser
        webbrowser.open("https://vb-audio.com/Cable/")

    def check_virtualcable_installed(self, devices):
        """VirtualCableがインストールされているかチェック"""
        cable_input_found = False
        cable_output_found = False
        
        for device in devices['input']:
            device_name = device['name'].lower()
            if 'cable input' in device_name or 'vb-audio virtual cable' in device_name:
                cable_input_found = True
            if 'cable output' in device_name or 'vb-audio virtual cable' in device_name:
                cable_output_found = True
        
        return cable_input_found and cable_output_found

    def on_devices_ready(self, devices):
        """オーディオデバイスのスキャン完了"""
        if 'error' in devices:
            self.vc_status_label.setText("❌ エラーが発生しました")
            self.vc_status_label.setStyleSheet("font-weight: bold; color: #e74c3c;")
            self.vc_info_text.setText(f"エラー: {devices['error']}")
            return
        
        self.audio_devices = devices
        
        # VirtualCableの確認
        vc_installed = self.check_virtualcable_installed(devices)
        
        if vc_installed:
            self.vc_status_label.setText("✅ VirtualCableがインストールされています")
            self.vc_status_label.setStyleSheet("font-weight: bold; color: #27ae60;")
            
            info_text = """✅ VirtualCableが正常にインストールされています！

【Windows音声設定】
1. Windows設定 → システム → サウンド を開く
2. 「サウンドの詳細設定」→「アプリの音量とデバイスの基本設定」
3. VRChatの出力デバイスを「CABLE Input」に変更
4. 「CABLE Input」の「このデバイスを聴く」にチェックを入れる

【次のステップ】
次のステップで「CABLE Output」を録音デバイスとして選択してください"""
        else:
            self.vc_status_label.setText("❌ VirtualCableがインストールされていません")
            self.vc_status_label.setStyleSheet("font-weight: bold; color: #e74c3c;")
            self.install_button.setVisible(True)
            
            info_text = """❌ VirtualCableがインストールされていません。

【VirtualCableのインストール手順】
1. 下の「VirtualCableをインストール」ボタンをクリック
2. ダウンロードしたインストーラーを実行
3. インストール完了後、PCを再起動
4. このアプリを再起動して再度確認"""
        
        self.vc_info_text.setText(info_text)
        
        # コンボボックスにデバイス名のみを追加
        self.record_device_combo.clear()
        self.mic_device_combo.clear()
        
        for device in devices['input']:
            self.record_device_combo.addItem(device['name'], device['index'])
            self.mic_device_combo.addItem(device['name'], device['index'])
        
        # デフォルト選択は行わない
        if self.record_device_combo.count() > 0:
            self.record_device_combo.setCurrentIndex(0)
        
        if self.mic_device_combo.count() > 0:
            self.mic_device_combo.setCurrentIndex(0)
    
    def update_navigation_buttons(self):
        """ナビゲーションボタンの状態を更新"""
        self.prev_button.setEnabled(self.current_step > 0)
        if self.current_step == 3:  # 最後のステップ
            self.next_button.setVisible(False)
            self.finish_button.setVisible(True)
        else:
            self.next_button.setVisible(True)
            self.finish_button.setVisible(False)
    
    def update_step_display(self):
        """ステップ表示を更新"""
        step_names = [
            "ステップ 1/4: VirtualCableの確認",
            "ステップ 2/4: 録音設定",
            "ステップ 3/4: 文字起こし設定",
            "ステップ 4/4: メタデータ生成設定"
        ]
        
        self.step_label.setText(step_names[self.current_step])
        self.progress_bar.setValue(self.current_step + 1)
        self.stacked_widget.setCurrentIndex(self.current_step)
        self.update_navigation_buttons()
    
    def prev_step(self):
        """前のステップに移動"""
        if self.current_step > 0:
            self.current_step -= 1
            self.update_step_display()
    
    def next_step(self):
        """次のステップに移動"""
        if self.current_step < 3:
            self.current_step += 1
            self.update_step_display()
    
    def validate_current_step(self):
        """現在のステップの入力値を検証"""
        if self.current_step == 0:
            # VirtualCableがインストールされているかチェック
            if len(self.audio_devices.get('input', [])) == 0:
                return False
            return self.check_virtualcable_installed(self.audio_devices)
        elif self.current_step == 1:
            return (self.record_device_combo.currentData() is not None and 
                   self.mic_device_combo.currentData() is not None)
        elif self.current_step == 2:
            return True  # デフォルト値があるので常に有効
        elif self.current_step == 3:
            return len(self.api_key_edit.text().strip()) > 0
        return True
    
    def collect_config(self):
        """設定を収集"""
        config = {
            "db_path": "./data/db.sqlite3",
            "base_dir": "./",
            "record_worker": {
                "vc_device_index": self.record_device_combo.currentData(),
                "vc_device_name": self.record_device_combo.currentText(),
                "mic_device_index": self.mic_device_combo.currentData(),
                "mic_device_name": self.mic_device_combo.currentText(),
                "monoral_mic": self.monoral_checkbox.isChecked(),
                "channels": 1 if self.monoral_checkbox.isChecked() else 2,
                "rate": 44100,
                "chunk": 1024,
                "record_seconds": self.record_seconds_spin.value()
            },
            "transcribe_worker": {
                "model_size": self.model_size_combo.currentText(),
                "device": self.device_combo.currentText(),
                "compute_type": self.compute_type_combo.currentText(),
                "wait_seconds_if_no_job": self.transcribe_wait_spin.value()
            },
            "metagen_worker": {
                "api_key": self.api_key_edit.text().strip(),
                "model_name": self.model_name_combo.currentText(),
                "wait_seconds_if_no_job": self.meta_wait_spin.value()
            }
        }
        return config
    
    def finish_setup(self):
        """セットアップ完了"""
        if not self.validate_current_step():
            QMessageBox.warning(self, "入力エラー", "必要な設定が完了していません。")
            return
        
        try:
            config = self.collect_config()
            
            # config.jsonを保存
            with open('config.json', 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            
            # dataディレクトリを作成
            os.makedirs('./data', exist_ok=True)
            
            QMessageBox.information(self, "セットアップ完了", 
                                  "設定が正常に保存されました。\nconfig.jsonファイルが作成されました。")
            
            self.close()
            
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"設定の保存中にエラーが発生しました:\n{str(e)}")

    def update_model_list_from_api(self):
        api_key = self.api_key_edit.text().strip()
        if not api_key or genai is None:
            return
        self.model_name_combo.clear()
        try:
            client = genai.Client(api_key=api_key)
            models = [m.name for m in client.models.list() if m.name is not None]
            if not models:
                self.model_name_combo.addItem("利用可能なモデルがありません")
            else:
                self.model_name_combo.addItems(models)
                # 2.5-flashがあれば初期選択
                for i, name in enumerate(models):
                    if "2.5-flash" in name:
                        self.model_name_combo.setCurrentIndex(i)
                        break
        except Exception as e:
            self.model_name_combo.addItem("モデル取得失敗")

def run_setup_wizard(app=None):
    if app is None:
        app = QApplication.instance() or QApplication(sys.argv)
    # 既にconfig.jsonが存在する場合は確認
    if os.path.exists('config.json'):
        reply = QMessageBox.question(None, "確認", 
                                   "config.jsonが既に存在します。\n上書きしますか？",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No:
            return False
    window = SetupWizard()
    window.show()
    app.exec()
    return os.path.exists('config.json')

def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(True)
    
    # 既にconfig.jsonが存在する場合は確認
    if os.path.exists('config.json'):
        reply = QMessageBox.question(None, "確認", 
                                   "config.jsonが既に存在します。\n上書きしますか？",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.No:
            return
    
    window = SetupWizard()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 