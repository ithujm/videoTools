import sys
import os
import re
import time
import urllib.parse
from typing import List, Tuple, Optional, Dict
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import numpy as np

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog, QTextEdit,
    QProgressBar, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QGroupBox, QSpinBox, QCheckBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QObject, QMutex, QMutexLocker
from PyQt5.QtGui import QFont, QColor
import subprocess

import time
import hashlib
import threading
from typing import Optional

import re
import time
import urllib.parse
from typing import Optional
from playwright.sync_api import sync_playwright

# 线程安全计数器+锁（保证多线程下编码唯一）
_code_counter = 0
_counter_lock = threading.Lock()

# 修复：正确定义请求头（3.0.2版本用|分隔多个header）
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
REFERER = "https://missav.ai/"
ORIGIN = "https://missav.ai"

# REFERER = "https://www.xvideos.com/"
# ORIGIN = "https://www.xvideos.com"

REQUEST_HEADERS = f"User-Agent: {USER_AGENT}|Referer: {REFERER}|Origin: {ORIGIN}"
PROXY_ADDR = "http://127.0.0.1:7890"


def extract_video_code(title: str) -> str:
    """
    自动提取标题里的视频编码，适配多线程场景，保证编码唯一且关联原始标题：
    匹配格式：字母/数字+(-/_)+数字，如 OKSN-293、MIDE-999、S1-123、OKAS_025、(ABC12-456)
    返回：统一格式（大写，-换_），如 OKSN_293、S1_123；匹配失败则生成关联标题的唯一编码
    """
    # 步骤1：清洗标题（去空格/特殊字符，统一大写，提升匹配率）
    cleaned_title = re.sub(r'[^\w\-_]', '', title.strip()).upper()

    # 步骤2：优化正则（覆盖更多场景：前缀含数字、编码前后有冗余）
    # 匹配规则：字母/数字+(-/_)+数字，且尽可能匹配完整编码
    pattern = r'[A-Z0-9]+[-_]\d+'
    matches = re.findall(pattern, cleaned_title)

    if matches:
        # 若多个匹配，选最长的（最可能是真实编码）
        best_match = max(matches, key=len)
        # 核心：-换_，避免CLI路径异常，统一大写
        return best_match.replace("-", "_").upper()

    # 步骤3：匹配失败时，生成「关联标题+唯一」的编码（多线程安全）
    # 3.1 生成标题的MD5前缀（8位，关联原始标题）
    title_md5 = hashlib.md5(title.encode("utf-8")).hexdigest()[:8]
    # 3.2 毫秒级时间戳（降低重复概率）
    timestamp = int(time.time() * 1000) % 1000000  # 取后6位，缩短长度
    # 3.3 线程安全计数器（彻底保证唯一）
    global _code_counter
    with _counter_lock:
        _code_counter = (_code_counter + 1) % 1000  # 循环计数器，避免过长
        counter = _code_counter

    # 最终生成：MD5前缀_时间戳_计数器（既关联标题，又保证多线程唯一）
    return f"video_{title_md5}_{timestamp}_{counter:03d}"


# ===================== 全局配置（缓存+导出版） =====================
class Config:
    # 基础配置
    save_dir: str = "./downloads"
    cli_path: str = "N_m3u8DL-CLI.exe"
    capture_timeout: int = 30  # 抓链超时（秒）
    link_cache_path: str = "m3u8_links_cache.csv"  # 链接缓存文件

    # 下载优化配置
    max_download_workers: int = 1  # 下载并发数
    download_timeout: int = 1800  # 单任务下载超时
    cli_threads: int = 1  # CLI线程数
    cli_retry: int = 5  # CLI重试次数
    cli_chunk_size: str = "1M"  # 分片大小
    min_file_size: int = 1024 * 1024  # 最小有效文件大小（1MB）
    download_retry_times: int = 1  # 下载失败重试次数
    concurrent_fragments: int = "32"


# ===================== 信号类（线程安全通信） =====================
class WorkerSignals(QObject):
    log = pyqtSignal(str, str)
    global_progress = pyqtSignal(int)
    task_status = pyqtSignal(str, str)
    capture_done = pyqtSignal(list)
    download_done = pyqtSignal()


# ===================== 彩色日志组件 =====================
class LogWidget(QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setFont(QFont("Consolas", 9))
        self.setStyleSheet("""
            QTextEdit { background-color: #f8f8f8; color: #333; }
        """)

    def log(self, msg: str, level: str = "INFO"):
        color_map = {
            "INFO": "#2196F3",
            "SUCCESS": "#4CAF50",
            "WARNING": "#FF9800",
            "ERROR": "#F44336",
            "STATUS": "#9C27B0",
            "DEBUG": "#607D8B",
            "CACHE": "#FF5722"
        }
        color = color_map.get(level, "#333")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_line = f'<font color="{color}">[{timestamp}] [{level}] {msg}</font><br>'

        self.append(log_line)
        self.ensureCursorVisible()


# ===================== 链接缓存管理（核心新增） =====================
class LinkCacheManager:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self.cache_data: Dict[str, str] = {}  # 标题: 链接
        self.load_cache()

    def load_cache(self):
        """加载本地链接缓存"""
        try:
            if os.path.exists(self.cache_path):
                df = pd.read_csv(self.cache_path, encoding="utf-8-sig").fillna("")
                # 构建标题-链接映射
                for _, row in df.iterrows():
                    title = str(row.get("title", "")).strip()
                    link = str(row.get("link", "")).strip()
                    if title and link:
                        self.cache_data[title] = link
                print(f"加载缓存成功，共 {len(self.cache_data)} 条链接")
        except Exception as e:
            print(f"加载缓存失败：{e}")
            self.cache_data = {}

    def save_cache(self):
        """保存链接缓存到CSV"""
        try:
            # 转换为DataFrame
            df = pd.DataFrame([
                {"title": title, "link": link}
                for title, link in self.cache_data.items()
            ])
            # 保存为UTF8-BOM格式，避免中文乱码
            df.to_csv(self.cache_path, index=False, encoding="utf-8-sig")
            return True
        except Exception as e:
            print(f"保存缓存失败：{e}")
            return False

    def get_link(self, title: str) -> Optional[str]:
        """获取标题对应的链接"""
        return self.cache_data.get(title.strip())

    def add_link(self, title: str, link: str):
        """添加新链接到缓存"""
        title = title.strip()
        link = link.strip()
        if title and link:
            self.cache_data[title] = link
            self.save_cache()

    def batch_add_links(self, links: List[Tuple[str, str]]):
        """批量添加链接"""
        for title, link in links:
            title = title.strip()
            link = link.strip()
            if title and link:
                self.cache_data[title] = link
        self.save_cache()

    def export_links(self, export_path: str):
        """导出链接到指定路径"""
        try:
            df = pd.DataFrame([
                {"title": title, "link": link}
                for title, link in self.cache_data.items()
            ])
            df.to_csv(export_path, index=False, encoding="utf-8-sig")
            return True
        except Exception as e:
            print(f"导出链接失败：{e}")
            return False

    def clear_cache(self):
        """清空缓存"""
        self.cache_data = {}
        if os.path.exists(self.cache_path):
            os.remove(self.cache_path)


# ===================== 单线程抓链（支持缓存跳过） =====================
class CaptureWorker(QThread):
    def __init__(self, tasks: List[Tuple[str, str]], cache_manager: LinkCacheManager):
        super().__init__()
        self.tasks = tasks
        self.cache_manager = cache_manager
        self.signals = WorkerSignals()
        self.is_running = True

    def run(self):
        total = len(self.tasks)
        if total == 0:
            self.signals.log.emit("无有效任务，抓链终止", "WARNING")
            self.signals.capture_done.emit([])
            return

        completed = 0
        m3u8_results = []
        cached_count = 0

        self.signals.log.emit(f"开始单线程抓链 | 总任务数：{total}", "INFO")

        # 先检查缓存，分离已有链接和需要抓链的任务
        need_capture = []
        for url, title in self.tasks:
            # 从缓存获取链接
            cached_link = self.cache_manager.get_link(title)
            if cached_link:
                m3u8_results.append((title, cached_link))
                self.signals.log.emit(f"缓存命中，跳过抓链：{title}", "CACHE")
                self.signals.task_status.emit(title, "缓存命中")
                cached_count += 1
                completed += 1
                self.signals.global_progress.emit(int(completed / total * 100))
            else:
                need_capture.append((url, title))

        self.signals.log.emit(f"缓存命中 {cached_count} 个任务，需抓链 {len(need_capture)} 个任务", "INFO")

        # 只抓没有缓存的任务
        for url, title in need_capture:
            if not self.is_running:
                break

            completed += 1
            progress = int(completed / total * 100)

            self.signals.task_status.emit(title, "抓链中")
            self.signals.log.emit(f"开始抓链 [{completed}/{total}]：{title}", "STATUS")

            m3u8_link = self.capture_single_task(url, title)

            if m3u8_link:
                m3u8_results.append((title, m3u8_link))
                # 添加到缓存
                self.cache_manager.add_link(title, m3u8_link)
                self.signals.log.emit(f"抓链成功 [{completed}/{total}]：{title}", "SUCCESS")
                self.signals.task_status.emit(title, "抓链完成")
            else:
                self.signals.log.emit(f"抓链失败 [{completed}/{total}]：{title}", "WARNING")
                self.signals.task_status.emit(title, "抓链失败")

            self.signals.global_progress.emit(progress)
            time.sleep(0.3)

        # 批量保存缓存（确保所有新链接都保存）
        self.cache_manager.save_cache()
        self.signals.log.emit(
            f"抓链完成 | 总计：{total} | 缓存命中：{cached_count} | 新抓链：{len(need_capture)} | 成功：{len(m3u8_results)}",
            "INFO")
        self.signals.capture_done.emit(m3u8_results)

    def capture_single_task(self, url: str, title: str) -> Optional[str]:
        # 1. 初始化：收集所有匹配的m3u8链接（替代原有单个m3u8_link）
        m3u8_list = []

        # ===================== 核心规则：优先 video.m3u8，其次最大清晰度 =====================
        def get_link_score(link):
            if "video.m3u8" in link:
                return 9999  # 最高优先级
            match = re.search(r"(\d+)[pP]", link)
            return int(match.group(1)) if match else 0

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=False,
                    args=[
                        "--no-sandbox",
                        "--disable-web-security",
                        "--ignore-certificate-errors",
                        "--disable-features=IsolateOrigins",
                        "--disable-site-isolation-trials"
                    ]
                )

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="zh-CN"
                )

                page = context.new_page()

                # ===================== 关键修复：获取 完整原始 URL（不会截断、不会丢参数） =====================
                def on_response(res):
                    try:
                        if not res.ok:
                            return

                        # 获取原始、完整、带全部参数的真实 URL（不会丢失 token/key/sign）
                        raw_url = res.url
                        decoded_url = urllib.parse.unquote(raw_url)  # 解码确保完整

                        if ".m3u8" in decoded_url:
                            if decoded_url not in m3u8_list:
                                m3u8_list.append(decoded_url)
                    except Exception:
                        pass

                page.on("response", on_response)

                # ===================== 打开页面（不卡死、不断连） =====================
                try:
                    page.goto(
                        url,
                        timeout=10 * 1000,
                        wait_until="domcontentloaded"  # 绝对不用 networkidle
                    )
                except Exception:
                    pass

                time.sleep(1)

                # ===================== 真人模拟行为（确保视频加载完整） =====================
                try:
                    # 滑动到视频区域
                    page.evaluate("window.scrollBy(0, 600)")
                    time.sleep(1)

                    # 点击视频触发播放（必须做，否则不加载完整 m3u8）
                    page.locator("video").click(force=True)
                    time.sleep(1)

                    # 等待视频流完整输出
                    time.sleep(4)

                except Exception:
                    time.sleep(3)

                # 关闭
                page.close()
                context.close()
                browser.close()

            if not m3u8_list:
                return None

            # 返回最优链接：优先 video.m3u8，否则清晰度最大
            best_link = max(m3u8_list, key=get_link_score)
            return best_link

        except Exception as e:
            self.signals.log.emit(f"抓链异常：{title} | {str(e)}", "ERROR")
            return None

    def stop(self):
        self.is_running = False
        self.signals.log.emit("正在停止抓链任务...", "WARNING")


# ===================== 多线程并发下载（保留优化版） =====================
class DownloadWorker(QThread):
    def __init__(self, m3u8_list: List[Tuple[str, str]], config: Config):
        super().__init__()
        self.m3u8_list = m3u8_list
        self.config = config
        self.signals = WorkerSignals()
        self.is_running = True
        self.mutex = QMutex()
        self.downloaded_count = 0
        self.total_count = len(m3u8_list)

    def run(self):
        if self.total_count == 0:
            self.signals.log.emit("无有效m3u8链接，下载终止", "WARNING")
            self.signals.download_done.emit()
            return

        self.signals.log.emit(
            f"开始多线程并发下载 | 任务数：{self.total_count} | 并发数：{self.config.max_download_workers}", "INFO")

        with ThreadPoolExecutor(max_workers=self.config.max_download_workers) as executor:
            future_to_task = {}
            for title, m3u8_url in self.m3u8_list:
                if not self.is_running:
                    break
                future = executor.submit(
                    self.download_single_task_with_retry,
                    title, m3u8_url
                )
                future_to_task[future] = title

            for future in as_completed(future_to_task):
                if not self.is_running:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                title = future_to_task[future]
                try:
                    success = future.result(timeout=self.config.download_timeout)
                    with QMutexLocker(self.mutex):
                        self.downloaded_count += 1
                        progress = int(self.downloaded_count / self.total_count * 100)

                    if success:
                        self.signals.log.emit(f"下载完成 [{self.downloaded_count}/{self.total_count}]：{title}",
                                              "SUCCESS")
                        self.signals.task_status.emit(title, "下载完成")
                    else:
                        self.signals.log.emit(f"最终下载失败 [{self.downloaded_count}/{self.total_count}]：{title}",
                                              "ERROR")
                        self.signals.task_status.emit(title, "下载失败")

                    self.signals.global_progress.emit(progress)
                except TimeoutError:
                    with QMutexLocker(self.mutex):
                        self.downloaded_count += 1
                        progress = int(self.downloaded_count / self.total_count * 100)
                    self.signals.log.emit(f"下载超时 [{self.downloaded_count}/{self.total_count}]：{title}", "ERROR")
                    self.signals.task_status.emit(title, "下载超时")
                    self.signals.global_progress.emit(progress)
                except Exception as e:
                    with QMutexLocker(self.mutex):
                        self.downloaded_count += 1
                        progress = int(self.downloaded_count / self.total_count * 100)
                    self.signals.log.emit(f"下载异常 [{self.downloaded_count}/{self.total_count}]：{title} | {str(e)}",
                                          "ERROR")
                    self.signals.task_status.emit(title, "下载异常")
                    self.signals.global_progress.emit(progress)

        self.signals.log.emit(f"\n下载任务全部完成 | 总计：{self.total_count} | 完成：{self.downloaded_count}", "SUCCESS")
        self.signals.global_progress.emit(100)
        self.signals.download_done.emit()

    def download_single_task_with_retry(self, title: str, m3u8_url: str) -> bool:
        """带重试的下载任务"""
        for retry in range(self.config.download_retry_times + 1):
            if not self.is_running:
                return False

            self.signals.log.emit(f"下载尝试 {retry + 1}/{self.config.download_retry_times + 1}：{title}", "STATUS")
            # 新增：接收是否是"跳过"的标记
            success, file_path, is_skip = self.download_single_task(title, m3u8_url)

            # 核心修改：优先处理跳过逻辑
            if is_skip:
                self.signals.log.emit(f"✅ 确认文件已存在且有效，无需下载：{title}", "INFO")
                return True

            if success and os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                if file_size >= self.config.min_file_size:
                    return True
                else:
                    self.signals.log.emit(f"文件过小（{file_size / 1024:.2f}KB），重试下载：{title}", "WARNING")
                    with suppress(Exception):
                        os.remove(file_path)
            elif not success and retry < self.config.download_retry_times:
                self.signals.log.emit(f"下载失败，{2}秒后重试：{title}", "WARNING")
                time.sleep(2)

        return False

    def download_single_task(self, title: str, m3u8_url: str) -> Tuple[bool, str]:
        """单个下载任务"""
        self.signals.task_status.emit(title, "下载中")

        # 生成超安全文件名
        video_code = extract_video_code(title)
        print(f"====={title}=={video_code}")

        # 🔥 核心：自动提取视频编码做文件名
        save_path = os.path.abspath(os.path.join(self.config.save_dir, f"{video_code}.mp4"))

        # 快速跳过已下载的有效文件

        # 跳过逻辑
        # 🔥 核心：自动提取视频编码做文件名

        is_skip = False
        # 跳过逻辑：检查文件是否真的存在且大小达标
        mp4_all_lst = [i for i in os.listdir(self.config.save_dir) if i.endswith(".mp4")]
        for file_name in mp4_all_lst:
            if extract_video_code(file_name) == video_code:
                real_file_path = os.path.abspath(os.path.join(self.config.save_dir, file_name))
                # 校验文件大小
                if os.path.exists(real_file_path) and os.path.getsize(real_file_path) >= self.config.min_file_size:
                    self.signals.log.emit(f"❌ 存在相同的视频{real_file_path}，跳过", "INFO")
                    return True, real_file_path, True  # is_skip=True
                else:
                    # 文件存在但无效，删除后继续下载
                    with suppress(Exception):
                        os.remove(real_file_path)
                    break

        # yt-dlp工具
        ffmpeg_path = r"C:\hjm\installApps\ffmpeg\bin\ffmpeg.exe"

        part_file = f"{save_path}.part"

        # 2. 清理残留.part文件
        if os.path.exists(part_file):
            os.remove(part_file)
            print(f"✅ 清理残留文件：{part_file}")

        cmd = [
            "yt-dlp",
            "--ffmpeg-location", ffmpeg_path,
            "-o", save_path,
            "--user-agent", USER_AGENT,
            "--add-header", f"Referer:{REFERER}",
            "--hls-prefer-ffmpeg",
            "--hls-use-mpegts",
            "--concurrent-fragments", self.config.concurrent_fragments,
            "--no-check-certificate",
            "--retries", "5",
            "--fragment-retries", "1",
            "--socket-timeout", "300",
            "--continue",
            "--no-part",
            "--no-overwrites",
            "--ignore-errors",
            m3u8_url
        ]

        try:
            self.signals.log.emit(f"执行CLI命令：{' '.join(cmd)}", "DEBUG")
            print(f"执行CLI命令：{' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=self.config.download_timeout,
                shell=False,
                creationflags=0x08000000 | 0x00000008,
                encoding="utf-8",
                errors="ignore"
            )

            if result.stdout:
                self.signals.log.emit(f"CLI输出：{result.stdout.strip()[:300]}", "DEBUG")
            if result.stderr:
                self.signals.log.emit(f"CLI错误：{result.stderr.strip()[:300]}", "WARNING")

            if os.path.exists(save_path):
                self.signals.log.emit(f"CLI执行完成，文件已生成：{title}", "INFO")
                return True, save_path, is_skip
            else:
                self.signals.log.emit(f"CLI执行完成但文件未生成：{title}", "ERROR")
                return False, save_path, is_skip

        except subprocess.TimeoutExpired:
            self.signals.log.emit(f"下载超时：{title}", "ERROR")
            return False, save_path, is_skip
        except Exception as e:
            self.signals.log.emit(f"下载异常：{title} | {str(e)}", "ERROR")
            return False, save_path, is_skip

    def stop(self):
        self.is_running = False
        self.signals.log.emit("正在停止下载任务...", "WARNING")


# ===================== 主GUI界面（新增缓存/导出功能） =====================
class M3U8Downloader(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("M3U8下载工具（缓存+导出版）- 无需重复抓链")
        self.setGeometry(100, 100, 1300, 850)

        # 初始化配置和缓存管理器
        self.config = Config()
        self.cache_manager = LinkCacheManager(self.config.link_cache_path)

        self.tasks = []
        self.m3u8_list = []
        self.capture_worker = None
        self.download_worker = None

        self.init_ui()
        self.check_playwright()

        # 日志提示缓存加载状态
        self.log_widget.log(f"链接缓存加载完成，共 {len(self.cache_manager.cache_data)} 条缓存链接", "CACHE")

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # 1. 缓存/导出功能区（核心新增）
        cache_group = QGroupBox("链接缓存与导出")
        cache_layout = QHBoxLayout(cache_group)

        self.export_btn = QPushButton("导出抓到的链接")
        self.export_btn.clicked.connect(self.export_links)
        cache_layout.addWidget(self.export_btn)

        self.clear_cache_btn = QPushButton("清空缓存")
        self.clear_cache_btn.clicked.connect(self.clear_cache)
        cache_layout.addWidget(self.clear_cache_btn)

        self.refresh_cache_btn = QPushButton("刷新缓存")
        self.refresh_cache_btn.clicked.connect(self.refresh_cache)
        cache_layout.addWidget(self.refresh_cache_btn)

        self.cache_checkbox = QCheckBox("启用链接缓存（默认开启）")
        self.cache_checkbox.setChecked(True)
        cache_layout.addWidget(self.cache_checkbox)

        layout.addWidget(cache_group)

        # 2. 高级配置区域
        advanced_group = QGroupBox("高级下载配置（提速关键）")
        advanced_layout = QHBoxLayout(advanced_group)

        advanced_layout.addWidget(QLabel("下载并发数："))
        self.worker_spin = QSpinBox()
        self.worker_spin.setRange(1, 8)
        self.worker_spin.setValue(self.config.max_download_workers)
        self.worker_spin.valueChanged.connect(lambda v: setattr(self.config, 'max_download_workers', v))
        advanced_layout.addWidget(self.worker_spin)

        advanced_layout.addWidget(QLabel("CLI线程数："))
        self.thread_spin = QSpinBox()
        self.thread_spin.setRange(1, 32)
        self.thread_spin.setValue(self.config.cli_threads)
        self.thread_spin.valueChanged.connect(lambda v: setattr(self.config, 'cli_threads', v))
        advanced_layout.addWidget(self.thread_spin)

        advanced_layout.addWidget(QLabel("重试次数："))
        self.retry_spin = QSpinBox()
        self.retry_spin.setRange(0, 5)
        self.retry_spin.setValue(self.config.download_retry_times)
        self.retry_spin.valueChanged.connect(lambda v: setattr(self.config, 'download_retry_times', v))
        advanced_layout.addWidget(self.retry_spin)

        layout.addWidget(advanced_group)

        # 3. 基础配置区域
        config_group = QGroupBox("基础配置")
        config_layout = QHBoxLayout(config_group)

        config_layout.addWidget(QLabel("保存目录："))
        self.save_dir_edit = QLineEdit(self.config.save_dir)
        config_layout.addWidget(self.save_dir_edit)
        self.save_dir_btn = QPushButton("选择")
        self.save_dir_btn.clicked.connect(self.choose_save_dir)
        config_layout.addWidget(self.save_dir_btn)

        config_layout.addWidget(QLabel("CLI路径："))
        self.cli_edit = QLineEdit(self.config.cli_path)
        config_layout.addWidget(self.cli_edit)
        self.cli_btn = QPushButton("选择")
        self.cli_btn.clicked.connect(self.choose_cli_path)
        config_layout.addWidget(self.cli_btn)

        layout.addWidget(config_group)

        # 4. 任务区域
        task_group = QGroupBox("任务管理（自动过滤空值）")
        task_layout = QVBoxLayout(task_group)

        self.import_btn = QPushButton("导入任务（Excel/CSV）")
        self.import_btn.clicked.connect(self.import_tasks)
        task_layout.addWidget(self.import_btn)

        self.task_table = QTableWidget()
        self.task_table.setColumnCount(3)
        self.task_table.setHorizontalHeaderLabels(["视频URL", "视频标题", "任务状态"])
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        task_layout.addWidget(self.task_table)

        layout.addWidget(task_group)

        # 5. 控制区域
        ctrl_group = QGroupBox("任务控制")
        ctrl_layout = QHBoxLayout(ctrl_group)

        self.capture_btn = QPushButton("智能抓链（缓存优先）")
        self.capture_btn.clicked.connect(self.start_capture)
        self.download_btn = QPushButton("多线程并发下载")
        self.download_btn.clicked.connect(self.start_download)
        self.download_btn.setEnabled(False)
        self.stop_btn = QPushButton("停止任务")
        self.stop_btn.clicked.connect(self.stop_all_tasks)
        self.stop_btn.setEnabled(False)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        ctrl_layout.addWidget(self.capture_btn)
        ctrl_layout.addWidget(self.download_btn)
        ctrl_layout.addWidget(self.stop_btn)
        ctrl_layout.addWidget(QLabel("全局进度："))
        ctrl_layout.addWidget(self.progress_bar)

        layout.addWidget(ctrl_group)

        # 6. 日志区域
        log_group = QGroupBox("运行日志（含缓存信息）")
        log_layout = QVBoxLayout(log_group)

        self.log_widget = LogWidget()
        log_layout.addWidget(self.log_widget)

        layout.addWidget(log_group)

    # ===================== 新增功能函数 =====================
    def export_links(self):
        """导出抓到的链接"""
        if not self.cache_manager.cache_data:
            QMessageBox.warning(self, "提示", "暂无可导出的链接！")
            return

        # 选择导出路径
        export_path, _ = QFileDialog.getSaveFileName(
            self, "导出链接", "./m3u8_links_export.csv",
            "CSV文件 (*.csv);;所有文件 (*.*)"
        )
        if export_path:
            success = self.cache_manager.export_links(export_path)
            if success:
                QMessageBox.information(self, "成功", f"链接导出成功！\n路径：{export_path}")
                self.log_widget.log(f"链接导出成功，路径：{export_path}", "SUCCESS")
            else:
                QMessageBox.critical(self, "失败", "链接导出失败！")

    def clear_cache(self):
        """清空缓存"""
        reply = QMessageBox.question(
            self, "确认", "确定要清空所有链接缓存吗？",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.cache_manager.clear_cache()
            self.log_widget.log("链接缓存已清空", "WARNING")
            QMessageBox.information(self, "成功", "缓存已清空！")

    def refresh_cache(self):
        """刷新缓存"""
        self.cache_manager.load_cache()
        self.log_widget.log(f"缓存刷新完成，当前缓存数：{len(self.cache_manager.cache_data)}", "CACHE")
        QMessageBox.information(self, "成功", f"缓存刷新完成！当前缓存 {len(self.cache_manager.cache_data)} 条链接")

    # ===================== 基础函数（保留+优化） =====================
    def check_playwright(self):
        try:
            import playwright
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                browser.close()
        except ImportError:
            QMessageBox.critical(self, "依赖缺失",
                                 "请先安装依赖：\n"
                                 "pip install PyQt5 pandas playwright openpyxl\n"
                                 "playwright install chromium")
            sys.exit(1)
        except Exception as e:
            QMessageBox.warning(self, "Playwright警告",
                                f"驱动检查警告：{str(e)}\n建议重新安装：playwright install chromium")

    def choose_save_dir(self):
        dir_path = QFileDialog.getExistingDirectory(self, "选择保存目录", self.save_dir_edit.text())
        if dir_path:
            self.save_dir_edit.setText(dir_path)
            self.config.save_dir = dir_path

    def choose_cli_path(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择N_m3U8DL-CLI", self.cli_edit.text(),
            "可执行文件 (*.exe);;所有文件 (*.*)"
        )
        if file_path:
            self.cli_edit.setText(file_path)
            self.config.cli_path = file_path

    def import_tasks(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "导入任务文件", "",
            "Excel文件 (*.xlsx);;CSV文件 (*.csv);;所有文件 (*.*)"
        )
        if not file_path:
            return

        try:
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path, encoding="utf-8").fillna("")
            else:
                df = pd.read_excel(file_path).fillna("")

            if df.shape[1] < 2:
                QMessageBox.warning(self, "警告", "任务文件必须包含至少两列（URL、标题）")
                return

            valid_tasks = []
            for _, row in df.iterrows():
                url = str(row.iloc[0]).strip()
                title = str(row.iloc[1]).strip()
                if url and title and url != "nan" and title != "nan":
                    valid_tasks.append((url, title))

            total_read = len(df)
            total_valid = len(valid_tasks)
            total_invalid = total_read - total_valid

            self.tasks = valid_tasks

            self.task_table.setRowCount(total_valid)
            for i, (url, title) in enumerate(valid_tasks):
                self.task_table.setItem(i, 0, QTableWidgetItem(url))
                self.task_table.setItem(i, 1, QTableWidgetItem(title))
                # 检查缓存状态
                if self.cache_manager.get_link(title):
                    self.task_table.setItem(i, 2, QTableWidgetItem("缓存已存在"))
                    self.task_table.item(i, 2).setForeground(QColor("#FF5722"))
                else:
                    self.task_table.setItem(i, 2, QTableWidgetItem("未开始"))

            self.log_widget.log(
                f"任务导入完成 | 读取总数：{total_read} | 有效任务：{total_valid} | 过滤无效：{total_invalid}", "INFO")
            if total_invalid > 0:
                self.log_widget.log(f"已过滤 {total_invalid} 个无效任务（空值/nan）", "WARNING")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"导入失败：{str(e)}")
            self.log_widget.log(f"任务导入失败：{str(e)}", "ERROR")

    def start_capture(self):
        if not self.tasks:
            QMessageBox.warning(self, "警告", "无有效任务，请先导入并确保任务非空")
            return

        self.progress_bar.setValue(0)

        # 启用缓存时，显示缓存状态；禁用时重置状态
        if self.cache_checkbox.isChecked():
            for i in range(self.task_table.rowCount()):
                title_item = self.task_table.item(i, 1)
                if title_item:
                    title = title_item.text()
                    if self.cache_manager.get_link(title):
                        self.task_table.setItem(i, 2, QTableWidgetItem("缓存已存在"))
                        self.task_table.item(i, 2).setForeground(QColor("#FF5722"))
                    else:
                        self.task_table.setItem(i, 2, QTableWidgetItem("未开始"))
        else:
            for i in range(self.task_table.rowCount()):
                self.task_table.setItem(i, 2, QTableWidgetItem("未开始"))

        self.capture_btn.setEnabled(False)
        self.download_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        # 启动抓链（传入缓存管理器）
        self.capture_worker = CaptureWorker(self.tasks,
                                            self.cache_manager if self.cache_checkbox.isChecked() else LinkCacheManager(
                                                ""))
        self.capture_worker.signals.log.connect(self.log_widget.log)
        self.capture_worker.signals.global_progress.connect(self.progress_bar.setValue)
        self.capture_worker.signals.task_status.connect(self.update_task_status)
        self.capture_worker.signals.capture_done.connect(self.on_capture_done)
        self.capture_worker.finished.connect(self.on_capture_finished)
        self.capture_worker.start()

    def on_capture_done(self, m3u8_list):
        self.m3u8_list = m3u8_list
        self.download_btn.setEnabled(True if m3u8_list else False)

    def on_capture_finished(self):
        self.capture_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

    def start_download(self):
        if not self.m3u8_list:
            QMessageBox.warning(self, "警告", "请先完成抓链")
            return

        if not os.path.exists(self.config.cli_path):
            QMessageBox.critical(self, "错误", f"CLI工具不存在：{self.config.cli_path}")
            return

        os.makedirs(self.config.save_dir, exist_ok=True)

        self.progress_bar.setValue(0)

        self.capture_btn.setEnabled(False)
        self.download_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        self.download_worker = DownloadWorker(self.m3u8_list, self.config)
        self.download_worker.signals.log.connect(self.log_widget.log)
        self.download_worker.signals.global_progress.connect(self.progress_bar.setValue)
        self.download_worker.signals.task_status.connect(self.update_task_status)
        self.download_worker.signals.download_done.connect(self.on_download_finished)
        self.download_worker.start()

    def on_download_finished(self):
        self.capture_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.progress_bar.setValue(100)

    def stop_all_tasks(self):
        if self.capture_worker and self.capture_worker.isRunning():
            self.capture_worker.stop()
        if self.download_worker and self.download_worker.isRunning():
            self.download_worker.stop()

        self.log_widget.log("任务已停止", "WARNING")
        self.capture_btn.setEnabled(True)
        self.download_btn.setEnabled(True if self.m3u8_list else False)
        self.stop_btn.setEnabled(False)

    def update_task_status(self, title: str, status: str):
        for i in range(self.task_table.rowCount()):
            item = self.task_table.item(i, 1)
            if item and item.text() == title:
                self.task_table.setItem(i, 2, QTableWidgetItem(status))
                color_map = {
                    "抓链中": QColor("#2196F3"),
                    "抓链完成": QColor("#4CAF50"),
                    "抓链失败": QColor("#F44336"),
                    "缓存命中": QColor("#FF5722"),
                    "缓存已存在": QColor("#FF5722"),
                    "下载中": QColor("#9C27B0"),
                    "下载完成": QColor("#4CAF50"),
                    "下载失败": QColor("#F44336"),
                    "下载超时": QColor("#FF9800"),
                    "下载异常": QColor("#F44336"),
                    "未开始": QColor("#795548")
                }
                if status in color_map:
                    self.task_table.item(i, 2).setForeground(color_map[status])
                break


# ===================== 程序入口 =====================
if __name__ == "__main__":
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

    app = QApplication(sys.argv)
    window = M3U8Downloader()
    window.show()
    sys.exit(app.exec_())
