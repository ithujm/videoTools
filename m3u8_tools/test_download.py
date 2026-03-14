import subprocess

def run_cli(cmd):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        bufsize=1
    )

    # 实时打印输出
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        print(line.strip())

    return proc.wait()

# 使用
URL ="https://surrit.com/41e190e3-6f20-4bd7-8bfd-58024aa7b6a9/480p/video.m3u8"
cmd = ["yt-dlp", "--external-downloader", "aria2c", "--external-downloader-args", "-x 4 -s 4", URL]
code = run_cli(cmd)
print("退出码:", code)