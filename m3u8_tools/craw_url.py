import re
import time
import urllib.parse
from playwright.sync_api import sync_playwright


def get_m3u8_stream(url: str, timeout: int = 30) -> str | None:
    m3u8_list = []

    # ===================== 核心规则：优先 video.m3u8，其次最大清晰度 =====================
    def get_link_score(link):
        if "video.m3u8" in link:
            return 9999  # 最高优先级
        match = re.search(r"(\d+)[pP]", link)
        return int(match.group(1)) if match else 0

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
                timeout=timeout * 1000,
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


# ===================== 测试 =====================
if __name__ == "__main__":
    url = input("输入URL：")
    m3u8 = get_m3u8_stream(url)
    print("\n✅ 获取到完整 m3u8：\n", m3u8)