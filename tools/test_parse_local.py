"""本地解析测试工具

用法:
  python tools/test_parse_local.py file:///C:/path/to/file.html
  python tools/test_parse_local.py https://news.example.com/detail/123

该脚本会调用包内的 `Spider.parse_from_html`（基于 BeautifulSoup），并打印 JSON 结果。
"""
import sys
import json
from pathlib import Path

from stocknews_spider.spider import Spider


def load_source(arg: str) -> (str, str):
    """如果是本地 file://... 路径，返回 (content, url)。否则用 requests 获取页面并返回 (text, url)。"""
    if arg.startswith("file://"):
        p = Path(arg[len("file://"):])
        text = p.read_text(encoding="utf-8")
        return text, str(p.resolve())
    else:
        import requests

        resp = requests.get(arg, timeout=15)
        resp.raise_for_status()
        return resp.text, arg


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/test_parse_local.py <file://... or http(s)://...>")
        sys.exit(1)

    src = sys.argv[1]
    html, url = load_source(src)
    spider = Spider([])
    result = spider.parse_from_html(html, url=url)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
