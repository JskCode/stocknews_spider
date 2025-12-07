"""命令行入口：解析最小参数并调用 `runner.run`。"""
"""命令行入口：解析参数并调用 `runner.run`。

支持两种运行方式：
- 推荐：`python -m stocknews_spider.cli ...`（作为包运行）
- 直接运行脚本：`python stocknews_spider/cli.py ...`（会在运行时自动调整 `sys.path`）
"""

import argparse
import sys
from pathlib import Path

# 当直接运行脚本（非包方式），修正 sys.path 以便可以使用包的绝对导入
if __package__ is None or __package__ == "":
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))

from stocknews_spider.spider import crawl
from stocknews_spider.config import load_config, load_default_config


def main() -> None:
    parser = argparse.ArgumentParser(prog="stocknews_spider", description="抓取新闻并可写入 Kafka 的示例项目")
    parser.add_argument("--config", help="YAML 配置文件路径（优先于本地默认配置）")
    parser.add_argument("--once", action="store_true", help="仅运行一次，忽略 YAML 中的周期配置")
    args = parser.parse_args()

    cfg = None
    try:
        if args.config:
            cfg = load_config(args.config)
        else:
            cfg = load_default_config()
    except Exception as e:
        print(f"加载配置出错: {e}")
        cfg = {}

    if not cfg.get("news_url"):
        args.url = ["https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"]

    crawl(args.url, config=cfg, once=args.once)


if __name__ == "__main__":
    main()
