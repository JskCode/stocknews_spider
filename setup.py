from setuptools import setup, find_packages
from pathlib import Path

here = Path(__file__).parent
requirements = []
req_file = here / "requirements.txt"
if req_file.exists():
    requirements = [r.strip() for r in req_file.read_text(encoding="utf-8").splitlines() if r.strip() and not r.strip().startswith("#")]

setup(
    name="stocknews_spider",
    version="0.1.0",
    description="示例：抓取新闻并（可选）写入 Kafka 的简易爬虫",
    packages=find_packages(exclude=("tests", "tests.*")),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "stocknews_spider=stocknews_spider.cli:main",
        ]
    },
)
