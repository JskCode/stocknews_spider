"""简单的去重存储实现。

当前实现使用文件持久化一个 URL 集合（每行一个 URL）。这是最小可用方案，适合小规模任务。
可替换为 Redis、SQLite 或 BloomFilter 以优化性能与内存占用。
"""
from pathlib import Path
from typing import Set


class Storage:
    def __init__(self, path: str | None = None):
        self.path = Path(path or Path.cwd() / "seen_urls.txt")
        self._seen: Set[str] = set()
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with self.path.open("r", encoding="utf-8") as f:
                    for line in f:
                        url = line.strip()
                        if url:
                            self._seen.add(url)
            except Exception:
                # 忽略加载错误，保持空集合
                pass

    def is_seen(self, url: str) -> bool:
        return url in self._seen

    def add(self, url: str) -> None:
        if url and url not in self._seen:
            self._seen.add(url)

    def save(self) -> None:
        try:
            with self.path.open("w", encoding="utf-8") as f:
                for url in sorted(self._seen):
                    f.write(url + "\n")
        except Exception:
            pass
