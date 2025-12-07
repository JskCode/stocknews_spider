"""配置加载器：从 YAML 加载 Kafka 等运行时配置。

提供两个函数：`load_config(path)` 和 `load_default_config()`。
默认的本地配置位于项目根的 `config/local_config.yaml`。
"""
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
except Exception:  # pragma: no cover - 如果未安装 PyYAML，会在导入时失败
    yaml = None  # type: ignore


def _default_config_path() -> Path:
    # 相对于包目录的项目根 config/local_config.yaml
    pkg_root = Path(__file__).parent.parent
    return pkg_root / "config" / "local_config.yaml"


def load_config(path: Optional[str]) -> Dict[str, Any]:
    """从给定路径加载 YAML 配置，返回 dict。"""
    p = Path(path) if path else _default_config_path()
    if not p.exists():
        return {}
    if yaml is None:
        raise RuntimeError("PyYAML 未安装，请在 requirements 中添加 PyYAML")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def load_default_config() -> Dict[str, Any]:
    return load_config(str(_default_config_path()))
