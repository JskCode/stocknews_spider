"""可选的 Kafka 写入器（如果本地没有 Kafka，可跳过或使用模拟）。

此实现优先使用 `kafka-python`，不存在时回退为打印到 stdout 的模拟器。
"""
from typing import Dict, Optional

try:
    from kafka import KafkaProducer
    _HAS_KAFKA = True
except Exception:
    KafkaProducer = None  # type: ignore
    _HAS_KAFKA = False


class KafkaWriter:
    def __init__(self, brokers: Optional[list[str]] = None, topic: str = "news"):
        self.topic = topic
        self.brokers = brokers or ["localhost:9092"]
        if _HAS_KAFKA:
            self.producer = KafkaProducer(bootstrap_servers=self.brokers)
        else:
            self.producer = None

    def send(self, article: Dict) -> None:
        """发送文章到 Kafka 或在本地打印（模拟）。"""
        if self.producer:
            self.producer.send(self.topic, value=str(article).encode("utf-8"))
        else:
            print(f"[KafkaWriter mock] topic={self.topic} article={article}")
