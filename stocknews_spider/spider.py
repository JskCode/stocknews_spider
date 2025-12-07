import random
import requests
import json
import re
import logging
import time
import redis
from datetime import datetime
from typing import List, Dict, Optional

try:
    from stocknews_spider.kafka_writer import KafkaWriter
except ImportError:
    # 如果作为脚本直接运行找不到父包，定义一个 Mock 类以便测试
    class KafkaWriter:
        def __init__(self, config): pass
        def send_message(self, msg): print(f"  [Mock Kafka] 写入: {msg.get('title')}")

class EastMoneySpider:
    def __init__(self, config):
        """
        初始化爬虫，建立 Redis 连接
        :param config: 配置对象或字典
        """
        self.config = config
        self.logger = logging.getLogger("stocknews.spider")
        
        # 默认 API 地址（如果 CLI 没传 urls 则使用此默认值）
        self.default_url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"

        # 兼容字典访问 (cfg['key']) 和 属性访问 (cfg.key)
        redis_host = self._get_cfg('redis_host', 'localhost')
        redis_port = self._get_cfg('redis_port', 6379)
        redis_pass = self._get_cfg('redis_password', None)

        # 初始化 Redis
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=int(redis_port),
                password=redis_pass,
                decode_responses=True,
                socket_timeout=5
            )
            # 测试连接
            self.redis_client.ping()
        except Exception as e:
            self.logger.error(f"Redis 连接失败: {e}")
            raise e

    def _get_cfg(self, key, default=None):
        """辅助方法：兼容对象属性和字典键值获取配置"""
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        return getattr(self.config, key, default)

    def parse_response(self, raw_text: str) -> List[Dict]:
        """
        解析 API 返回的原始文本
        修正点：使用 'LivesList' 字段
        """
        try:
            # 使用正则去除 var ajaxResult= 和末尾的分号，比 replace 更稳健
            json_str = re.search(r'var ajaxResult=(.*?);?$', raw_text, re.DOTALL | re.MULTILINE)
            
            if not json_str:
                self.logger.warning("未匹配到 JSON 数据格式")
                return []
                
            data = json.loads(json_str.group(1))
            
            # === 关键修正：使用你发现的 LivesList 字段 ===
            news_items = data.get('LivesList', [])
            
            if not news_items:
                self.logger.debug("API 返回的新闻列表为空")
                return []
                
            return news_items

        except json.JSONDecodeError:
            self.logger.error("JSON 解析异常")
            return []
        except Exception as e:
            self.logger.error(f"解析过程发生错误: {e}")
            return []

    def fetch_new_items(self, target_url: str) -> List[Dict]:
        """
        抓取并返回经过 Redis 去重后的新数据
        """
        # 添加随机时间戳防止缓存
        timestamp = int(datetime.now().timestamp() * 1000)
        # 判断 URL 是否已有参数
        join_char = '&' if '?' in target_url else '?'
        final_url = f"{target_url}{join_char}t={timestamp}"

        try:
            resp = requests.get(final_url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://kuaixun.eastmoney.com/"
            }, timeout=10)
            
            if resp.status_code != 200:
                self.logger.error(f"请求失败 HTTP {resp.status_code}")
                return []
            
            raw_items = self.parse_response(resp.text)
            
            clean_items = []
            for item in raw_items:
                # 提取关键字段
                news_id = str(item.get('id'))
                
                # === Redis 去重 ===
                # 如果 ID 已存在，跳过
                if self.redis_client.sismember("spider:eastmoney:seen_ids", news_id):
                    continue
                
                # 构造清洗后的数据对象
                clean_item = {
                    "id": news_id,
                    "title": item.get('title'),
                    "content": item.get('digest'),
                    "pub_time": item.get('showtime'),
                    "url": item.get('url_w'),
                    "timestamp": item.get('sort'), # 原始排序时间戳
                    "crawled_at": datetime.now().isoformat()
                }
                
                clean_items.append(clean_item)
                
                # 存入 Redis (标记为已抓取)
                self.redis_client.sadd("spider:eastmoney:seen_ids", news_id)
                # 可选：设置过期时间，防止 key 无限膨胀 (例如保留 7 天)
                # self.redis_client.expire("spider:eastmoney:seen_ids", 60*60*24*7)

            return clean_items

        except Exception as e:
            self.logger.error(f"抓取流程异常: {e}")
            return []

# === 模块入口函数，供 CLI 调用 ===
def crawl(urls: List[str], config, once: bool = False):
    """
    爬虫主逻辑
    :param urls: 目标 URL 列表（通常包含 API 地址）
    :param config: 配置对象
    :param once: 是否只运行一次（True=单次执行，False=死循环定时执行）
    """
    # 1. 初始化日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("stocknews.scheduler")
    
    def get_cfg(key, default):
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)

    # 2. 初始化组件
    try:
        spider = EastMoneySpider(config)
        kafka_writer = KafkaWriter(config)
    except Exception as e:
        logger.critical(f"组件初始化失败，程序退出: {e}")
        return

    # 确定抓取目标：优先使用传入的 urls，否则使用默认 API
    target_url = urls[0] if urls and len(urls) > 0 else spider.default_url
    logger.info(f"启动爬虫，目标: {target_url}，模式: {'单次' if once else '循环'}")

    min_delay = get_cfg('min_delay', 1)
    max_delay = get_cfg('max_delay', 3)

    # 3. 抓取循环
    while True:
        try:
            logger.info("开始一轮抓取...")
            new_items = spider.fetch_new_items(target_url)
            
            if new_items:
                logger.info(f"发现 {len(new_items)} 条新消息，准备写入 Kafka")
                # 排序：按时间从旧到新写入，保证 Kafka 里的顺序正确
                # 'sort' 字段通常是时间戳，越小越旧
                new_items.sort(key=lambda x: str(x.get('timestamp', '0')))
                
                for item in new_items:
                    kafka_writer.send_message(item)
            else:
                logger.info("暂无新消息")

        except Exception as e:
            logger.error(f"主循环发生未捕获异常: {e}")

        # 如果是单次模式，跑完退出
        if once:
            logger.info("单次任务完成，退出。")
            break

        sleep_time = getattr(config, 'sleep_interval', 30) + random.uniform(min_delay, max_delay)
        if isinstance(config, dict):
            sleep_time = config.get('sleep_interval', 30) + random.uniform(min_delay, max_delay)
            
        logger.info(f"休眠 {sleep_time} 秒...")
        time.sleep(sleep_time)

# === 本地测试入口 ===
if __name__ == "__main__":
    # 模拟一个配置对象（模拟 CLI 传入的 cfg）
    class MockConfig:
        redis_host = '192.168.0.135'
        redis_port = 6379
        redis_password = None
        kafka_bootstrap_servers = '192.168.0.135:9092'
        kafka_topic = 'news'
        sleep_interval = 5

    cfg = MockConfig()
    
    print(">>> 开始本地测试 <<<")
    test_urls = ["https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"]
    
    crawl(test_urls, config=cfg, once=True)