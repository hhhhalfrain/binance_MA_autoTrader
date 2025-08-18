import os
import json
from dotenv import load_dotenv


class ConfigManager:
    """
    配置与 APIKey 管理类
    功能：
      1) 读取 JSON 配置文件
      2) 优先从 .env 加载 API_KEY / API_SECRET（若 testnet=True，加载 TEST_API_KEY/TEST_API_SECRET）
      3) 提供统一的配置访问接口
    """

    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self._load_env_keys()

    def _load_config(self, path: str) -> dict:
        """读取 JSON 配置文件，如果不存在则抛异常"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"配置文件 {path} 不存在")
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        # 设置默认值
        cfg.setdefault("symbols", [])
        cfg.setdefault("intervals", [])
        cfg.setdefault("max_k", 100)
        cfg.setdefault("testnet", False)
        cfg.setdefault("debug", True)
        return cfg

    def _load_env_keys(self):
        """优先从 .env 加载 APIKey，如果存在则覆盖 config.json"""
        load_dotenv(override=True)
        testnet = bool(self.config.get("testnet", False))

        if testnet:
            api_key = os.getenv("TEST_API_KEY")
            api_secret = os.getenv("TEST_API_SECRET")
        else:
            api_key = os.getenv("API_KEY")
            api_secret = os.getenv("API_SECRET")

        # 如果 env 中存在，覆盖 config 中的
        if api_key and api_secret:
            self.config["API_KEY"] = api_key
            self.config["API_SECRET"] = api_secret

    def get(self, key: str, default=None):
        """获取配置字段"""
        return self.config.get(key, default)

    def get_api_keys(self):
        """返回 (api_key, api_secret)"""
        return self.config.get("API_KEY", ""), self.config.get("API_SECRET", "")

    def all(self) -> dict:
        """返回完整配置 dict"""
        return self.config
