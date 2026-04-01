import json
import random
import re
import sqlite3
import logging
import time
import signal
import sys
import hashlib
import urllib.robotparser
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, asdict, field
from urllib.parse import urljoin, urlparse

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import ValidationError as PydanticValidationError

try:
    from fake_useragent import UserAgent
    UA_AVAILABLE = True
except ImportError:
    UA_AVAILABLE = False

FRAGRANTICA_BASE = "https://www.fragrantica.com"
FRAGRANCENET_BASE = "https://www.fragrancenet.com"

UA = None

PROXIES_FILE = "proxies.txt"
PROXIES: List[str] = []

DELAYS = {'min': 5, 'max': 12}
MAX_RETRIES = 5
RETRY_DELAY = 15

DB_PATH = "scraping_progress.db"
OUTPUT_DIR = "output"
MAX_REVIEWS_PER_PERFUME = 50


class StructuredLogger:
    def __init__(self, name: str, log_file: str = "scraping.log"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            stream_handler = logging.StreamHandler()

            formatter = logging.Formatter("%(message)s")
            file_handler.setFormatter(formatter)
            stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

    def _build_entry(self, level: str, message: str, **kwargs) -> str:
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
        }
        entry.update(kwargs)
        return json.dumps(entry, ensure_ascii=False)

    def info(self, message: str, **kwargs):
        self.logger.info(self._build_entry("INFO", message, **kwargs))

    def warning(self, message: str, **kwargs):
        self.logger.warning(self._build_entry("WARNING", message, **kwargs))

    def error(self, message: str, **kwargs):
        self.logger.error(self._build_entry("ERROR", message, **kwargs))

    def debug(self, message: str, **kwargs):
        self.logger.debug(self._build_entry("DEBUG", message, **kwargs))


logger = StructuredLogger(__name__)


class ReviewSchema(BaseModel):
    username: str = ""
    rating: float = Field(default=0.0, ge=0.0, le=10.0)
    date: str = ""
    title: str = ""
    content: str = ""
    likes: int = Field(default=0, ge=0)

    @field_validator("rating", mode="before")
    @classmethod
    def clamp_rating(cls, v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(10.0, v))


class PriceSchema(BaseModel):
    retailer: str = ""
    size: str = ""
    price: float = Field(default=0.0, ge=0.0)
    currency: str = "USD"
    in_stock: bool = True
    url: str = ""

    @field_validator("price", mode="before")
    @classmethod
    def parse_price(cls, v):
        if isinstance(v, str):
            v = re.sub(r"[^\d.]", "", v)
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0


class FragranticaPerfumeSchema(BaseModel):
    id: str = ""
    name: str = ""
    brand: str = ""
    url: str = ""
    perfumer: str = ""
    year: str = ""
    gender: str = ""
    fragrance_type: str = ""
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0)
    votes_count: int = Field(default=0, ge=0)
    notes_top: List[str] = Field(default_factory=list)
    notes_middle: List[str] = Field(default_factory=list)
    notes_base: List[str] = Field(default_factory=list)
    main_accords: Dict[str, float] = Field(default_factory=dict)
    reviews: List[ReviewSchema] = Field(default_factory=list)
    scraped_at: str = Field(default_factory=lambda: datetime.now().isoformat())

    @field_validator("year", mode="before")
    @classmethod
    def validate_year(cls, v):
        if v:
            try:
                year_int = int(str(v).strip())
                if 1800 <= year_int <= datetime.now().year:
                    return str(year_int)
            except (ValueError, TypeError):
                pass
        return ""

    @field_validator("rating", mode="before")
    @classmethod
    def validate_rating(cls, v):
        if v is None:
            return None
        try:
            return max(0.0, min(10.0, float(v)))
        except (TypeError, ValueError):
            return None


class FragranceNetPerfumeSchema(BaseModel):
    id: str = ""
    name: str = ""
    brand: str = ""
    url: str = ""
    gender: str = ""
    size: str = ""
    fragrance_family: str = ""
    prices: List[PriceSchema] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    in_stock: bool = True
    scraped_at: str = Field(default_factory=lambda: datetime.now().isoformat())

    @model_validator(mode="after")
    def check_has_identifier(self):
        if not self.id and not self.name:
            raise ValueError("FragranceNetPerfume must have either id or name")
        return self


def initialize_user_agent():
    global UA
    if UA_AVAILABLE:
        try:
            UA = UserAgent()
            UA.update()
            logger.info("UserAgent initialized with fake-useragent")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize fake-useragent: {e}")

    global USER_AGENTS
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36',
    ]
    logger.info("Using fallback static UserAgents")
    return False


def get_random_user_agent() -> str:
    global UA
    if UA_AVAILABLE and UA:
        try:
            browsers = ['chrome', 'firefox', 'safari', 'edge']
            browser = random.choice(browsers)
            return UA.random if browser == 'chrome' else getattr(UA, browser)
        except Exception:
            pass

    try:
        return random.choice(USER_AGENTS)
    except NameError:
        return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'


def load_proxies(filepath: str = PROXIES_FILE) -> List[str]:
    proxies = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '://' not in line:
                    line = f"http://{line}"
                proxies.append(line)
        logger.info(f"Loaded {len(proxies)} proxies", source=filepath)
    except FileNotFoundError:
        logger.warning(f"Proxies file not found", filepath=filepath)
    except Exception as e:
        logger.error(f"Error loading proxies", filepath=filepath, error=str(e))
    return proxies


@dataclass
class Review:
    username: str = ""
    rating: float = 0.0
    date: str = ""
    title: str = ""
    content: str = ""
    likes: int = 0

    def to_dict(self):
        return asdict(self)

    def validate(self) -> Optional["Review"]:
        try:
            validated = ReviewSchema(
                username=self.username,
                rating=self.rating,
                date=self.date,
                title=self.title,
                content=self.content,
                likes=self.likes,
            )
            self.rating = validated.rating
            self.likes = validated.likes
            return self
        except PydanticValidationError:
            return None


@dataclass
class Price:
    retailer: str = ""
    size: str = ""
    price: float = 0.0
    currency: str = "USD"
    in_stock: bool = True
    url: str = ""

    def to_dict(self):
        return asdict(self)

    def validate(self) -> Optional["Price"]:
        try:
            validated = PriceSchema(**asdict(self))
            self.price = validated.price
            return self
        except PydanticValidationError:
            return None


@dataclass
class FragranticaPerfume:
    id: str = ""
    name: str = ""
    brand: str = ""
    url: str = ""
    perfumer: str = ""
    year: str = ""
    gender: str = ""
    fragrance_type: str = ""
    rating: Optional[float] = None
    votes_count: int = 0
    notes_top: List[str] = field(default_factory=list)
    notes_middle: List[str] = field(default_factory=list)
    notes_base: List[str] = field(default_factory=list)
    main_accords: Dict[str, float] = field(default_factory=dict)
    reviews: List[Review] = field(default_factory=list)
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def generate_fingerprint(self) -> str:
        key = f"{self.brand.lower().strip()}:{self.name.lower().strip()}"
        return hashlib.md5(key.encode()).hexdigest()

    def to_dict(self):
        data = asdict(self)
        data['reviews'] = [r.to_dict() for r in self.reviews]
        return data

    def validate(self) -> Optional["FragranticaPerfume"]:
        try:
            review_schemas = [ReviewSchema(**asdict(r)) for r in self.reviews]
            FragranticaPerfumeSchema(
                id=self.id,
                name=self.name,
                brand=self.brand,
                url=self.url,
                perfumer=self.perfumer,
                year=self.year,
                gender=self.gender,
                fragrance_type=self.fragrance_type,
                rating=self.rating,
                votes_count=self.votes_count,
                notes_top=self.notes_top,
                notes_middle=self.notes_middle,
                notes_base=self.notes_base,
                main_accords=self.main_accords,
                reviews=review_schemas,
            )
            return self
        except PydanticValidationError as e:
            logger.warning("FragranticaPerfume validation failed", url=self.url, error=str(e))
            return None


@dataclass
class FragranceNetPerfume:
    id: str = ""
    name: str = ""
    brand: str = ""
    url: str = ""
    gender: str = ""
    size: str = ""
    fragrance_family: str = ""
    prices: List[Price] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    in_stock: bool = True
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def generate_fingerprint(self) -> str:
        key = f"{self.brand.lower().strip()}:{self.name.lower().strip()}"
        return hashlib.md5(key.encode()).hexdigest()

    def to_dict(self):
        data = asdict(self)
        data['prices'] = [p.to_dict() for p in self.prices]
        return data

    def validate(self) -> Optional["FragranceNetPerfume"]:
        try:
            price_schemas = [PriceSchema(**asdict(p)) for p in self.prices]
            FragranceNetPerfumeSchema(
                id=self.id,
                name=self.name,
                brand=self.brand,
                url=self.url,
                gender=self.gender,
                size=self.size,
                fragrance_family=self.fragrance_family,
                prices=price_schemas,
                notes=self.notes,
                in_stock=self.in_stock,
            )
            return self
        except PydanticValidationError as e:
            logger.warning("FragranceNetPerfume validation failed", url=self.url, error=str(e))
            return None


class RobotsChecker:
    def __init__(self):
        self._cache: Dict[str, urllib.robotparser.RobotFileParser] = {}

    def _get_parser(self, base_url: str) -> Optional[urllib.robotparser.RobotFileParser]:
        if base_url in self._cache:
            return self._cache[base_url]
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"{base_url}/robots.txt")
        try:
            rp.read()
            self._cache[base_url] = rp
            return rp
        except Exception:
            self._cache[base_url] = None
            return None

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._get_parser(base)
        if parser is None:
            return True
        return parser.can_fetch(user_agent, url)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> Optional[float]:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._get_parser(base)
        if parser is None:
            return None
        return parser.crawl_delay(user_agent)


class RateLimiter:
    def __init__(self, min_delay: float = DELAYS['min'], max_delay: float = DELAYS['max']):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last_request: Dict[str, float] = {}
        self._request_counts: Dict[str, int] = {}
        self._domain_stats: Dict[str, Dict[str, Any]] = {}

    def wait_if_needed(self, url: str):
        domain = urlparse(url).netloc
        now = time.time()
        last = self._last_request.get(domain, 0)
        delay = random.uniform(self.min_delay, self.max_delay)
        elapsed = now - last

        if elapsed < delay:
            time.sleep(delay - elapsed)

        self._last_request[domain] = time.time()
        self._request_counts[domain] = self._request_counts.get(domain, 0) + 1

        if domain not in self._domain_stats:
            self._domain_stats[domain] = {
                "total_requests": 0,
                "first_request": datetime.utcnow().isoformat(),
                "last_request": None,
            }
        self._domain_stats[domain]["total_requests"] += 1
        self._domain_stats[domain]["last_request"] = datetime.utcnow().isoformat()

    def get_stats(self) -> Dict[str, Any]:
        return dict(self._domain_stats)


class DuplicateDetector:
    def __init__(self, db: "ProgressDatabase"):
        self.db = db
        self._memory: set = set()
        self._load_existing()

    def _load_existing(self):
        fingerprints = self.db.get_all_fingerprints()
        self._memory.update(fingerprints)

    def is_duplicate(self, perfume) -> bool:
        fp = perfume.generate_fingerprint()
        return fp in self._memory

    def add(self, perfume):
        fp = perfume.generate_fingerprint()
        self._memory.add(fp)
        self.db.add_fingerprint(fp, perfume.brand, perfume.name)


class ProgressDatabase:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS collected_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                url TEXT UNIQUE,
                url_type TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS scraping_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                url TEXT UNIQUE,
                status TEXT,
                error_message TEXT,
                retries INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                perfume_id TEXT,
                data TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                total_urls INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fingerprints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT UNIQUE,
                brand TEXT,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def is_url_collected(self, url: str) -> bool:
        cursor = self.conn.execute("SELECT 1 FROM collected_urls WHERE url = ?", (url,))
        return cursor.fetchone() is not None

    def add_collected_url(self, source: str, url: str, url_type: str):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO collected_urls (source, url, url_type) VALUES (?, ?, ?)",
                (source, url, url_type)
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error adding collected URL", error=str(e))

    def add_urls_batch(self, source: str, urls: List[Tuple[str, str]]):
        try:
            self.conn.executemany(
                "INSERT OR IGNORE INTO collected_urls (source, url, url_type) VALUES (?, ?, ?)",
                [(source, url, url_type) for url, url_type in urls]
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error adding URLs batch", error=str(e))

    def get_pending_urls(self, source: str, limit: int = 50) -> list:
        cursor = self.conn.execute("""
            SELECT url FROM scraping_progress 
            WHERE source = ? AND status IN ('pending', 'failed') 
            AND retries < 5
            ORDER BY last_attempt ASC
            LIMIT ?
        """, (source, limit))
        return [row[0] for row in cursor.fetchall()]

    def get_all_pending_count(self, source: str) -> int:
        cursor = self.conn.execute("""
            SELECT COUNT(*) FROM scraping_progress 
            WHERE source = ? AND status IN ('pending', 'failed') AND retries < 5
        """, (source,))
        return cursor.fetchone()[0]

    def update_progress(self, source: str, url: str, status: str, error: str = None):
        try:
            cursor = self.conn.execute(
                "SELECT retries FROM scraping_progress WHERE url = ?", (url,)
            )
            row = cursor.fetchone()
            retries = (row[0] + 1) if row else 1

            self.conn.execute("""
                INSERT INTO scraping_progress 
                    (source, url, status, error_message, last_attempt, retries, completed_at)
                VALUES (?, ?, ?, ?, ?, ?,
                    CASE WHEN ? = 'completed' THEN CURRENT_TIMESTAMP ELSE NULL END)
                ON CONFLICT(url) DO UPDATE SET
                    status        = excluded.status,
                    error_message = excluded.error_message,
                    last_attempt  = excluded.last_attempt,
                    retries       = excluded.retries,
                    completed_at  = excluded.completed_at
            """, (source, url, status, error,
                  datetime.now().isoformat(), retries, status))
            self.conn.commit()
            self.update_stats(source)
        except Exception as e:
            logger.error(f"Error updating progress", error=str(e))

    def update_stats(self, source: str):
        cursor = self.conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM scraping_progress WHERE source = ?
        """, (source,))
        total, completed, failed = cursor.fetchone()

        self.conn.execute("""
            INSERT OR REPLACE INTO stats (source, total_urls, completed, failed, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (source, total or 0, completed or 0, failed or 0))
        self.conn.commit()

    def get_stats(self, source: str) -> Dict:
        cursor = self.conn.execute(
            "SELECT total_urls, completed, failed FROM stats WHERE source = ?",
            (source,)
        )
        row = cursor.fetchone()
        return (
            {'total': row[0] or 0, 'completed': row[1] or 0, 'failed': row[2] or 0}
            if row else {'total': 0, 'completed': 0, 'failed': 0}
        )

    def save_scraped_data(self, source: str, perfume_id: str, data: dict):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO scraped_data (source, perfume_id, data) VALUES (?, ?, ?)",
                (source, perfume_id, json.dumps(data, ensure_ascii=False))
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving scraped data", error=str(e))

    def get_all_scraped_data(self, source: str = None) -> list:
        if source:
            cursor = self.conn.execute(
                "SELECT data FROM scraped_data WHERE source = ?", (source,)
            )
        else:
            cursor = self.conn.execute("SELECT data FROM scraped_data")
        return [json.loads(row[0]) for row in cursor]

    def initialize_progress_from_collected(self, source: str):
        cursor = self.conn.execute("""
            INSERT OR IGNORE INTO scraping_progress (source, url, status, last_attempt)
            SELECT ?, url, 'pending', CURRENT_TIMESTAMP
            FROM collected_urls 
            WHERE source = ? AND url_type = 'perfume'
            AND NOT EXISTS (
                SELECT 1 FROM scraping_progress WHERE url = collected_urls.url
            )
        """, (source, source))
        self.conn.commit()
        logger.info(f"Initialized pending URLs", source=source, count=cursor.rowcount)

    def add_fingerprint(self, fingerprint: str, brand: str, name: str):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO fingerprints (fingerprint, brand, name) VALUES (?, ?, ?)",
                (fingerprint, brand, name)
            )
            self.conn.commit()
        except Exception as e:
            logger.error("Error adding fingerprint", error=str(e))

    def is_fingerprint_exists(self, fingerprint: str) -> bool:
        cursor = self.conn.execute(
            "SELECT 1 FROM fingerprints WHERE fingerprint = ?", (fingerprint,)
        )
        return cursor.fetchone() is not None

    def get_all_fingerprints(self) -> List[str]:
        cursor = self.conn.execute("SELECT fingerprint FROM fingerprints")
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        self.conn.close()


class BaseScraper:
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = self._create_session()
        self.request_count = 0
        self.robots_checker = RobotsChecker()
        self.rate_limiter = RateLimiter()

    def _get_random_proxy(self) -> Optional[Dict[str, str]]:
        if not PROXIES:
            return None
        proxy = random.choice(PROXIES)
        return {'http': proxy, 'https': proxy}

    def _create_session(self):
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
                delay=10
            )
        except ImportError:
            import requests
            scraper = requests.Session()
            logger.warning("cloudscraper not found, using requests.Session as fallback")

        proxy = self._get_random_proxy()
        if proxy:
            scraper.proxies = proxy

        return scraper

    def _rotate_user_agent(self):
        self.session.headers.update({
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': FRAGRANTICA_BASE,
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        })
        proxy = self._get_random_proxy()
        if proxy:
            self.session.proxies = proxy

    def _smart_delay(self):
        delay = random.uniform(DELAYS['min'], DELAYS['max'])
        self.request_count += 1
        if self.request_count % 50 == 0:
            delay += random.uniform(10, 20)
            logger.info("Taking extended break", delay=round(delay, 1))
        time.sleep(delay)

    def get(self, url: str, retry_count: int = 0):
        from bs4 import BeautifulSoup

        if not self.robots_checker.can_fetch(url):
            logger.warning("Robots.txt disallows URL", url=url)
            return None

        self._rotate_user_agent()
        self.rate_limiter.wait_if_needed(url)

        try:
            response = self.session.get(url, timeout=45, allow_redirects=True)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            elif response.status_code in [403, 429, 503, 404]:
                logger.warning("Blocked or missing page", url=url, status=response.status_code)
                if retry_count < MAX_RETRIES and response.status_code != 404:
                    wait_time = RETRY_DELAY * (retry_count + 1) * 2
                    logger.info("Retrying", retry=retry_count + 1, max=MAX_RETRIES, wait=wait_time)
                    time.sleep(wait_time)
                    return self.get(url, retry_count + 1)
            else:
                logger.warning("Unexpected HTTP status", url=url, status=response.status_code)
        except Exception as e:
            logger.error("Error fetching URL", url=url, error=str(e))
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_DELAY * (retry_count + 1))
                return self.get(url, retry_count + 1)
        return None


class FragranticaScraper(BaseScraper):
    def __init__(self, db: ProgressDatabase):
        super().__init__("fragrantica")
        self.db = db
        self.duplicate_detector = DuplicateDetector(db)

    def collect_all_brands(self) -> List[str]:
        logger.info("Collecting all brand URLs from Fragrantica")
        brand_urls = set()

        brand_sources = [
            f"{FRAGRANTICA_BASE}/brands/",
            f"{FRAGRANTICA_BASE}/browse/",
            f"{FRAGRANTICA_BASE}/designers/",
            f"{FRAGRANTICA_BASE}/index.php?show=brands",
        ]

        for source_url in brand_sources:
            if brand_urls:
                break

            logger.info("Trying brand source", url=source_url)
            soup = self.get(source_url)
            if not soup:
                continue

            brand_selectors = [
                ('a[href*="/brands/"]', '/brands/'),
                ('a[href*="/designers/"]', '/designers/'),
                ('.brand-list a', None),
                ('.brands-list a', None),
                ('.brands a', None),
                ('.alphabetical a', None),
                ('a[href*="perfume"]', None),
            ]

            for selector, href_filter in brand_selectors:
                links = soup.select(selector)
                if links:
                    for link in links:
                        href = link.get('href', '')
                        if href_filter and href_filter not in href:
                            continue
                        if '/perfume/' in href and '.html' in href:
                            continue
                        if href and ('.html' in href or '/brands/' in href or '/designers/' in href):
                            full_url = urljoin(FRAGRANTICA_BASE, href)
                            if full_url not in brand_urls and full_url != source_url:
                                if 'page=' not in full_url:
                                    brand_urls.add(full_url)

                    if brand_urls:
                        logger.info("Found brands", count=len(brand_urls), selector=selector)
                        break

            if not brand_urls:
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if ('/brands/' in href or '/designers/' in href) and '.html' in href:
                        full_url = urljoin(FRAGRANTICA_BASE, href)
                        if full_url not in brand_urls:
                            brand_urls.add(full_url)

        brand_urls = list(brand_urls)
        logger.info("Unique brands found", count=len(brand_urls))

        if brand_urls:
            urls_to_add = [(url, "brand") for url in brand_urls[:500]]
            self.db.add_urls_batch(self.source_name, urls_to_add)

        return brand_urls

    def collect_perfumes_from_brand(self, brand_url: str) -> List[str]:
        perfume_urls = set()
        current_url = brand_url
        visited_pages = set()
        max_pages = 50

        while current_url and current_url not in visited_pages and len(visited_pages) < max_pages:
            visited_pages.add(current_url)
            soup = self.get(current_url)
            if not soup:
                break

            perfume_selectors = [
                'a[href*="/perfume/"]',
                '.perfume-list a',
                '.perfumes a',
                '.product a',
                '.item a',
            ]

            for selector in perfume_selectors:
                for link in soup.select(selector):
                    href = link.get('href', '')
                    if '/perfume/' in href and href.endswith('.html'):
                        full_url = urljoin(FRAGRANTICA_BASE, href)
                        perfume_urls.add(full_url)

            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/perfume/' in href and href.endswith('.html'):
                    full_url = urljoin(FRAGRANTICA_BASE, href)
                    perfume_urls.add(full_url)

            next_url = None
            pagination_selectors = [
                'a.next',
                'a:contains("next")',
                'a:contains("Next")',
                'a:contains("›")',
                'a:contains("»")',
                'a[rel="next"]',
            ]

            for selector in pagination_selectors:
                try:
                    next_link = soup.select_one(selector)
                    if next_link and next_link.get('href'):
                        candidate = urljoin(FRAGRANTICA_BASE, next_link['href'])
                        if candidate not in visited_pages:
                            next_url = candidate
                            break
                except Exception:
                    pass

            if not next_url:
                current_page_match = re.search(r'[?&]page=(\d+)', current_url)
                current_page = int(current_page_match.group(1)) if current_page_match else 1
                next_page_link = soup.find('a', href=re.compile(rf'page={current_page + 1}'))
                if next_page_link:
                    candidate = urljoin(FRAGRANTICA_BASE, next_page_link['href'])
                    if candidate not in visited_pages:
                        next_url = candidate

            current_url = next_url

        return list(perfume_urls)

    def collect_all_perfumes(self) -> List[str]:
        logger.info("Collecting perfume URLs using search pages")

        all_perfume_urls = set()
        max_pages = 100

        for page in range(1, max_pages + 1):
            url = f"{FRAGRANTICA_BASE}/search/?q=&page={page}"
            logger.info("Scraping search page", page=page, url=url)

            soup = self.get(url)
            if not soup:
                continue

            found_on_page = 0

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if re.match(r"^/perfume/.+\.html$", href):
                    full_url = urljoin(FRAGRANTICA_BASE, href)
                    if full_url not in all_perfume_urls:
                        all_perfume_urls.add(full_url)
                        found_on_page += 1

            logger.info("Perfumes found on page", page=page, count=found_on_page)

            if found_on_page == 0:
                logger.info("No more perfumes found, stopping pagination")
                break

        if not all_perfume_urls:
            logger.error("No perfume URLs collected. Check blocking or selectors.")
            return []

        urls_to_add = [(url, "perfume") for url in all_perfume_urls]
        self.db.add_urls_batch(self.source_name, urls_to_add)

        logger.info("Total unique perfume URLs collected", count=len(all_perfume_urls))
        return list(all_perfume_urls)

    def scrape_perfume_details(self, url: str) -> Optional[FragranticaPerfume]:
        soup = self.get(url)
        if not soup:
            return None

        perfume = FragranticaPerfume(url=url)

        match = re.search(r'-(\d+)\.html$', url)
        if match:
            perfume.id = match.group(1)
        else:
            match = re.search(r'/(\d+)(?:[/?]|$)', url)
            if match:
                perfume.id = match.group(1)

        h1 = soup.find('h1', {'itemprop': 'name'}) or soup.find('h1')
        if h1:
            brand_elem = (
                    h1.find('span', {'itemprop': 'brand'})
                    or h1.find('span', {'itemprop': 'name'})
                    or h1.find('a', href=re.compile(r'/brands/|/designers/'))
            )
            if brand_elem:
                perfume.brand = brand_elem.get_text(strip=True)
                perfume.name = h1.get_text(strip=True).replace(perfume.brand, '').strip()
            else:
                perfume.name = h1.get_text(strip=True)

            if not perfume.brand:
                brand_link = soup.find('a', href=re.compile(r'/brands/|/designers/'))
                if brand_link:
                    perfume.brand = brand_link.get_text(strip=True)

        perfumer_elem = soup.find(itemprop='author') or soup.find('a', href=re.compile(r'/noses/'))
        if perfumer_elem:
            perfume.perfumer = perfumer_elem.get_text(strip=True)

        year_elem = soup.find(string=re.compile(r'\b(19|20)\d{2}\b'))
        if year_elem:
            year_match = re.search(r'\b((19|20)\d{2})\b', year_elem)
            if year_match:
                perfume.year = year_match.group(1)

        for text_elem in soup.find_all(string=re.compile(r'for women|for men|unisex', re.I)):
            gender_match = re.search(r'for (women|men)|unisex', text_elem, re.I)
            if gender_match:
                perfume.gender = gender_match.group(0).title()
                break

        rating_elem = soup.find(itemprop='ratingValue')
        if rating_elem:
            try:
                perfume.rating = float(rating_elem.get_text(strip=True))
            except ValueError:
                pass

        votes_elem = soup.find(itemprop='ratingCount')
        if votes_elem:
            try:
                votes_text = re.sub(r'[^\d]', '', votes_elem.get_text(strip=True))
                perfume.votes_count = int(votes_text) if votes_text else 0
            except ValueError:
                pass

        pyramid = soup.find('div', id='pyramid')
        if not pyramid:
            pyramid = soup.find('div', class_=re.compile(r'pyramid', re.I))

        if pyramid:
            sections = pyramid.find_all('div', recursive=False)
            if not sections:
                sections = pyramid.find_all(['div', 'ul', 'ol'], recursive=True)

            for section in sections:
                header = section.find(['h3', 'h4', 'h5', 'strong', 'b'])
                if not header:
                    continue
                header_text = header.get_text(strip=True).lower()
                notes = [
                    a.get_text(strip=True)
                    for a in section.find_all('a')
                    if a.get_text(strip=True) and len(a.get_text(strip=True)) < 50
                ]
                if 'top' in header_text:
                    perfume.notes_top = notes[:20]
                elif 'middle' in header_text or 'heart' in header_text:
                    perfume.notes_middle = notes[:20]
                elif 'base' in header_text:
                    perfume.notes_base = notes[:20]

        for accord_elem in soup.find_all(class_=re.compile(r'accord', re.I)):
            name_elem = accord_elem.find(['span', 'div'], class_=re.compile(r'name|label', re.I))
            style = accord_elem.get('style', '')
            width_match = re.search(r'width:\s*([\d.]+)%', style)
            value = None
            if width_match:
                value = float(width_match.group(1))
            else:
                val_elem = accord_elem.find(
                    ['span', 'div'], class_=re.compile(r'value|percent|bar', re.I)
                )
                if val_elem:
                    val_text = re.sub(r'[^\d.]', '', val_elem.get_text(strip=True))
                    value = float(val_text) if val_text else None

            if name_elem and value is not None:
                perfume.main_accords[name_elem.get_text(strip=True)] = value

        perfume.reviews = self.scrape_all_reviews(url, soup)

        return perfume

    def scrape_all_reviews(self, perfume_url: str, soup=None) -> List[Review]:
        from bs4 import BeautifulSoup

        all_reviews = []

        if not soup:
            soup = self.get(perfume_url)
            if not soup:
                return all_reviews

        def extract_reviews_from_soup(s) -> List[Review]:
            reviews = []
            review_container = s.find('div', id=re.compile(r'review', re.I))
            blocks = []
            if review_container:
                blocks = review_container.find_all('div', class_=re.compile(r'cell|review', re.I))
            if not blocks:
                blocks = s.find_all('div', class_=re.compile(r'review', re.I))
            if not blocks:
                blocks = s.find_all('div', class_=re.compile(r'comment', re.I))

            for block in blocks[:MAX_REVIEWS_PER_PERFUME]:
                review = Review()

                user_elem = (
                        block.find('span', class_=re.compile(r'user|author|member', re.I))
                        or block.find('a', href=re.compile(r'/member/|/user/', re.I))
                        or block.find(class_=re.compile(r'username', re.I))
                )
                if user_elem:
                    review.username = user_elem.get_text(strip=True)

                rating_elem = block.find(class_=re.compile(r'rating|star|score', re.I))
                if rating_elem:
                    rating_text = rating_elem.get_text(strip=True)
                    rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                    if rating_match:
                        try:
                            review.rating = float(rating_match.group(1))
                        except ValueError:
                            pass

                date_elem = block.find(['time', 'span'], class_=re.compile(r'date|time', re.I))
                if date_elem:
                    review.date = (
                            date_elem.get('datetime')
                            or date_elem.get_text(strip=True)
                    )

                title_elem = block.find(['h3', 'h4', 'strong'])
                if title_elem:
                    review.title = title_elem.get_text(strip=True)

                content_elem = block.find('p') or block.find(class_=re.compile(r'text|content|body', re.I))
                if content_elem:
                    review.content = content_elem.get_text(strip=True)

                likes_elem = block.find(class_=re.compile(r'like|helpful|vote', re.I))
                if likes_elem:
                    likes_text = re.sub(r'[^\d]', '', likes_elem.get_text(strip=True))
                    review.likes = int(likes_text) if likes_text else 0

                validated = review.validate()
                if validated and (validated.content or validated.title):
                    reviews.append(validated)

            return reviews

        all_reviews.extend(extract_reviews_from_soup(soup))

        if not all_reviews:
            review_links = soup.find_all('a', href=re.compile(r'reviews/\d+'))
            for link in review_links:
                reviews_url = urljoin(FRAGRANTICA_BASE, link['href'])
                reviews_soup = self.get(reviews_url)
                if reviews_soup:
                    all_reviews.extend(extract_reviews_from_soup(reviews_soup))

        logger.info("Reviews extracted", url=perfume_url, count=len(all_reviews))
        return all_reviews

    def crawl_all(self):
        logger.info("Starting Fragrantica full crawl")

        self.db.initialize_progress_from_collected(self.source_name)

        pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

        if not pending_urls:
            logger.info("No pending URLs found. Collecting all perfume URLs first.")
            self.collect_all_perfumes()

            self.db.initialize_progress_from_collected(self.source_name)
            pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

            if not pending_urls:
                logger.error("Still no URLs after collection. Stopping.")
                return

        total_pending = self.db.get_all_pending_count(self.source_name)
        logger.info("Total pending perfumes", count=total_pending)

        processed = 0
        while pending_urls and processed < total_pending:
            for url in pending_urls:
                logger.info("Scraping perfume", index=processed + 1, total=total_pending, url=url)

                try:
                    perfume = self.scrape_perfume_details(url)
                    if perfume and (perfume.name or perfume.id):
                        if not self.duplicate_detector.is_duplicate(perfume):
                            validated = perfume.validate()
                            if validated:
                                self.db.save_scraped_data(self.source_name, perfume.id, perfume.to_dict())
                                self.duplicate_detector.add(perfume)
                            else:
                                self.db.save_scraped_data(self.source_name, perfume.id, perfume.to_dict())
                        else:
                            logger.info("Duplicate skipped", brand=perfume.brand, name=perfume.name)
                        self.db.update_progress(self.source_name, url, "completed")
                        logger.info("Scraped successfully", brand=perfume.brand, name=perfume.name)
                    else:
                        self.db.update_progress(
                            self.source_name, url, "failed", "No data extracted"
                        )
                        logger.warning("Failed to extract data", url=url)
                except Exception as e:
                    logger.error("Scraping error", url=url, error=str(e))
                    self.db.update_progress(self.source_name, url, "failed", str(e))

                processed += 1

                stats = self.db.get_stats(self.source_name)
                logger.info(
                    "Progress update",
                    completed=stats['completed'],
                    total=stats['total'],
                    failed=stats['failed']
                )

            pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

        logger.info("Fragrantica crawl complete", processed=processed)


class FragranceNetScraper(BaseScraper):
    def __init__(self, db: ProgressDatabase):
        super().__init__("fragrancenet")
        self.db = db
        self.duplicate_detector = DuplicateDetector(db)

    def collect_all_products(self) -> List[str]:
        logger.info("Collecting all product URLs from FragranceNet")
        product_urls = set()

        catalog_base_urls = [
            f"{FRAGRANCENET_BASE}/fragrance/women",
            f"{FRAGRANCENET_BASE}/fragrance/men",
            f"{FRAGRANCENET_BASE}/fragrance/unisex",
            f"{FRAGRANCENET_BASE}/perfume",
            f"{FRAGRANCENET_BASE}/cologne",
            f"{FRAGRANCENET_BASE}/browse",
        ]

        for catalog_url in catalog_base_urls:
            page = 1
            visited_pages = set()
            max_pages = 30

            while page <= max_pages:
                if page == 1:
                    paged_url = catalog_url
                else:
                    paged_url = f"{catalog_url}?page={page}"

                if paged_url in visited_pages:
                    break
                visited_pages.add(paged_url)

                logger.info("Collecting catalog page", page=page, catalog=catalog_url)
                soup = self.get(paged_url)
                if not soup:
                    page += 1
                    continue

                found_on_page = 0

                product_selectors = [
                    'a[href*="/p/"]',
                    'a[href*="/product/"]',
                    'a[href*="/fragrance/"]',
                    '.product-item a',
                    '.product-tile a',
                    '.grid-item a',
                ]

                for selector in product_selectors:
                    for link in soup.select(selector):
                        href = link.get('href', '')
                        if re.search(r'/(p/|product/|fragrance/)[^/]+/\d+', href):
                            full_url = urljoin(FRAGRANCENET_BASE, href.split('?')[0])
                            if full_url not in product_urls:
                                product_urls.add(full_url)
                                found_on_page += 1

                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if re.search(r'/(\d+)\.html', href) or re.search(r'/(p/|product/)[^/]+/\d+', href):
                        full_url = urljoin(FRAGRANCENET_BASE, href.split('?')[0])
                        if full_url not in product_urls:
                            product_urls.add(full_url)
                            found_on_page += 1

                logger.info("Products found on page", page=page, count=found_on_page)

                has_next = False
                pagination_selectors = [
                    'a.next-page',
                    'a:contains("Next")',
                    'a:contains("next")',
                    'a[rel="next"]',
                    'a.pagination__next',
                ]

                for selector in pagination_selectors:
                    try:
                        next_link = soup.select_one(selector)
                        if next_link and next_link.get('href'):
                            has_next = True
                            break
                    except Exception:
                        pass

                if not has_next and found_on_page == 0:
                    break

                if has_next or found_on_page > 0:
                    page += 1
                else:
                    break

        if not product_urls:
            logger.info("Trying sitemap approach")
            sitemap_url = f"{FRAGRANCENET_BASE}/sitemap.xml"
            soup = self.get(sitemap_url)
            if soup:
                for loc in soup.find_all('loc'):
                    url_text = loc.get_text(strip=True)
                    if 'fragrance' in url_text and ('/p/' in url_text or '/product/' in url_text):
                        product_urls.add(url_text)

        logger.info("Total unique product URLs", count=len(product_urls))

        if product_urls:
            urls_to_add = [(url, "perfume") for url in product_urls]
            self.db.add_urls_batch(self.source_name, urls_to_add)

        return list(product_urls)

    def _extract_prices(self, soup, url: str) -> List[Price]:
        prices = []

        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get('@type') not in ('Product', 'Offer', 'ProductGroup'):
                        continue
                    offers = item.get('offers', [])
                    if isinstance(offers, dict):
                        offers = [offers]
                    for offer in offers:
                        price = Price()
                        price.retailer = "FragranceNet"
                        price.url = url
                        try:
                            price_val = offer.get('price', 0)
                            if isinstance(price_val, str):
                                price_val = price_val.replace('$', '').replace(',', '')
                            price.price = float(price_val)
                        except (ValueError, TypeError):
                            continue
                        if price.price == 0:
                            continue
                        price.currency = offer.get('priceCurrency', 'USD')
                        price.in_stock = 'InStock' in str(offer.get('availability', ''))
                        price.size = offer.get('name', '').replace(price.retailer, '').strip()
                        size_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:oz|ml|fl\.?\s*oz)', price.size, re.I)
                        if size_match:
                            price.size = size_match.group(0)
                        validated = price.validate()
                        if validated:
                            prices.append(validated)
            except (json.JSONDecodeError, AttributeError):
                pass

        if not prices:
            price_selectors = [
                '[itemprop="price"]',
                '.price-current',
                '.product-price',
                '.price',
                '.sale-price',
                '.our-price',
                '[data-price]',
                '.js-price',
            ]

            for selector in price_selectors:
                for price_elem in soup.select(selector):
                    price_text = price_elem.get_text(strip=True)
                    match = re.search(r'[\$€£]([\d,]+(?:\.\d{1,2})?)', price_text)
                    if match:
                        price = Price()
                        price.retailer = "FragranceNet"
                        price.url = url
                        try:
                            price.price = float(match.group(1).replace(',', ''))
                        except ValueError:
                            continue
                        currency_map = {'$': 'USD', '€': 'EUR', '£': 'GBP'}
                        price.currency = currency_map.get(price_text[0], 'USD')
                        validated = price.validate()
                        if validated:
                            prices.append(validated)
                if prices:
                    break

        return prices[:5]

    def scrape_product_details(self, url: str) -> Optional[FragranceNetPerfume]:
        soup = self.get(url)
        if not soup:
            return None

        perfume = FragranceNetPerfume(url=url)

        match = re.search(r'/(\d+)(?:[/?]|$)', url)
        if match:
            perfume.id = match.group(1)

        for script in soup.find_all('script', type='application/ld+json'):
            if not script.string:
                continue
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get('@type') == 'Product':
                        perfume.name = item.get('name', '')
                        brand_info = item.get('brand', {})
                        if isinstance(brand_info, dict):
                            perfume.brand = brand_info.get('name', '')
                        if not perfume.brand and 'brand' in item:
                            perfume.brand = item.get('brand', '')
                        break
            except (json.JSONDecodeError, AttributeError):
                pass

        if not perfume.name:
            h1 = soup.find('h1')
            if h1:
                full_title = h1.get_text(strip=True)
                by_match = re.search(r'^(.+?)\s+by\s+(.+)$', full_title, re.I)
                if by_match:
                    perfume.name = by_match.group(1).strip()
                    perfume.brand = by_match.group(2).strip()
                else:
                    perfume.name = full_title

        if not perfume.brand:
            brand_selectors = [
                '[itemprop="brand"]',
                '.brand-name',
                '.product-brand',
                '.brand a',
            ]
            for selector in brand_selectors:
                brand_elem = soup.select_one(selector)
                if brand_elem:
                    perfume.brand = brand_elem.get_text(strip=True)
                    break

        for elem in soup.find_all(string=re.compile(r"women'?s?|men'?s?|unisex|for her|for him", re.I)):
            gender_match = re.search(r"(women'?s?|men'?s?|unisex|for her|for him)", elem, re.I)
            if gender_match:
                gender_text = gender_match.group(1).lower()
                if 'women' in gender_text or 'her' in gender_text:
                    perfume.gender = "Women"
                elif 'men' in gender_text or 'him' in gender_text:
                    perfume.gender = "Men"
                elif 'unisex' in gender_text:
                    perfume.gender = "Unisex"
                break

        family_elem = soup.find(string=re.compile(r'fragrance family|scent family|family:', re.I))
        if family_elem:
            if family_elem.parent:
                sibling = family_elem.parent.find_next_sibling()
                if sibling:
                    perfume.fragrance_family = sibling.get_text(strip=True)
                elif family_elem.parent.parent:
                    next_elem = family_elem.parent.find_next_sibling()
                    if next_elem:
                        perfume.fragrance_family = next_elem.get_text(strip=True)

        notes_section = soup.find(string=re.compile(r'notes?|scent notes', re.I))
        if notes_section:
            notes_container = notes_section.parent
            if notes_container:
                for container in [notes_container, notes_container.find_next_sibling(), notes_container.parent]:
                    if container:
                        notes = []
                        for elem in container.find_all(['a', 'li', 'span', 'div']):
                            text = elem.get_text(strip=True)
                            if text and len(text) < 50 and not re.search(r'\d', text):
                                notes.append(text)
                        if notes:
                            perfume.notes = notes[:15]
                            break

        in_stock_elem = soup.find(itemprop='availability')
        if in_stock_elem:
            perfume.in_stock = 'instock' in in_stock_elem.get_text(strip=True).lower()
        else:
            out_of_stock_text = soup.find(string=re.compile(r'out of stock|sold out|discontinued', re.I))
            if out_of_stock_text:
                perfume.in_stock = False

        perfume.prices = self._extract_prices(soup, url)

        return perfume

    def crawl_all(self):
        logger.info("Starting FragranceNet full crawl")

        self.db.initialize_progress_from_collected(self.source_name)

        pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

        if not pending_urls:
            logger.info("No pending URLs found. Collecting all product URLs first.")
            self.collect_all_products()
            self.db.initialize_progress_from_collected(self.source_name)
            pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

        total_pending = self.db.get_all_pending_count(self.source_name)
        logger.info("Total pending products", count=total_pending)

        processed = 0
        while pending_urls and processed < total_pending:
            for url in pending_urls:
                logger.info("Scraping product", index=processed + 1, total=total_pending, url=url)

                try:
                    product = self.scrape_product_details(url)
                    if product and (product.name or product.id):
                        if not self.duplicate_detector.is_duplicate(product):
                            validated = product.validate()
                            if validated:
                                self.db.save_scraped_data(self.source_name, product.id, product.to_dict())
                                self.duplicate_detector.add(product)
                            else:
                                self.db.save_scraped_data(self.source_name, product.id, product.to_dict())
                        else:
                            logger.info("Duplicate skipped", brand=product.brand, name=product.name)
                        self.db.update_progress(self.source_name, url, "completed")
                        price_info = f"${product.prices[0].price}" if product.prices else 'N/A'
                        logger.info("Scraped successfully", brand=product.brand, name=product.name, price=price_info)
                    else:
                        self.db.update_progress(
                            self.source_name, url, "failed", "No data extracted"
                        )
                        logger.warning("Failed to extract data", url=url)
                except Exception as e:
                    logger.error("Scraping error", url=url, error=str(e))
                    self.db.update_progress(self.source_name, url, "failed", str(e))

                processed += 1

                stats = self.db.get_stats(self.source_name)
                logger.info(
                    "Progress update",
                    completed=stats['completed'],
                    total=stats['total'],
                    failed=stats['failed']
                )

            pending_urls = self.db.get_pending_urls(self.source_name, limit=100)

        logger.info("FragranceNet crawl complete", processed=processed)


class ScrapingManager:
    def __init__(self):
        self.db = ProgressDatabase()
        self.fragrantica_scraper = FragranticaScraper(self.db)
        self.fragrancenet_scraper = FragranceNetScraper(self.db)
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        logger.info("Received stop signal. Saving progress and exiting gracefully.")
        self.running = False

    def export_to_json(self):
        logger.info("Exporting data to JSON files")

        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        fragrantica_data = self.db.get_all_scraped_data("fragrantica")
        if fragrantica_data:
            fragrantica_file = output_path / f"fragrantica_perfumes_{timestamp}.json"
            with open(fragrantica_file, 'w', encoding='utf-8') as f:
                json.dump(fragrantica_data, f, ensure_ascii=False, indent=2)
            logger.info("Exported fragrantica data", count=len(fragrantica_data), file=str(fragrantica_file))

        fragrancenet_data = self.db.get_all_scraped_data("fragrancenet")
        if fragrancenet_data:
            fragrancenet_file = output_path / f"fragrancenet_products_{timestamp}.json"
            with open(fragrancenet_file, 'w', encoding='utf-8') as f:
                json.dump(fragrancenet_data, f, ensure_ascii=False, indent=2)
            logger.info("Exported fragrancenet data", count=len(fragrancenet_data), file=str(fragrancenet_file))

        all_data = {
            "metadata": {
                "scraped_at": datetime.now().isoformat(),
                "total_fragrantica": len(fragrantica_data),
                "total_fragrancenet": len(fragrancenet_data),
                "sources": ["fragrantica", "fragrancenet"]
            },
            "fragrantica": fragrantica_data,
            "fragrancenet": fragrancenet_data
        }

        combined_file = output_path / f"all_perfumes_data_{timestamp}.json"
        with open(combined_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        logger.info("Exported combined data", file=str(combined_file))

    def print_statistics(self):
        fragrantica_stats = self.db.get_stats("fragrantica")
        fragrancenet_stats = self.db.get_stats("fragrancenet")

        logger.info(
            "Final statistics",
            fragrantica_completed=fragrantica_stats['completed'],
            fragrancenet_completed=fragrancenet_stats['completed'],
            output_dir=OUTPUT_DIR
        )

    def run_continuous(self, days: int = 7):
        start_time = datetime.now()
        end_time = start_time + timedelta(days=days)

        logger.info("Starting scraping operation", days=days, until=str(end_time.date()))

        iteration = 0
        while self.running and datetime.now() < end_time:
            iteration += 1
            logger.info("Starting iteration", iteration=iteration)

            try:
                logger.info("Crawling Fragrantica")
                self.fragrantica_scraper.crawl_all()
            except Exception as e:
                logger.error("Error in Fragrantica scraper", error=str(e))

            try:
                logger.info("Crawling FragranceNet")
                self.fragrancenet_scraper.crawl_all()
            except Exception as e:
                logger.error("Error in FragranceNet scraper", error=str(e))

            self.export_to_json()
            self.print_statistics()

            pending_fragrantica = self.db.get_all_pending_count("fragrantica")
            pending_fragrancenet = self.db.get_all_pending_count("fragrancenet")

            if pending_fragrantica == 0 and pending_fragrancenet == 0:
                logger.info("All scraping tasks completed successfully!")
                break

            if self.running and datetime.now() < end_time:
                wait_hours = random.uniform(6, 12)
                logger.info("Taking a break", hours=round(wait_hours, 1))
                time.sleep(wait_hours * 3600)

        self.export_to_json()
        self.print_statistics()
        self.db.close()

        logger.info("Scraping operation completed!")


def main():
    print("=" * 60)
    print("Perfume Scraper - Complete Dataset Collection")
    print("=" * 60)

    global PROXIES
    PROXIES = load_proxies()

    initialize_user_agent()

    manager = ScrapingManager()

    try:
        manager.run_continuous(days=7)
    except KeyboardInterrupt:
        logger.info("User interrupted. Saving data.")
        manager.export_to_json()
        manager.print_statistics()
    except Exception as e:
        logger.error("Fatal error", error=str(e))
        manager.export_to_json()

    print("\nDone! Check the 'output' folder for JSON files.")


if __name__ == "__main__":
    main()