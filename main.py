"""
Telegram News Filter Bot — БЕЗОПАСНАЯ версия (RSS + Telegram каналы)
=====================================================================
Два бота:
  Бот 1 → Risk Management News
  Бот 2 → Counterparty Risk News

Secrets (только 4):
  BOT1_TOKEN      — токен бота "Risk Management News"
  BOT1_CHAT_ID    — chat_id канала "Risk Management News"
  BOT2_TOKEN      — токен бота "Counterparty Risk News"
  BOT2_CHAT_ID    — chat_id канала "Counterparty Risk News"
"""

import asyncio
import hashlib
import logging
import os
import re
import threading
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
from telegram import Bot
from telegram.constants import ParseMode

# ─── Логирование ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Secrets ──────────────────────────────────────────────────────────────────

BOT1_TOKEN   = os.environ["BOT1_TOKEN"]
BOT1_CHAT_ID = os.environ["BOT1_CHAT_ID"]
BOT2_TOKEN   = os.environ["BOT2_TOKEN"]
BOT2_CHAT_ID = os.environ["BOT2_CHAT_ID"]

# ─── Интервал проверки ────────────────────────────────────────────────────────

CHECK_INTERVAL_SECONDS = 300  # каждые 5 минут

# ─── Источники ────────────────────────────────────────────────────────────────

TELEGRAM_CHANNELS = [
    "asiaplus",
    "SputnikTj",
    "riskovik",
    "centralbankuzbekistan",
    "interfaxonline",
    "rbc_news",
    "banksta",
    "russianmacro",
    "tass_agency",
    "kommersant",
    "vedomosti",
    "markettwits",
    "economistg",
    "World_Sanctions",
    "toporlive",
    "ifinvest",
    "profinansy_news",
]

RSS_FEEDS = [
    # 🇹🇯 Таджикистан
    {"url": "https://asiaplustj.info/ru/rss",                    "name": "Asia-Plus"},
    {"url": "https://avesta.tj/feed",                            "name": "Авеста"},
    {"url": "https://khovar.tj/feed",                            "name": "Ховар"},
    # 🇷🇺 СНГ / Россия
    {"url": "https://rbc.ru/rss/news",                           "name": "РБК"},
    {"url": "https://tass.ru/rss/v2.xml",                        "name": "ТАСС"},
    {"url": "https://www.kommersant.ru/RSS/news.xml",            "name": "Коммерсантъ"},
    {"url": "https://www.interfax.ru/rss.asp",                   "name": "Интерфакс"},
    # 🌍 Мировые
    {"url": "https://cabar.asia/feed",                           "name": "CABAR Asia"},
    {"url": "https://www.imf.org/en/News/rss",                   "name": "МВФ"},
    {"url": "https://feeds.reuters.com/reuters/businessNews",    "name": "Reuters"},
    {"url": "https://oilprice.com/rss/main",                     "name": "OilPrice"},
    {"url": "https://www.kitco.com/rss/kitco-news.xml",          "name": "Kitco"},
    {"url": "https://www.fatf-gafi.org/en/publications/rss.xml", "name": "FATF"},
]

# ─── Ключевые слова ───────────────────────────────────────────────────────────

BOT1_KEYWORDS = [
    # 🔴 САНКЦИИ И РЕГУЛЯТОРНЫЕ РИСКИ
    "санкции", "санкционный список", "SDN", "OFAC", "блокировка активов",
    "отзыв лицензии", "приостановление лицензии", "регулятор ввёл запрет",
    "запрет на операции", "блокировка счетов", "заморозка активов",
    "ограничения на переводы", "запрет на снятие средств",
    # 🔴 БАНКРОТСТВО И ДЕФОЛТ
    "дефолт", "банкротство", "ликвидация банка", "несостоятельность",
    "временная администрация", "санация банка", "bail-out", "bail-in",
    # 🟡 БАНКОВСКИЙ РИСК
    "NPL", "просрочка кредитов", "проблемный кредит",
    "реструктуризация кредитов", "кредитный риск", "рыночный риск",
    "операционный риск", "риск ликвидности", "достаточность капитала",
    "стресс-тест", "резервы под потери", "отток депозитов",
    "дефицит капитала", "нормативы ЦБ",
    # 🟡 ДЕНЕЖНО-КРЕДИТНАЯ ПОЛИТИКА
    "ключевая ставка", "ставка рефинансирования", "процентная ставка",
    "денежно-кредитная политика", "НБТ", "Нацбанк Таджикистана",
    "Национальный банк Таджикистана", "центральный банк", "центробанк",
    "интервенция ЦБ", "валютная интервенция", "регулятор",
    # 🟡 МАКРОЭКОНОМИКА
    "инфляция", "девальвация", "финансовый кризис", "рецессия",
    "валютный риск", "дефицит бюджета", "внешний долг",
    "ослабление валюты", "укрепление валюты", "фондовый рынок",
    "денежный перевод мигрантов", "ВВП", "фискальная политика",
    # 🟢 РЫНКИ — только движение цен
    "нефть подорожала", "нефть подешевела", "цена нефти",
    "нефть выросла", "нефть упала", "нефть резко",
    "золото подорожало", "золото подешевело", "цена золота",
    "золото выросло", "золото упало",
    "курс доллара", "курс евро", "курс рубля", "курс юаня",
    # 🇬🇧 ENGLISH
    "sanctions", "default", "bankruptcy", "financial crisis",
    "inflation", "devaluation", "interest rate", "central bank",
    "license revocation", "asset freeze", "capital adequacy",
    "stress test", "non-performing loan",
    "oil prices fall", "oil prices rise", "oil prices drop",
    "oil prices climb", "per barrel", "brent at $", "wti at $", "crude at $",
    "gold prices fall", "gold prices rise", "gold prices drop",
    "exchange rate",
    # 🇹🇯 ТОҶИКӢ
    "таҳримҳо", "бӯҳрони молиявӣ", "муфлисшавӣ",
    "хавфи бонкӣ", "хавфи қарзӣ", "хавфи бозорӣ",
    "қарзи бад", "қарзи мушкил", "таҷдиди қарз", "беқурбшавӣ",
    "қарзҳои батаъхирафтода", "баромади пасандозҳо",
    "ноустувории қурб", "интиқоли пулии муҳоҷирон",
    "Бонки миллии Тоҷикистон", "шустушӯи пул",
    "нархи нафт", "арзиши нафт", "нархи тилло",
    "қурби асъор", "таваррум", "бозхонди иҷозатнома",
]

BOT2_KEYWORDS = [
    # 🇷🇺 Российские банки
    "Сбербанк", "Сбер", "Sberbank",
    "Tinkoff", "Тинькофф", "Т-Банк", "T-Bank",
    "Транскапиталбанк", "ТКБ Банк", "TKB Bank",
    "МТС Банк", "МТС-Банк", "MTS Bank",
    "Москоммерцбанк", "Moskommertsbank",
    "Цифра банк", "Tsifra Bank",
    "Банк 131", "Bank 131",
    "Солид Банк", "Солидбанк", "Solid Bank",
    # 🇹🇯 Таджикские банки
    "Банк Эсхата", "Эсхата", "Eskhata",
    "Спитамен Банк", "Спитаменбанк", "Spitamen Bank",
    "Азия-Инвест Банк", "Азия Инвест", "Asia Invest Bank",
    "Универсал банк", "АКБ Универсал", "Universal Bank",
    # 🇰🇬 Кыргызские банки
    "Бакай Банк", "Бакайбанк", "Bakai Bank",
    # 🇧🇾 Белорусские банки
    "Паритетбанк", "Паритет Банк", "Paritetbank",
    "МТБанк", "МТ Банк", "MTBank",
    "Технобанк", "Technobank",
    "Белорусский народный банк", "БНБ-Банк", "BNB Bank",
    # 🇰🇿 Казахские банки
    "Банк ЦентрКредит", "ЦентрКредит", "BCC", "CenterCredit",
    # 🌍 Международные банки
    "Bank of Georgia", "Банк Грузии",
    "Ardshinbank", "Ардшинбанк",
    "Mashreqbank", "Машрекбанк", "Mashreq",
    "Agricultural Bank of China", "AgriBank", "Агробанк Китая",
    "Chouzhou Commercial Bank", "Чжоушан банк",
    "Arab Banking Corporation", "ABC Bank",
    "Aktif Yatirim", "Aktif Bank",
    "Asakabank", "Асакабанк",
]

# ─── Фильтр нефть/золото ──────────────────────────────────────────────────────

OIL_GOLD_PRICE_WORDS = [
    "нефть подорожала", "нефть подешевела", "цена нефти",
    "нефть выросла", "нефть упала", "нефть резко",
    "oil prices fall", "oil prices rise", "oil prices drop",
    "oil prices climb", "per barrel", "brent at $", "wti at $", "crude at $",
    "нархи нафт", "арзиши нафт",
    "золото подорожало", "золото подешевело", "цена золота",
    "золото выросло", "золото упало",
    "gold prices fall", "gold prices rise", "gold prices drop",
    "нархи тилло",
]
OIL_GOLD_SOURCES = {"OilPrice", "Kitco"}
OIL_GOLD_GENERAL = ["нефт", "золот", "oil", "gold", "barrel", "brent", "wti", "crude", "нафт", "тилло"]


def is_price_news(text: str, source: str) -> bool:
    if source in OIL_GOLD_SOURCES:
        return True
    low = text.lower()
    return any(kw.lower() in low for kw in OIL_GOLD_PRICE_WORDS)


# ─── Дедупликация по заголовку ────────────────────────────────────────────────

# Хранит заголовки последних N новостей для сравнения похожести
recent_titles: list[str] = []
RECENT_TITLES_MAX = 500
SIMILARITY_THRESHOLD = 0.80  # 80% похожести = дубликат


def is_duplicate_title(title: str) -> bool:
    """Возвращает True если заголовок похож на уже отправленный."""
    if not title:
        return False
    title_low = title.lower().strip()
    for prev in recent_titles:
        ratio = SequenceMatcher(None, title_low, prev).ratio()
        if ratio >= SIMILARITY_THRESHOLD:
            return True
    return False


def remember_title(title: str):
    """Запоминает заголовок для будущих проверок."""
    if not title:
        return
    recent_titles.append(title.lower().strip())
    if len(recent_titles) > RECENT_TITLES_MAX:
        recent_titles.pop(0)


# ─── Вспомогательные функции ──────────────────────────────────────────────────

def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def item_id(source: str, link: str, title: str) -> str:
    return hashlib.md5(f"{source}:{link or title}".encode()).hexdigest()


def match_keywords(text: str, keywords: list) -> list:
    low = text.lower()
    return [kw for kw in keywords if kw.lower() in low]


def build_message(title: str, source: str, link: str, matched: list) -> str:
    kw_str = " · ".join(f"<i>{kw}</i>" for kw in matched[:5])
    link_part = f'\n🔗 <a href="{link}">читать</a>' if link else ""
    return f"🔔 {title}\n📢 {source}{link_part}\n🔑 {kw_str}"


# ─── Ping сервер для UptimeRobot / Railway ────────────────────────────────────

stats = {"bot1_sent": 0, "bot2_sent": 0, "checks": 0, "posts_seen": 0}


class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        if self.path == "/status":
            import json
            body = json.dumps(stats).encode()
            self.wfile.write(body)
        else:
            self.wfile.write(b"OK - Bot is running")

    def log_message(self, format, *args):
        pass


def start_ping_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), PingHandler)
    log.info(f"🌐 Ping сервер на порту {port}")
    server.serve_forever()


# ─── RSS парсер ───────────────────────────────────────────────────────────────

async def fetch_rss(client: httpx.AsyncClient, url: str, name: str) -> list:
    try:
        r = await client.get(url, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"RSS ошибка [{name}]: {e}")
        return []
    posts = []
    try:
        root = ET.fromstring(r.content)
        for item in root.findall(".//item"):
            title = strip_html(item.findtext("title") or "")
            link  = (item.findtext("link") or "").strip()
            desc  = strip_html(item.findtext("description") or "")
            posts.append({
                "source": name,
                "title":  title,
                "link":   link,
                "text":   f"{title} {desc}".strip(),
            })
    except ET.ParseError as e:
        log.warning(f"XML ошибка [{name}]: {e}")
    return posts


async def collect_all_posts(http: httpx.AsyncClient) -> list:
    tasks = []
    for ch in TELEGRAM_CHANNELS:
        tasks.append(fetch_rss(http, f"https://rsshub.app/telegram/channel/{ch}", f"@{ch}"))
    for feed in RSS_FEEDS:
        tasks.append(fetch_rss(http, feed["url"], feed["name"]))
    results = await asyncio.gather(*tasks)
    return [post for batch in results for post in batch]


# ─── Отправка ─────────────────────────────────────────────────────────────────

async def send(bot: Bot, chat_id: str, text: str):
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await asyncio.sleep(0.5)
    except Exception as e:
        log.error(f"Ошибка отправки → {chat_id}: {e}")


# ─── Проверка новостей ────────────────────────────────────────────────────────

seen_ids: set = set()


async def check_once(bot1: Bot, bot2: Bot, http: httpx.AsyncClient):
    all_posts = await collect_all_posts(http)
    sent1 = sent2 = new_total = skipped_dup = 0

    for post in all_posts:
        # 1. Проверка по ID (точный дубликат из того же источника)
        pid = item_id(post["source"], post["link"], post["title"])
        if pid in seen_ids:
            continue
        seen_ids.add(pid)

        # 2. Проверка по похожести заголовка (одна новость из разных источников)
        title  = post["title"] or post["text"][:120]
        if is_duplicate_title(title):
            skipped_dup += 1
            continue

        new_total += 1
        text   = post["text"]
        source = post["source"]
        link   = post["link"]

        # БОТ 1 — Risk Management
        m1 = match_keywords(text, BOT1_KEYWORDS)
        if m1:
            oil_gold = [kw for kw in m1 if any(w in kw.lower() for w in OIL_GOLD_GENERAL)]
            other    = [kw for kw in m1 if kw not in oil_gold]
            final_m1 = other[:]
            if oil_gold and is_price_news(text, source):
                final_m1 += oil_gold
            if final_m1:
                remember_title(title)
                await send(bot1, BOT1_CHAT_ID, build_message(title, source, link, final_m1))
                sent1 += 1
                stats["bot1_sent"] += 1
                log.info(f"[BOT1] {source}: {final_m1[:3]}")

        # БОТ 2 — Counterparty Risk
        m2 = match_keywords(text, BOT2_KEYWORDS)
        if m2:
            remember_title(title)
            await send(bot2, BOT2_CHAT_ID, build_message(title, source, link, m2))
            sent2 += 1
            stats["bot2_sent"] += 1
            log.info(f"[BOT2] {source}: {m2[:3]}")

    stats["checks"] += 1
    stats["posts_seen"] = len(seen_ids)
    log.info(f"✅ новых={new_total} дублей={skipped_dup} | BOT1={sent1} BOT2={sent2} | памяти={len(seen_ids)}")


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    bot1 = Bot(token=BOT1_TOKEN)
    bot2 = Bot(token=BOT2_TOKEN)

    log.info("🤖 Risk Management News + Counterparty Risk News")
    log.info(f"📡 Telegram: {len(TELEGRAM_CHANNELS)} каналов | RSS: {len(RSS_FEEDS)} лент")
    log.info(f"🔑 BOT1: {len(BOT1_KEYWORDS)} слов | BOT2: {len(BOT2_KEYWORDS)} слов")

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (RSS Reader Bot)"},
        follow_redirects=True,
    ) as http:
        log.info("⏳ Загружаю историю (без отправки)...")
        posts = await collect_all_posts(http)
        for p in posts:
            seen_ids.add(item_id(p["source"], p["link"], p["title"]))
            remember_title(p["title"])
        log.info(f"✅ Загружено {len(seen_ids)} постов. Жду новые...")

        while True:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await check_once(bot1, bot2, http)


if __name__ == "__main__":
    threading.Thread(target=start_ping_server, daemon=True).start()
    asyncio.run(main())
