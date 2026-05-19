"""
Telegram News Filter Bot — БЕЗОПАСНАЯ версия (RSS + Telegram каналы)
=====================================================================
Два бота:
  Бот 1 → Risk Management News
  Бот 2 → Counterparty Risk News

Переменные (только 4):
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

# ─── RSSHub инстансы — твой личный первый! ───────────────────────────────────

RSSHUB_INSTANCES = [
    "https://rsshub-news.vercel.app",   # ← твой личный (главный)
    "https://rsshub.rssforever.com",    # запасной 1
    "https://rss.shab.fun",             # запасной 2
    "https://hub.slarker.me",           # запасной 3
]

# ─── Telegram каналы ──────────────────────────────────────────────────────────

TELEGRAM_CHANNELS = [
    "asiaplus",
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

# ─── Прямые RSS ленты ─────────────────────────────────────────────────────────

RSS_FEEDS = [
    # 🇹🇯 Таджикистан
    {"url": "https://tj.sputniknews.ru/export/rss2/archive/index.xml", "name": "Sputnik TJ"},
    {"url": "https://asiaplustj.info/ru/rss",                    "name": "Asia-Plus"},
    {"url": "https://avesta.tj/feed",                            "name": "Авеста"},
    {"url": "https://khovar.tj/feed",                            "name": "Ховар"},
    # 🇷🇺 СНГ / Россия
    {"url": "https://tass.ru/rss/v2.xml",                        "name": "ТАСС"},
    {"url": "https://www.kommersant.ru/RSS/news.xml",            "name": "Коммерсантъ"},
    {"url": "https://www.interfax.ru/rss.asp",                   "name": "Интерфакс"},
    # 🌍 Мировые
    {"url": "https://cabar.asia/feed",                           "name": "CABAR Asia"},
    {"url": "https://www.imf.org/en/News/rss",                   "name": "МВФ"},
    {"url": "https://oilprice.com/rss/main",                     "name": "OilPrice"},
    {"url": "https://www.mining.com/feed/",                      "name": "Mining.com"},
    {"url": "https://financialpost.com/feed",                    "name": "Financial Post"},
]

# ─── Ключевые слова ───────────────────────────────────────────────────────────

BOT1_KEYWORDS = [
    # 🔴 САНКЦИИ И РЕГУЛЯТОРНЫЕ РИСКИ
    "экономические санкции", "санкционный список", "SDN", "OFAC", "блокировка активов", "финансовые санкции", "торговые санкции", "международные экономические санкции", "введены санкции",   
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
    "интервенция ЦБ", "валютная интервенция", "нормативы регулятора", "банковский надзор", "проблемные кредиты", "банковский регулятор",
    # 🟡 МАКРОЭКОНОМИКА
    "инфляция в Таджикистан", "девальвация сомони", "финансовый кризис", "рецессия", "девальвация доллар", "уровень инфляции в Таджикистан", "ставка рефинансирования Таджикистан", "ослабление сомони", "сомони к доллару",
    "валютный риск", "дефицит бюджета", "внешний долг", "валютный рынок Таджикистан",
    "ослабление валюты", "укрепление валюты", "фондовый рынок",
    "денежный перевод мигрантов", "ВВП Таджикистана", "фискальная политика", "экономика Таджикистана", "банковская система", "банковские системы", "банковской системы", "банковской системы",
    # 🟢 РЫНКИ — только движение цен
    "нефть подорожала", "нефть подешевела", "цена нефти", "экономический кризис", "AML", "Антифрод",
    "нефть выросла", "нефть упала", "нефть резко",
    "золото подорожало", "золото подешевело", "цена золота",
    "золото выросла", "золото упало",
    "курс доллара", "курс евро", "курс рубля", "курс юаня",
    # 🇬🇧 ENGLISH
    "sanctions", "default", "bankruptcy", "financial crisis",
    "Tajikistan inflation", "devaluation", "interest rate", "central bank", "target inflation in Tajikistan",
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
    "Сбербанк", "Сбер ", "Sberbank", "СберБанк",
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
    "Банк ЦентрКредит", "ЦентрКредит", "BCC (Bank CenterCredit)", "CenterCredit",
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
    "нархи нафт", "арзиши нафт", "Urals at $"
    "золото подорожало", "золото подешевело", "цена золота",
    "золото выросло", "золото упало",
    "gold prices fall", "gold prices rise", "gold prices drop",
    "нархи тилло",
]
OIL_GOLD_SOURCES = {"OilPrice", "Mining.com", "Kitco"}
OIL_GOLD_GENERAL = ["нефт", "золот", "oil", "gold", "barrel", "brent", "wti", "crude", "нафт", "тилло"]


def is_price_news(text: str, source: str) -> bool:
    if source in OIL_GOLD_SOURCES:
        return True
    low = text.lower()
    return any(kw.lower() in low for kw in OIL_GOLD_PRICE_WORDS)


# ─── Дедупликация ─────────────────────────────────────────────────────────────

recent_titles: list = []
RECENT_TITLES_MAX = 500
SIMILARITY_THRESHOLD = 0.80


def is_duplicate_title(title: str) -> bool:
    if not title:
        return False
    title_low = title.lower().strip()
    for prev in recent_titles:
        if SequenceMatcher(None, title_low, prev).ratio() >= SIMILARITY_THRESHOLD:
            return True
    return False


def remember_title(title: str):
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


# ─── Ping сервер ──────────────────────────────────────────────────────────────

stats = {"bot1_sent": 0, "bot2_sent": 0, "checks": 0, "posts_seen": 0}


class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        if self.path == "/status":
            import json
            self.wfile.write(json.dumps(stats).encode())
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


async def fetch_telegram_channel(client: httpx.AsyncClient, channel: str) -> list:
    """Пробует все rsshub инстансы по очереди."""
    for instance in RSSHUB_INSTANCES:
        url = f"{instance}/telegram/channel/{channel}"
        try:
            r = await client.get(url, timeout=20)
            if r.status_code == 200:
                posts = []
                try:
                    root = ET.fromstring(r.content)
                    for item in root.findall(".//item"):
                        title = strip_html(item.findtext("title") or "")
                        link  = (item.findtext("link") or "").strip()
                        desc  = strip_html(item.findtext("description") or "")
                        posts.append({
                            "source": f"@{channel}",
                            "title":  title,
                            "link":   link,
                            "text":   f"{title} {desc}".strip(),
                        })
                    log.info(f"✓ @{channel} → {instance} ({len(posts)} постов)")
                    return posts
                except ET.ParseError:
                    continue
        except Exception:
            continue
    log.warning(f"✗ @{channel}: все инстансы недоступны")
    return []


async def collect_all_posts(http: httpx.AsyncClient) -> list:
    tasks = []
    for ch in TELEGRAM_CHANNELS:
        tasks.append(fetch_telegram_channel(http, ch))
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
        pid = item_id(post["source"], post["link"], post["title"])
        if pid in seen_ids:
            continue
        seen_ids.add(pid)

        title = post["title"] or post["text"][:120]
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
        headers={"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"},
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
