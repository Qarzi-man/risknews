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

ИЗМЕНЕНИЯ В ЭТОЙ ВЕРСИИ (строгая фильтрация):
  1. Убраны все английские ключевые слова (были источником шума —
     цепляли мировые новости, не относящиеся к региону).
  2. BOT1_KEYWORDS разделены на CRITICAL (всегда триггерят — санкции,
     дефолт, отзыв лицензии и т.п.) и CONTEXT (общие макро-термины —
     курс валют, ставка ЦБ, инфляция), которые триггерят ТОЛЬКО если
     в тексте также упоминается регион (Таджикистан/СНГ/Центральная
     Азия) — иначе это может быть новость о любой стране мира.
  3. BOT2 теперь требует ОДНОВРЕМЕННО: (а) название банка-контрагента
     И (б) риск/негативное слово (санкции, банкротство, отток
     депозитов, уголовное дело и т.п.). Просто упоминание банка без
     негативного контекста больше не отправляется.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import signal
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

# ─── Персистентное состояние (переживает передеплой) ─────────────────────────
# RAILWAY_VOLUME_MOUNT_PATH выставляется автоматически, если в сервисе
# подключён Volume (Settings → Volumes → Add Volume, mount path напр. /data).
# Без подключённого Volume состояние будет жить только в /tmp этого контейнера
# и всё равно потеряется при следующем деплое — просто ничего не сломается.

STATE_DIR = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", os.environ.get("STATE_DIR", "/tmp"))
STATE_FILE = os.path.join(STATE_DIR, "newsbot_state.json")
MAX_SEEN_IDS = 8000  # ограничение, чтобы файл не рос бесконечно

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

# ─── Регион-маркеры (для контекстных ключевых слов BOT1) ─────────────────────
# Общие макро-термины (курс валют, ставка ЦБ и т.п.) триггерят только если
# в тексте ЕСТЬ хотя бы один из этих маркеров — иначе это может быть новость
# о любой стране мира, не относящаяся к зоне интересов банка.

REGION_MARKERS = [
    "Таджикистан", "Точикистон", "Тоҷикистон", "сомони", "Душанбе",
    "НБТ", "Национальный банк Таджикистана", "Бонки миллии Тоҷикистон",
    "СНГ", "Центральная Азия", "Центральной Азии",
    "Узбекистан", "Кыргызстан", "Киргизия", "Казахстан",
    "Россия", "росси", "рубл", "Беларусь", "Белоруссия",
    "Грузия", "Армения", "Азербайджан",
]


def has_region_context(text: str) -> bool:
    low = text.lower()
    return any(rm.lower() in low for rm in REGION_MARKERS)


# ─── Ключевые слова BOT1 — Risk Management ───────────────────────────────────
# CRITICAL — специфичные термины, всегда релевантны сами по себе.

BOT1_KEYWORDS_CRITICAL = [
    # 🔴 САНКЦИИ И РЕГУЛЯТОРНЫЕ РИСКИ
    "экономические санкции", "санкционный список", "блокировка активов",
    "финансовые санкции", "торговые санкции",
    "международные экономические санкции", "введены санкции",
    "отзыв лицензии", "приостановление лицензии", "регулятор ввёл запрет",
    "запрет на операции", "блокировка счетов", "заморозка активов",
    "ограничения на переводы", "запрет на снятие средств",
    # 🔴 БАНКРОТСТВО И ДЕФОЛТ
    "дефолт", "банкротство", "ликвидация банка", "несостоятельность",
    "временная администрация", "санация банка",
    # 🟡 БАНКОВСКИЙ РИСК
    "просрочка кредитов", "проблемный кредит", "проблемные кредиты",
    "реструктуризация кредитов", "кредитный риск", "рыночный риск",
    "операционный риск", "риск ликвидности", "достаточность капитала",
    "стресс-тест", "резервы под потери", "отток депозитов",
    "дефицит капитала", "нормативы ЦБ", "нормативы регулятора",
    "банковский надзор", "банковский регулятор", "антифрод",
    # 🟡 ТАДЖИКИСТАН-СПЕЦИФИЧНЫЕ (уже со страновой привязкой)
    "инфляция в Таджикистан", "девальвация сомони",
    "уровень инфляции в Таджикистан", "ставка рефинансирования Таджикистан",
    "ослабление сомони", "сомони к доллару", "валютный рынок Таджикистан",
    "денежный перевод мигрантов", "ВВП Таджикистана",
    "экономика Таджикистана", "банковская система",
    "Нацбанк Таджикистана", "Национальный банк Таджикистана",
    # 🟢 РЫНКИ — движение цен (нефть/золото), источник уточняется отдельно
    "нефть подорожала", "нефть подешевела", "цена нефти",
    "нефть выросла", "нефть упала", "нефть резко",
    "золото подорожало", "золото подешевело", "цена золота",
    "золото выросла", "золото упало",
    # 🇹🇯 ТОҶИКӢ
    "таҳримҳо", "бӯҳрони молиявӣ", "муфлисшавӣ",
    "хавфи бонкӣ", "хавфи қарзӣ", "хавфи бозорӣ",
    "қарзи бад", "қарзи мушкил", "таҷдиди қарз", "беқурбшавӣ",
    "қарзҳои батаъхирафтода", "баромади пасандозҳо",
    "ноустувории қурб", "интиқоли пулии муҳоҷирон",
    "Бонки миллии Тоҷикистон", "шустушӯи пул",
    "нархи нафт", "арзиши нафт", "нархи тилло",
    "бозхонди иҷозатнома",
]

# CONTEXT — общие макро-термины: могут относиться к ЛЮБОЙ стране мира,
# поэтому триггерят только вместе с REGION_MARKERS.

BOT1_KEYWORDS_CONTEXT = [
    "ключевая ставка", "ставка рефинансирования", "процентная ставка",
    "денежно-кредитная политика", "центральный банк", "центробанк",
    "интервенция ЦБ", "валютная интервенция",
    "финансовый кризис", "экономический кризис", "рецессия",
    "валютный риск", "дефицит бюджета", "внешний долг",
    "ослабление валюты", "укрепление валюты", "фондовый рынок",
    "фискальная политика",
    "курс доллара", "курс евро", "курс рубля", "курс юаня",
    "қурби асъор", "таваррум",
]

# ─── Ключевые слова BOT2 — Counterparty Risk ─────────────────────────────────
# Названия банков (латиница оставлена намеренно — это имена собственные,
# нужны для распознавания в новостях, где банк упомянут на английском).

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

# RISK-слова: новость по банку-контрагенту отправляется, только если рядом
# есть хотя бы одно из этих слов — то есть новость именно о проблеме/риске,
# а не нейтральное/позитивное упоминание банка.

BOT2_RISK_KEYWORDS = [
    "санкции", "санкционный список", "под санкциями",
    "дефолт", "банкротство", "ликвидация", "несостоятельность",
    "отзыв лицензии", "приостановление лицензии", "приостановка операций",
    "отток депозитов", "заморозка активов", "блокировка счетов",
    "уголовное дело", "обыски", "арест счетов", "арестован",
    "обвинение", "мошенничество", "отмывание денег", "расследование",
    "штраф", "оштрафовал", "иск", "судебный процесс", "суд",
    "скандал", "крах", "коллапс", "убытки", "чистый убыток",
    "снижение рейтинга", "понижение рейтинга", "отозвал рейтинг",
    "кризис ликвидности", "невыполнение обязательств",
    "задержка платежей", "просрочка", "дефицит капитала",
    "утечка данных", "кибератака", "взлом системы",
    "временная администрация", "санация", "проверка ЦБ",
    "нарушение", "предписание регулятора",
]

# ─── Фильтр нефть/золото — только новости о движении цены ────────────────────

OIL_GOLD_SOURCES = {"OilPrice", "Mining.com", "Kitco"}
OIL_GOLD_GENERAL = ["нефт", "золот", "нафт", "тилло"]


def is_price_news(text: str, source: str) -> bool:
    if source in OIL_GOLD_SOURCES:
        return True
    low = text.lower()
    return any(kw.lower() in low for kw in [
        "нефть подорожала", "нефть подешевела", "цена нефти",
        "нефть выросла", "нефть упала", "нефть резко",
        "золото подорожало", "золото подешевело", "цена золота",
        "золото выросло", "золото упало",
        "нархи нафт", "арзиши нафт", "нархи тилло",
    ])


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


# ─── Антипозитивный фильтр ─────────────────────────────────────────────────────
# Если risk-слово нашлось, но рядом есть одна из этих фраз — новость на самом
# деле позитивная ("избежал дефолта", "признан лучшим банком" и т.п.), и её
# отправлять не нужно, хотя формальное совпадение по ключевым словам есть.

EXCLUDE_PHRASES = [
    "избежал", "избежала", "избежали", "удалось избежать",
    "предотвратил", "предотвратила", "предотвратили", "не допустил",
    "успешно погасил", "успешно погасила", "полностью погасил",
    "полностью погасила", "досрочно погасил", "досрочно погасила",
    "признан лучшим", "признан лучшей", "признана лучшей",
    "получил награду", "получила награду", "занял первое место",
    "заняла первое место", "вошёл в топ", "вошла в топ",
    "повысил рейтинг", "улучшил рейтинг", "рейтинг повышен",
    "рейтинг улучшен", "прогноз улучшен", "прогноз пересмотрен в сторону улучшения",
    "успешно завершил", "успешно завершила", "опроверг информацию",
    "опровергла информацию", "не подтвердил", "не подтвердила",
    "нет оснований для", "снял санкции", "сняты санкции",
    "исключён из санкционного списка", "исключена из санкционного списка",
]


def has_exclude_phrase(text: str) -> bool:
    low = text.lower()
    return any(p.lower() in low for p in EXCLUDE_PHRASES)


# ─── Логика отбора для BOT1 (строгая) ─────────────────────────────────────────

def bot1_final_matches(text: str, source: str) -> list:
    matched_critical = match_keywords(text, BOT1_KEYWORDS_CRITICAL)
    matched_context = match_keywords(text, BOT1_KEYWORDS_CONTEXT)

    # Контекстные (общие) слова триггерят только вместе с регион-маркером
    if matched_context and not has_region_context(text):
        matched_context = []

    # Нефть/золото — только если это реально новость о цене
    oil_gold = [kw for kw in matched_critical if any(w in kw.lower() for w in OIL_GOLD_GENERAL)]
    other_critical = [kw for kw in matched_critical if kw not in oil_gold]

    final = other_critical + matched_context
    if oil_gold and is_price_news(text, source):
        final += oil_gold

    if final and has_exclude_phrase(text):
        return []

    return final


# ─── Логика отбора для BOT2 (только риски/негатив) ────────────────────────────

def bot2_final_matches(text: str) -> list:
    banks = match_keywords(text, BOT2_KEYWORDS)
    if not banks:
        return []
    risks = match_keywords(text, BOT2_RISK_KEYWORDS)
    if not risks:
        return []
    if has_exclude_phrase(text):
        return []
    return banks[:3] + risks[:3]


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

seen_ids_set: set = set()
seen_ids_order: list = []


def remember_id(pid: str):
    if pid in seen_ids_set:
        return
    seen_ids_set.add(pid)
    seen_ids_order.append(pid)
    if len(seen_ids_order) > MAX_SEEN_IDS:
        old = seen_ids_order.pop(0)
        seen_ids_set.discard(old)


def load_state() -> bool:
    """Возвращает True, если состояние успешно восстановлено с диска."""
    global seen_ids_set, seen_ids_order, recent_titles
    if not os.path.exists(STATE_FILE):
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        seen_ids_order = list(data.get("seen_ids", []))
        seen_ids_set = set(seen_ids_order)
        recent_titles = list(data.get("recent_titles", []))
        log.info(f"💾 Состояние восстановлено: {len(seen_ids_set)} id, {len(recent_titles)} заголовков")
        return True
    except Exception as e:
        log.warning(f"Не удалось прочитать {STATE_FILE}: {e}")
        return False


def save_state():
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        tmp_path = STATE_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"seen_ids": seen_ids_order, "recent_titles": recent_titles}, f)
        os.replace(tmp_path, STATE_FILE)
    except Exception as e:
        log.warning(f"Не удалось сохранить {STATE_FILE}: {e}")


def _handle_shutdown(signum, frame):
    log.info(f"⏹️ Получен сигнал {signum} — сохраняю состояние перед остановкой...")
    save_state()
    os._exit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


async def check_once(bot1: Bot, bot2: Bot, http: httpx.AsyncClient):
    all_posts = await collect_all_posts(http)
    sent1 = sent2 = new_total = skipped_dup = 0

    for post in all_posts:
        pid = item_id(post["source"], post["link"], post["title"])
        if pid in seen_ids_set:
            continue
        remember_id(pid)

        title = post["title"] or post["text"][:120]
        if is_duplicate_title(title):
            skipped_dup += 1
            continue

        new_total += 1
        text   = post["text"]
        source = post["source"]
        link   = post["link"]

        # БОТ 1 — Risk Management (строгий отбор с регион-контекстом)
        final_m1 = bot1_final_matches(text, source)
        if final_m1:
            remember_title(title)
            await send(bot1, BOT1_CHAT_ID, build_message(title, source, link, final_m1))
            sent1 += 1
            stats["bot1_sent"] += 1
            log.info(f"[BOT1] {source}: {final_m1[:3]}")

        # БОТ 2 — Counterparty Risk (только банк + риск-слово вместе)
        final_m2 = bot2_final_matches(text)
        if final_m2:
            remember_title(title)
            await send(bot2, BOT2_CHAT_ID, build_message(title, source, link, final_m2))
            sent2 += 1
            stats["bot2_sent"] += 1
            log.info(f"[BOT2] {source}: {final_m2[:3]}")

    stats["checks"] += 1
    stats["posts_seen"] = len(seen_ids_set)
    log.info(f"✅ новых={new_total} дублей={skipped_dup} | BOT1={sent1} BOT2={sent2} | памяти={len(seen_ids_set)}")
    save_state()


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    bot1 = Bot(token=BOT1_TOKEN)
    bot2 = Bot(token=BOT2_TOKEN)

    log.info("🤖 Risk Management News + Counterparty Risk News")
    log.info(f"📡 Telegram: {len(TELEGRAM_CHANNELS)} каналов | RSS: {len(RSS_FEEDS)} лент")
    log.info(f"🔑 BOT1: {len(BOT1_KEYWORDS_CRITICAL) + len(BOT1_KEYWORDS_CONTEXT)} слов "
             f"({len(BOT1_KEYWORDS_CRITICAL)} critical / {len(BOT1_KEYWORDS_CONTEXT)} context) "
             f"| BOT2: {len(BOT2_KEYWORDS)} банков + {len(BOT2_RISK_KEYWORDS)} risk-слов")

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"},
        follow_redirects=True,
    ) as http:
        restored = load_state()
        if not restored:
            log.info("⏳ Состояние не найдено (первый запуск) — загружаю историю без отправки...")
            posts = await collect_all_posts(http)
            for p in posts:
                remember_id(item_id(p["source"], p["link"], p["title"]))
                remember_title(p["title"])
            save_state()
            log.info(f"✅ Загружено {len(seen_ids_set)} постов. Жду новые...")
        else:
            log.info("🔁 Состояние восстановлено после перезапуска — повторной отправки не будет.")

        while True:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await check_once(bot1, bot2, http)


if __name__ == "__main__":
    threading.Thread(target=start_ping_server, daemon=True).start()
    asyncio.run(main())
