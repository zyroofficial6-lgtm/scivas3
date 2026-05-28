# KY-SHIRO API — Multi-Account iVAS SMS
# Developer: Kiki Faizal

from flask import Flask, request, jsonify, Response
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import logging
import os
import gzip
import re
import random
import threading
import time
import json
import html as html_lib
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load .env otomatis kalau ada
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


_LOG_LEVEL = logging.DEBUG if os.getenv("DEBUG","0")=="1" else logging.INFO
logging.basicConfig(level=_LOG_LEVEL, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)
for _lib in ('urllib3','requests','werkzeug'):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# UTF-8 patch — fix 'latin-1 codec' error dari requests
import http.client as _http_client
_orig_putheader = _http_client.HTTPConnection.putheader
def _utf8_putheader(self, header, *values):
    values = tuple(
        v.encode('utf-8').decode('latin-1', errors='replace') if isinstance(v, str) else v
        for v in values
    )
    return _orig_putheader(self, header, *values)
_http_client.HTTPConnection.putheader = _utf8_putheader

# ── Simple in-memory response cache untuk endpoint heavy ──
import hashlib as _hashlib
_resp_cache      = {}
_resp_cache_lock = threading.Lock()
_RESP_CACHE_TTL  = 30   # detik — cache response /numbers/my-list dll

# ── Cache TTL untuk fetch iVAS (get_ranges / get_numbers / get_sms) ──────────
# Mencegah spam request ke iVAS saat polling diff
# WS /livesms tetap realtime — cache ini hanya untuk polling fallback
_IVAS_RANGES_TTL  = 300   # 5 menit  — ranges jarang berubah
_IVAS_NUMBERS_TTL = 300   # 5 menit  — nomor jarang berubah
_IVAS_SMS_TTL     = 0     # 0 = selalu fresh dari iVAS (polling worker flush sendiri)
_ivas_cache       : dict = {}
_ivas_cache_lock  = threading.Lock()

def _ivas_cache_get(key: str, ttl: int):
    with _ivas_cache_lock:
        entry = _ivas_cache.get(key)
        if entry and (time.time() - entry["ts"]) < ttl:
            return entry["data"], True   # (data, hit)
        return None, False

def _ivas_cache_set(key: str, data):
    with _ivas_cache_lock:
        _ivas_cache[key] = {"data": data, "ts": time.time()}
        if len(_ivas_cache) > 500:
            oldest = sorted(_ivas_cache.items(), key=lambda x: x[1]["ts"])
            for k, _ in oldest[:100]:
                _ivas_cache.pop(k, None)

def _ivas_cache_invalidate(prefix: str = ""):
    with _ivas_cache_lock:
        keys = [k for k in _ivas_cache if not prefix or k.startswith(prefix)]
        for k in keys:
            _ivas_cache.pop(k, None)

def _cache_get(key: str):
    """Ambil dari cache kalau masih fresh."""
    with _resp_cache_lock:
        entry = _resp_cache.get(key)
        if entry and (time.time() - entry["ts"]) < _RESP_CACHE_TTL:
            return entry["data"]
        return None

def _cache_set(key: str, data):
    """Simpan ke cache."""
    with _resp_cache_lock:
        _resp_cache[key] = {"data": data, "ts": time.time()}
        # Trim cache kalau terlalu besar
        if len(_resp_cache) > 200:
            oldest = sorted(_resp_cache.items(), key=lambda x: x[1]["ts"])
            for k, _ in oldest[:50]:
                _resp_cache.pop(k, None)

def _cache_invalidate(prefix: str = ""):
    """Hapus cache (panggil setelah add/del number)."""
    with _resp_cache_lock:
        keys = [k for k in _resp_cache if k.startswith(prefix)]
        for k in keys:
            _resp_cache.pop(k, None)
    if keys:
        logger.debug(f"[CACHE] Invalidated {len(keys)} keys (prefix={prefix!r})")


# ── Request Coalescing — cegah duplicate request ke iVAS ──────────────
# Kalau 10 request bersamaan dengan key sama, hanya 1 yang jalan ke iVAS
# Sisanya tunggu hasilnya. Ini mencegah spam ke iVAS.
import concurrent.futures as _cf

_inflight_lock   = threading.Lock()
_inflight: dict  = {}  # key → Future

def _coalesced(key: str, fn):
    """
    Jalankan fn() hanya sekali per key yang sedang in-flight.
    Request lain dengan key sama akan tunggu hasil yang pertama.
    """
    with _inflight_lock:
        fut = _inflight.get(key)
        if fut is not None:
            # Ada request yang sedang berjalan — tunggu hasilnya
            is_first = False
        else:
            # Kita yang pertama — buat future dan daftarkan
            fut = _cf.Future()
            _inflight[key] = fut
            is_first = True

    if not is_first:
        logger.debug(f"[COALESCE] Menunggu inflight request: {key}")
        try:
            return fut.result(timeout=180)
        except Exception as e:
            raise e

    # Kita yang jalan — eksekusi fn()
    try:
        result = fn()
        with _inflight_lock:
            _inflight.pop(key, None)
        fut.set_result(result)
        return result
    except Exception as e:
        with _inflight_lock:
            _inflight.pop(key, None)
        fut.set_exception(e)
        raise

# ════════════════════════════════════════════════════════
# TELEGRAM NOTIFIER
# Kirim OTP/SMS masuk ke group Telegram secara real-time
#
# Config via env var:
#   TG_BOT_TOKEN  — token bot dari @BotFather  (WAJIB)
#   TG_CHAT_IDS   — chat/group ID, pisah koma  (WAJIB)
#                   contoh: "-1001234567890,-1009876543210"
#
# Fitur:
#   - Auto extract OTP dari isi pesan
#   - Detect service (WhatsApp, Telegram, dll)
#   - Flag negara dari range name
#   - Sensor nomor (privacy)
#   - Deduplikasi — pesan yang sama tidak dikirim 2x
#   - Non-blocking (pakai thread pool)
# ════════════════════════════════════════════════════════

_TG_BOT_TOKEN : str       = os.getenv("TG_BOT_TOKEN", "8783291753:AAGw99YkxfQ6p-dVY2T5c_HhsDlmW3bY9fM").strip()
_TG_CHAT_IDS  : list[str] = [c.strip() for c in os.getenv("TG_CHAT_IDS", "-1003917796517").split(",") if c.strip()]
_TG_ENABLED   : bool      = bool(_TG_BOT_TOKEN and _TG_CHAT_IDS)

# Dedup cache — hindari kirim pesan yang sama 2x
_tg_sent_lock  = threading.Lock()
_TG_SENT_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tg_sent.json")

def _load_sent_ids() -> set:
    """Load sent IDs dari file JSON (persist across restart)."""
    try:
        if os.path.exists(_TG_SENT_FILE):
            import json as _json
            with open(_TG_SENT_FILE, "r") as _f:
                data = _json.load(_f)
                return set(data) if isinstance(data, list) else set()
    except Exception:
        pass
    return set()

def _save_sent_ids(ids: set):
    """Simpan sent IDs ke file JSON."""
    try:
        import json as _json
        # Simpan max 5000 entry terbaru
        items = list(ids)[-5000:]
        with open(_TG_SENT_FILE, "w") as _f:
            _json.dump(items, _f)
    except Exception as e:
        logger.warning(f"[TG] Gagal simpan sent IDs: {e}")

_tg_sent_cache : set  = _load_sent_ids()
_tg_sent_lock  = threading.Lock()
_TG_CACHE_MAX  = 5000

# ── Fast Telegram queue — instant send, no inter-message delay ───────────────
# 429 ditangani dengan non-blocking re-enqueue di _send_one
_TG_RETRY_MAX  = 3              # max retry saat timeout
_tg_queue      = []
_tg_queue_lock = threading.Lock()
_tg_queue_event = threading.Event()

def _tg_queue_worker():
    """Worker tunggal: kirim antrean TG secepat mungkin, tanpa sleep antar pesan."""
    while True:
        _tg_queue_event.wait()
        _tg_queue_event.clear()
        while True:
            with _tg_queue_lock:
                if not _tg_queue:
                    break
                task = _tg_queue.pop(0)
            text_t, otp_t = task
            _tg_send_blocking(text_t, otp_t)
            # ⚡ No sleep — kirim langsung pesan berikutnya

_tg_worker_thread = threading.Thread(target=_tg_queue_worker, daemon=True, name="tg_queue_worker")
_tg_worker_thread.start()

def _tg_enqueue(text: str, otp_code: str | None):
    """Tambah pesan ke antrian TG (thread-safe)."""
    with _tg_queue_lock:
        _tg_queue.append((text, otp_code))
    _tg_queue_event.set()

_TG_COUNTRY_FLAGS = {
    # A
    "Afghanistan":"🇦🇫","Albania":"🇦🇱","Algeria":"🇩🇿","Andorra":"🇦🇩","Angola":"🇦🇴",
    "Antigua And Barbuda":"🇦🇬","Argentina":"🇦🇷","Armenia":"🇦🇲","Australia":"🇦🇺",
    "Austria":"🇦🇹","Azerbaijan":"🇦🇿",
    # B
    "Bahamas":"🇧🇸","Bahrain":"🇧🇭","Bangladesh":"🇧🇩","Barbados":"🇧🇧","Belarus":"🇧🇾",
    "Belgium":"🇧🇪","Belize":"🇧🇿","Benin":"🇧🇯","Bhutan":"🇧🇹","Bolivia":"🇧🇴",
    "Bosnia And Herzegovina":"🇧🇦","Botswana":"🇧🇼","Brazil":"🇧🇷","Brunei":"🇧🇳",
    "Bulgaria":"🇧🇬","Burkina Faso":"🇧🇫","Burundi":"🇧🇮",
    # C
    "Cambodia":"🇰🇭","Cameroon":"🇨🇲","Canada":"🇨🇦","Cape Verde":"🇨🇻",
    "Central African Republic":"🇨🇫","Chad":"🇹🇩","Chile":"🇨🇱","China":"🇨🇳",
    "Colombia":"🇨🇴","Comoros":"🇰🇲","Congo":"🇨🇬","Costa Rica":"🇨🇷","Croatia":"🇭🇷",
    "Cuba":"🇨🇺","Cyprus":"🇨🇾","Czech Republic":"🇨🇿",
    # D
    "Denmark":"🇩🇰","Djibouti":"🇩🇯","Dominican Republic":"🇩🇴",
    # E
    "Ecuador":"🇪🇨","Egypt":"🇪🇬","El Salvador":"🇸🇻","Equatorial Guinea":"🇬🇶",
    "Eritrea":"🇪🇷","Estonia":"🇪🇪","Eswatini":"🇸🇿","Ethiopia":"🇪🇹",
    # F
    "Fiji":"🇫🇯","Finland":"🇫🇮","France":"🇫🇷",
    # G
    "Gabon":"🇬🇦","Gambia":"🇬🇲","Georgia":"🇬🇪","Germany":"🇩🇪","Ghana":"🇬🇭",
    "Greece":"🇬🇷","Grenada":"🇬🇩","Guatemala":"🇬🇹","Guinea":"🇬🇳",
    "Guinea Bissau":"🇬🇼","Guyana":"🇬🇾",
    # H
    "Haiti":"🇭🇹","Honduras":"🇭🇳","Hong Kong":"🇭🇰","Hungary":"🇭🇺",
    # I
    "Iceland":"🇮🇸","India":"🇮🇳","Indonesia":"🇮🇩","Iran":"🇮🇷","Iraq":"🇮🇶",
    "Ireland":"🇮🇪","Israel":"🇮🇱","Italy":"🇮🇹","Ivory Coast":"🇨🇮",
    # J
    "Jamaica":"🇯🇲","Japan":"🇯🇵","Jordan":"🇯🇴",
    # K
    "Kazakhstan":"🇰🇿","Kenya":"🇰🇪","Kosovo":"🇽🇰","Kuwait":"🇰🇼","Kyrgyzstan":"🇰🇬",
    # L
    "Laos":"🇱🇦","Latvia":"🇱🇻","Lebanon":"🇱🇧","Lesotho":"🇱🇸","Liberia":"🇱🇷",
    "Libya":"🇱🇾","Liechtenstein":"🇱🇮","Lithuania":"🇱🇹","Luxembourg":"🇱🇺",
    # M
    "Madagascar":"🇲🇬","Malawi":"🇲🇼","Malaysia":"🇲🇾","Maldives":"🇲🇻","Mali":"🇲🇱",
    "Malta":"🇲🇹","Mauritania":"🇲🇷","Mauritius":"🇲🇺","Mexico":"🇲🇽","Moldova":"🇲🇩",
    "Monaco":"🇲🇨","Mongolia":"🇲🇳","Montenegro":"🇲🇪","Morocco":"🇲🇦","Mozambique":"🇲🇿",
    "Myanmar":"🇲🇲",
    # N
    "Namibia":"🇳🇦","Nepal":"🇳🇵","Netherlands":"🇳🇱","New Zealand":"🇳🇿",
    "Nicaragua":"🇳🇮","Niger":"🇳🇪","Nigeria":"🇳🇬","North Korea":"🇰🇵",
    "North Macedonia":"🇲🇰","Norway":"🇳🇴",
    # O
    "Oman":"🇴🇲",
    # P
    "Pakistan":"🇵🇰","Palestine":"🇵🇸","Panama":"🇵🇦","Papua New Guinea":"🇵🇬",
    "Paraguay":"🇵🇾","Peru":"🇵🇪","Philippines":"🇵🇭","Poland":"🇵🇱","Portugal":"🇵🇹",
    # Q
    "Qatar":"🇶🇦",
    # R
    "Romania":"🇷🇴","Russia":"🇷🇺","Rwanda":"🇷🇼",
    # S
    "Saudi Arabia":"🇸🇦","Senegal":"🇸🇳","Serbia":"🇷🇸","Sierra Leone":"🇸🇱",
    "Singapore":"🇸🇬","Slovakia":"🇸🇰","Slovenia":"🇸🇮","Somalia":"🇸🇴",
    "South Africa":"🇿🇦","South Korea":"🇰🇷","South Sudan":"🇸🇸","Spain":"🇪🇸",
    "Sri Lanka":"🇱🇰","Sudan":"🇸🇩","Suriname":"🇸🇷","Sweden":"🇸🇪","Switzerland":"🇨🇭",
    "Syria":"🇸🇾",
    # T
    "Taiwan":"🇹🇼","Tajikistan":"🇹🇯","Tanzania":"🇹🇿","Thailand":"🇹🇭","Timor Leste":"🇹🇱",
    "Togo":"🇹🇬","Trinidad And Tobago":"🇹🇹","Tunisia":"🇹🇳","Turkey":"🇹🇷",
    "Turkmenistan":"🇹🇲",
    # U
    "Uganda":"🇺🇬","Ukraine":"🇺🇦","United Arab Emirates":"🇦🇪","United Kingdom":"🇬🇧",
    "United States":"🇺🇸","Uruguay":"🇺🇾","Uzbekistan":"🇺🇿",
    # V
    "Venezuela":"🇻🇪","Vietnam":"🇻🇳",
    # Y Z
    "Yemen":"🇾🇪","Zambia":"🇿🇲","Zimbabwe":"🇿🇼",
    # Unknown fallback
    "Unknown":"🌐",
}

# ── Platform emoji map ───────────────────────────────────────────────────────
_TG_PLATFORM_EMOJI = {
    "WhatsApp":         "💬",
    "WhatsApp Business":"🏢",
    "Telegram":         "✈️",
    "Facebook":         "👤",
    "Instagram":        "📷",
    "Google":           "🔍",
    "Gmail":            "📧",
    "Amazon":           "📦",
    "Netflix":          "🎬",
    "Microsoft":        "🪟",
    "Apple":            "🍎",
    "Twitter":          "🐦",
    "TikTok":           "🎵",
    "Discord":          "🎧",
    "Snapchat":         "👻",
    "LinkedIn":         "💼",
    "Pinterest":        "📌",
    "Reddit":           "👽",
    "Spotify":          "🎶",
    "Yahoo":            "🟣",
    "Grab":             "🚗",
    "Gojek":            "🛵",
    "Shopee":           "🛒",
    "Tokopedia":        "🏪",
    "Lazada":           "🛍️",
    "OVO":              "💜",
    "Dana":             "💙",
    "GoPay":            "💚",
    "Uber":             "🚕",
    "Airbnb":           "🏠",
    "PayPal":           "💳",
    "Binance":          "🪙",
    "Bybit":            "📊",
    "Coinbase":         "🔵",
    "Steam":            "🕹️",
    "Roblox":           "🎮",
    "Line":             "💚",
    "WeChat":           "🟢",
    "Signal":           "🔒",
    "Viber":            "💜",
    "KakaoTalk":        "💛",
    "Unknown":          "📩",
}

# ── Language detect dari isi pesan ───────────────────────────────────────────
_TG_LANG_KEYWORDS = {
    "Indonesian": ["kode","anda","adalah","verifikasi","masukkan","gunakan","jangan",
                   "bagikan","dengan","untuk","nomor","akun","kedaluwarsa","selamat"],
    "Arabic":     ["رمز","التحقق","كلمة","المرور","حسابك","الرجاء","إدخال","لا تشارك",
                   "صالح","دقيقة","رسالة","تسجيل"],
    "French":     ["votre","code","est","vérification","entrez","ne partagez","compte",
                   "valide","minute","bonjour"],
    "Spanish":    ["código","verificación","ingresa","cuenta","válido","minutos","hola",
                   "no compartas","tu código"],
    "Portuguese": ["código","verificação","insira","conta","válido","minutos","olá",
                   "não compartilhe"],
    "Turkish":    ["kodunuz","doğrulama","hesabınız","girin","paylaşmayın","dakika"],
    "Russian":    ["код","подтверждения","аккаунт","введите","не сообщайте","минут"],
    "Hindi":      ["कोड","सत्यापन","खाता","दर्ज","साझा"],
    "Chinese":    ["验证码","账户","请勿","分享","有效"],
    "Japanese":   ["認証","コード","アカウント","入力","共有しないで"],
    "Korean":     ["인증","코드","계정","입력","공유하지"],
    "English":    ["your","code","verification","enter","account","valid","minutes",
                   "don't share","otp","password","expires"],
}

def _tg_detect_language(text: str) -> str:
    """Detect bahasa dari isi pesan."""
    lower = text.lower()
    scores = {}
    for lang, keywords in _TG_LANG_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in lower)
        if score > 0:
            scores[lang] = score
    if not scores:
        return "English"
    return max(scores, key=scores.get)

def _tg_get_flag(range_name: str) -> str:
    clean = re.sub(r"\d+", "", range_name).strip().title()
    if clean in _TG_COUNTRY_FLAGS:
        return _TG_COUNTRY_FLAGS[clean]
    for k, f in _TG_COUNTRY_FLAGS.items():
        if k.upper() == clean.upper():
            return f
    return "🌐"


def _tg_detect_service(text: str) -> tuple[str, str]:
    """Return (nama_service, emoji_service)."""
    lower = text.lower()
    kw = {
        "WhatsApp Business":["whatsapp business","wa business"],
        "WhatsApp":        ["whatsapp"],
        "Telegram":        ["telegram"],
        "Facebook":        ["facebook","fb"],
        "Instagram":       ["instagram"],
        "Gmail":           ["gmail"],
        "Google":          ["google"],
        "Amazon":          ["amazon"],
        "Netflix":         ["netflix"],
        "Microsoft":       ["microsoft","outlook","hotmail"],
        "Apple":           ["apple","icloud"],
        "Twitter":         ["twitter","x.com"],
        "TikTok":          ["tiktok"],
        "Discord":         ["discord"],
        "Snapchat":        ["snapchat"],
        "LinkedIn":        ["linkedin"],
        "Pinterest":       ["pinterest"],
        "Reddit":          ["reddit"],
        "Spotify":         ["spotify"],
        "Yahoo":           ["yahoo"],
        "Grab":            ["grab"],
        "Gojek":           ["gojek"],
        "Shopee":          ["shopee"],
        "Tokopedia":       ["tokopedia"],
        "Lazada":          ["lazada"],
        "OVO":             ["ovo"],
        "Dana":            ["dana wallet","dana.id"," dana "],
        "GoPay":           ["gopay"],
        "Uber":            ["uber"],
        "Airbnb":          ["airbnb"],
        "PayPal":          ["paypal"],
        "Binance":         ["binance"],
        "Bybit":           ["bybit"],
        "Coinbase":        ["coinbase"],
        "Steam":           ["steam"],
        "Roblox":          ["roblox"],
    }
    for svc, keys in kw.items():
        if any(k in lower for k in keys):
            emoji = _TG_PLATFORM_EMOJI.get(svc, "📩")
            return svc, emoji
    return "Unknown", "📩"


def _tg_extract_otp(text: str) -> str | None:
    dash = re.search(r"\b(\d{3}[-\u2013]\d{3})\b", text)
    if dash:
        return dash.group(1)
    letter = re.search(r"\b[A-Z]-(\d{4,8})\b", text)
    if letter:
        return letter.group(1)
    digits = re.search(r"\b(\d{4,8})\b", text)
    if digits:
        return digits.group(1)
    return None


def _tg_sensor(num: str) -> str:
    s = str(num)
    if len(s) <= 6:
        return s
    return f"{s[:4]}****{s[-4:]}"


def _tg_format_message(phone: str, message: str, range_name: str = "", sid: str = "", account: str = "") -> str:
    """Format pesan OTP — gaya Bulk SMS seperti referensi."""
    import html as _html

    # ── Data dasar ─────────────────────────────────────────────
    country    = re.sub(r"\d+", "", range_name).strip().title() or "Unknown"
    flag       = _tg_get_flag(range_name)
    otp        = _tg_extract_otp(message)
    otp_str    = otp if otp else "N/A"
    phone_safe = _tg_sensor(phone)
    now_wib    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Service & emoji ─────────────────────────────────────────
    svc_name, svc_emoji = _tg_detect_service(message)
    if sid and sid != "Unknown":
        # Cek kalau sid cocok dengan platform yang diketahui
        sid_lower = sid.lower()
        for svc, keys in {
            "WhatsApp Business":["whatsapp business"],
            "WhatsApp":["whatsapp"],"Telegram":["telegram"],
            "TikTok":["tiktok"],"Facebook":["facebook"],
            "Instagram":["instagram"],"Google":["google"],
            "Apple":["apple"],"Discord":["discord"],
        }.items():
            if any(k in sid_lower for k in keys):
                svc_name  = svc
                svc_emoji = _TG_PLATFORM_EMOJI.get(svc, "📩")
                break
        else:
            if svc_name == "Unknown":
                svc_name  = sid
                svc_emoji = "📩"

    # ── Bahasa auto-detect ──────────────────────────────────────
    lang = _tg_detect_language(message)

    # ── Format HTML ─────────────────────────────────────────────
    text = f"{flag} {country} | <code>{phone_safe}</code> | {svc_name} | 🌐 {lang}"
    return text


def _tg_send_blocking(text: str, otp_code: str | None):
    """Kirim pesan ke semua chat — dipanggil dari thread pool."""
    if not _TG_ENABLED:
        return
    url = f"https://api.telegram.org/bot{_TG_BOT_TOKEN}/sendMessage"
    # Button selalu ada: row1 = Copy OTP (kalau ada), row2 = Owner + Numbers
    kb_rows = []
    if otp_code:
        kb_rows.append([
            {"text": f" {otp_code}", "copy_text": {"text": otp_code}}
        ])
    kb_rows.append([
        {"text": "👤 Owner",   "url": "https://t.me/Shiroky1"},
        {"text": "📢 Numbers", "url": "https://t.me/numberchshiro"},
    ])
    kb = {"inline_keyboard": kb_rows}
    success_count = 0
    fail_count    = 0

    def _send_one(cid):
        nonlocal success_count, fail_count
        for attempt in range(1, _TG_RETRY_MAX + 1):
            try:
                r = requests.post(url, json={
                    "chat_id": cid, "text": text,
                    "parse_mode": "HTML", "reply_markup": kb,
                }, timeout=10)  # timeout lebih pendek supaya cepat

                if r.status_code == 200:
                    resp_data  = r.json()
                    msg_id     = resp_data.get("result", {}).get("message_id", "?")
                    chat_info  = resp_data.get("result", {}).get("chat", {})
                    chat_title = chat_info.get("title") or chat_info.get("first_name", cid)
                    otp_info   = f" | OTP: {otp_code}" if otp_code else ""
                    success_count += 1
                    logger.info(
                        f"[TG-SENT] ✅ Terkirim → {chat_title} ({cid})"
                        f" | msg_id: {msg_id}{otp_info}"
                    )
                    return  # sukses

                elif r.status_code == 429:
                    try:
                        retry_after = r.json().get("parameters", {}).get("retry_after", 3)
                    except Exception:
                        retry_after = 3
                    logger.warning(f"[TG-SENT] ⏳ 429 rate limit → {cid} | re-enqueue setelah {retry_after}s")
                    # Re-enqueue ke queue utama dengan delay — tidak blocking thread ini
                    def _delayed_retry(c=cid, t=text, o=otp_code, d=retry_after):
                        time.sleep(d)
                        _tg_enqueue(t, o)
                    threading.Thread(target=_delayed_retry, daemon=True).start()
                    return  # keluar, biarkan re-enqueue yang kirim ulang

                else:
                    fail_count += 1
                    try:
                        err_desc = r.json().get("description", r.text[:80])
                    except Exception:
                        err_desc = r.text[:80]
                    logger.warning(f"[TG-SENT] ❌ Gagal → {cid} | HTTP {r.status_code}: {err_desc}")
                    return

            except requests.exceptions.Timeout:
                logger.warning(f"[TG-SENT] ⏱️ Timeout → {cid} (attempt {attempt}/{_TG_RETRY_MAX})")
                # Langsung retry tanpa sleep
            except Exception as e:
                fail_count += 1
                logger.warning(f"[TG-SENT] ❌ Error → {cid} | {e}")
                return

        fail_count += 1
        logger.warning(f"[TG-SENT] ❌ Gagal {_TG_RETRY_MAX}x retry → {cid}")

    for cid in _TG_CHAT_IDS:
        _send_one(cid)

    logger.info(
        f"[TG-SENT] 📊 Hasil pengiriman — ✅ Berhasil: {success_count}"
        f" | ❌ Gagal: {fail_count} | Total target: {len(_TG_CHAT_IDS)} chat"
    )


def tg_notify(phone: str, message: str, range_name: str = "", sid: str = "", account: str = ""):
    """
    Kirim notif OTP ke Telegram (non-blocking).
    Dipanggil setiap kali SMS/OTP baru masuk.
    Otomatis skip duplikat.
    """
    if not _TG_ENABLED:
        logger.debug("[TG] Notifier dinonaktifkan (TG_BOT_TOKEN / TG_CHAT_IDS belum diset)")
        return

    if not message or not phone:
        logger.debug(f"[TG] Skip — phone atau message kosong (phone={phone!r}, msg={message!r})")
        return

    # Dedup key — sama dengan polling worker
    key = f"{phone}|{message[:80]}"
    with _tg_sent_lock:
        if key in _tg_sent_cache:
            logger.debug(f"[TG] Skip duplikat: {phone} | {message[:30]}")
            return
        _tg_sent_cache.add(key)
        # Trim kalau terlalu besar
        if len(_tg_sent_cache) > _TG_CACHE_MAX:
            _tg_sent_cache.difference_update(set(list(_tg_sent_cache)[:500]))
        _save_sent_ids(_tg_sent_cache)

    text        = _tg_format_message(phone, message, range_name, sid, account)
    otp         = _tg_extract_otp(message)
    service_log = sid or _tg_detect_service(message)
    country_log = re.sub(r"\d+", "", range_name).strip().title() or "Unknown"
    otp_log     = f" | OTP: {otp}" if otp else " | OTP: (tidak ditemukan)"

    logger.info(
        f"[TG-QUEUE] 📩 SMS masuk → nomor: {_tg_sensor(phone)}"
        f" | negara: {country_log} | service: {service_log}{otp_log}"
    )
    logger.info(
        f"[TG-QUEUE] 📤 Mengirim notifikasi ke {len(_TG_CHAT_IDS)} chat Telegram..."
    )

    _tg_enqueue(text, otp)


if _TG_ENABLED:
    logger.info(f"[TG] ✅ Notifier aktif — {len(_TG_CHAT_IDS)} grup, bot={_TG_BOT_TOKEN[:12]}...")
else:
    logger.warning("[TG] ⚠️  Notifier NONAKTIF — set TG_BOT_TOKEN + TG_CHAT_IDS di env")

# ════════════════════════════════════════════════════════
# CORS & DOMAIN CONFIG — Support custom domain & panel
# Domain: https://api.kyshiro.serverkicen.biz.id
# Panel: Pterodactyl / Railway / Render / dll
# ════════════════════════════════════════════════════════
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")  # * = semua domain

# ════════════════════════════════════════════════════════
# MULTI-ACCOUNT CONFIG
# Tambah akun baru cukup tambah dict baru di list ini
# Atau set env var: IVAS_ACCOUNTS = "email1:pass1,email2:pass2"
# ════════════════════════════════════════════════════════

def load_accounts():
    """
    Load daftar akun dari environment variable atau default.

    Priority:
    1. Env var IVAS_ACCOUNTS = "email1:pass1,email2:pass2,..."
       → dipakai kalau diset, TAMBAH ke default (tidak replace)
    2. Default 4 akun hardcoded di bawah

    PENTING: Jangan set IVAS_ACCOUNTS dengan 1 akun saja di Vercel
    kalau mau multi-akun. Gunakan format lengkap semua akun,
    atau biarkan kosong supaya pakai default 4 akun di bawah.
    """
    # 4 akun default — selalu ada
    defaults = [
        {"email": "kicenofficial@gmail.com", "password": "@Kiki2008"},
    ]

    env = os.getenv("IVAS_ACCOUNTS", "").strip()
    if env:
        # Kalau env var diset → pakai env var SAJA (full override)
        accounts = []
        for pair in env.split(","):
            pair = pair.strip()
            if ":" in pair:
                parts = pair.split(":", 1)
                email = parts[0].strip()
                pwd   = parts[1].strip()
                if email and pwd:
                    accounts.append({"email": email, "password": pwd})
        if accounts:
            logger.info(f"[CONFIG] {len(accounts)} akun dari env IVAS_ACCOUNTS")
            return accounts
        else:
            logger.warning("[CONFIG] IVAS_ACCOUNTS diset tapi format salah, pakai default")

    logger.info(f"[CONFIG] Pakai {len(defaults)} akun default")
    return defaults

ACCOUNTS     = load_accounts()
BASE_URL     = "https://ivaskicen2.serverkicen.biz.id"
_BASE_DOMAIN = "ivaskicen2.serverkicen.biz.id"   # domain tanpa https — untuk cookie inject

# Path ke cookies.json — definisikan di sini supaya tersedia di seluruh modul
COOKIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.json")

def _cget(jar, name, default=""):
    """
    Safe cookie getter — menghindari CookieConflictError saat ada duplikat cookie.
    Prioritas: _BASE_DOMAIN → www.ivasms.com → iterasi langsung.
    """
    try:
        v = jar.get(name, domain=_BASE_DOMAIN)
        if v:
            return v
    except Exception:
        pass
    try:
        v = jar.get(name, domain="www.ivasms.com")
        if v:
            return v
    except Exception:
        pass
    # Fallback: iterasi langsung supaya tidak raise CookieConflictError
    for c in jar:
        if c.name == name:
            return c.value
    return default

# ════════════════════════════════════════════════════════
# PRESET COOKIES — disimpan langsung, di-inject saat startup
# Update nilai cookies di sini kalau sudah expired
# ════════════════════════════════════════════════════════

LOGIN_URL    = "https://www.ivasms.com/login"
LIVE_URL     = f"{BASE_URL}/portal/live/my_sms"
RECV_URL     = f"{BASE_URL}/portal/sms/received"


# ════════════════════════════════════════════════════════
# STEALTH — Random User-Agent & Headers
# ════════════════════════════════════════════════════════

_USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

_ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.8,id;q=0.6",
    "fr-FR,fr;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,fr;q=0.7",
]

def build_scraper():
    """Buat scraper dengan UA acak, headers realistis, dan retry otomatis."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    s  = requests.Session()
    ua = random.choice(_USER_AGENTS)
    al = random.choice(_ACCEPT_LANGUAGES)

    # Retry otomatis 3x untuk connection error / timeout
    retry_strategy = Retry(
        total=2,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],  # hapus 429 dari retry — handle manual
        allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"],
    )
    # Pool size lebih besar supaya tidak overflow
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=50,   # naik dari 20 → 50
        pool_maxsize=50,       # naik dari 20 → 50
        pool_block=False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)

    s.headers.update({
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           al,
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Cache-Control":             "max-age=0",
    })
    return s


def decode_response(response):
    enc = response.headers.get("Content-Encoding", "").lower()
    try:
        if enc == "gzip":
            return gzip.decompress(response.content).decode("utf-8", errors="replace")
        if enc == "br":
            import brotli
            return brotli.decompress(response.content).decode("utf-8", errors="replace")
    except Exception:
        pass
    return response.text


def ajax_hdrs(referer=None):
    return {
        "Accept":           "text/html, */*; q=0.01",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           BASE_URL,
        "Referer":          referer or RECV_URL,
    }


def to_ivas_date(date_str):
    """DD/MM/YYYY → YYYY-MM-DD"""
    try:
        d = datetime.strptime(date_str, "%d/%m/%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return date_str


def to_ivas_start(date_str):
    """YYYY-MM-DD → YYYY-MM-DD 00:00:00 (CONFIRMED iVAS butuh datetime penuh)"""
    base = to_ivas_date(date_str)
    return f"{base} 00:00:00" if len(base) == 10 and base[4] == "-" else base


def to_ivas_end(date_str):
    """YYYY-MM-DD → YYYY-MM-DD 23:59:59 (CONFIRMED iVAS butuh datetime penuh)"""
    base = to_ivas_date(date_str)
    return f"{base} 23:59:59" if len(base) == 10 and base[4] == "-" else base



# ════════════════════════════════════════════════════════
# LOGIN PER AKUN
# ════════════════════════════════════════════════════════




def login_account(account):
    """
    Login satu akun. Auto re-login kalau session expired.
    Return dict: {ok, scraper, csrf, live_html, email} atau {ok: False, error, email}
    """
    email    = account["email"]
    password = account["password"]
    scraper  = build_scraper()

    try:
        # ── STEP 1: GET login page → _token ──
        login_page = scraper.get(LOGIN_URL, timeout=25)
        page_html  = decode_response(login_page)
        soup       = BeautifulSoup(page_html, "html.parser")

        tok_el = soup.find("input", {"name": "_token"})
        tok    = tok_el["value"] if tok_el else None
        if not tok:
            meta = soup.find("meta", {"name": "csrf-token"})
            tok  = meta["content"] if meta else None
        if not tok:
            return {"ok": False, "error": "_token tidak ditemukan", "email": email}
        logger.info(f"[LOGIN] _token OK {email}")

        # ── STEP 2: POST login ──
        # Strategi: coba 3 kali dengan variasi cf-turnstile-response
        # Beberapa site tidak strict validate token di backend
        _dummy_tokens = [
            "",                        # tanpa token sama sekali
            "PASSED",                  # nilai dummy umum
            "0." + "x" * 60,          # format mirip token asli
        ]

        resp = None
        for _attempt, _dummy in enumerate(_dummy_tokens):
            login_data = {
                "email":    email,
                "password": password,
                "_token":   tok,
            }
            if _dummy:
                login_data["cf-turnstile-response"] = _dummy

            logger.info(f"[LOGIN] attempt={_attempt+1} dummy_token={repr(_dummy[:20])} {email}")
            resp = scraper.post(
                LOGIN_URL,
                data=login_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer":      LOGIN_URL,
                    "Origin":       BASE_URL,
                },
                allow_redirects=True,
                timeout=30,
            )
            resp_url = getattr(resp, "url", "")
            if "/login" not in resp_url:
                logger.info(f"[LOGIN] Berhasil attempt={_attempt+1} {email}")
                break  # login sukses, keluar loop
            logger.warning(f"[LOGIN] attempt={_attempt+1} masih di /login, coba berikutnya...")
            if _attempt < len(_dummy_tokens) - 1:
                pass  # no delay
                # Re-fetch _token baru setiap retry (token bisa expire)
                try:
                    _rp = scraper.get(LOGIN_URL, timeout=25)
                    _rs = BeautifulSoup(decode_response(_rp), "html.parser")
                    _ti = _rs.find("input", {"name": "_token"})
                    _tm = _rs.find("meta", {"name": "csrf-token"})
                    tok = (_ti["value"] if _ti else (_tm["content"] if _tm else tok))
                except Exception:
                    pass

        resp_url = getattr(resp, "url", "")
        if "/login" in resp_url:
            resp_html = decode_response(resp)
            if "turnstile" in resp_html.lower():
                return {"ok": False,
                        "error": "Turnstile verify gagal — iVAS strict validate token, perlu solver",
                        "email": email}
            return {"ok": False, "error": "Email/password salah", "email": email}


        # Ambil halaman live → dapat csrf terbaru
        portal = scraper.get(LIVE_URL)
        html   = decode_response(portal)
        psoup  = BeautifulSoup(html, "html.parser")

        meta = psoup.find("meta", {"name": "csrf-token"})
        inp  = psoup.find("input", {"name": "_token"})
        csrf = (meta["content"] if meta else (inp["value"] if inp else tok))

        # Ambil CSRF khusus dari halaman received — iVAS bisa pakai token berbeda
        # Confirmed dari debug: _token di GetSMS() diambil dari halaman /portal/sms/received
        recv_csrf = csrf  # default fallback
        try:
            recv_page = scraper.get(RECV_URL)
            recv_html = decode_response(recv_page)
            recv_soup = BeautifulSoup(recv_html, "html.parser")

            # Cari _token dari meta tag dulu
            recv_meta = recv_soup.find("meta", {"name": "csrf-token"})
            if recv_meta:
                recv_csrf = recv_meta["content"]
            else:
                # Cari dari input hidden _token
                recv_inp = recv_soup.find("input", {"name": "_token"})
                if recv_inp:
                    recv_csrf = recv_inp["value"]
                else:
                    # Cari dari inline JS: _token: 'XXXX' atau "_token":"XXXX"
                    m = re.search(r"['\"]_token['\"]\s*[,:]?\s*['\"]([A-Za-z0-9_\-+/=]{20,})['\"]", recv_html)
                    if m:
                        recv_csrf = m.group(1)
            logger.info(f"[LOGIN] recv_csrf OK  {email}")
        except Exception as e:
            logger.warning(f"[LOGIN] Gagal ambil recv_csrf {email}: {e}, pakai csrf generik")

        logger.info(f"[LOGIN] OK  {email}")
        result = {
            "ok": True,
            "scraper":   scraper,
            "csrf":      csrf,
            "recv_csrf": recv_csrf,
            "live_html": html,
            "email":     email,
        }
        # Auto-save cookies ke file setelah login berhasil
        _extract_cookies_from_scraper(scraper, email)
        return result

    except Exception as e:
        logger.error(f"[LOGIN] Error {email}: {e}")
        return {"ok": False, "error": str(e), "email": email}



def _get_all_accounts():
    """
    Kembalikan semua akun: ACCOUNTS default + semua email di cookies.json.
    Support multi-account otomatis — tambah akun cukup via /set-cookies.
    """
    all_accs = {a["email"]: a for a in ACCOUNTS}
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE) as f:
                data = json.load(f)
            meta_keys = {"_readme", "_format", "_saved_at"}
            for email in data:
                if email not in meta_keys and email not in all_accs:
                    all_accs[email] = {"email": email, "password": ""}
    except Exception:
        pass
    return list(all_accs.values())


def _get_account(email):
    """Cari akun di ACCOUNTS default + cookies.json."""
    for a in ACCOUNTS:
        if a["email"] == email:
            return a
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE) as f:
                data = json.load(f)
            if email in data:
                return {"email": email, "password": ""}
    except Exception:
        pass
    return None


# Session cache — diinisialisasi sebelum login functions
_session_cache: dict = {}
_session_lock  = threading.Lock()


_PRESET_COOKIES = []  # Auto-login via password — tidak perlu set manual


# ════════════════════════════════════════════════════════
# AUTO-ROTATE COOKIES SYSTEM
# ════════════════════════════════════════════════════════
# Cara kerja:
# 1. Setiap request cek apakah session expired (_is_session_expired)
# 2. Kalau expired: coba cookies.json dulu → kalau gagal → login ulang
# 3. Setiap 30 menit, background thread cek kesehatan semua session
# 4. Manual override: POST /set-cookies atau GET /update-cookies
# ════════════════════════════════════════════════════════

_cookie_rotate_lock = threading.Lock()
_last_health_check  = {}   # email → timestamp last successful health check
_HEALTH_CHECK_INTERVAL = 1800  # 30 menit


def _save_cookies_to_file(email: str, xsrf: str, session_cookie: str, extra: dict = None):
    """
    Simpan cookies ke cookies.json setelah login berhasil.
    Dipanggil otomatis setelah login sukses.
    """
    try:
        data = {}
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE) as f:
                data = json.load(f)

        data[email] = {
            "XSRF-TOKEN":       xsrf,
            "ivas_sms_session": session_cookie,
            "saved_at":         datetime.now().isoformat(),
            "expires_at":       (datetime.now() + __import__('datetime').timedelta(days=30)).isoformat(),
        }
        if extra:
            data[email].update(extra)

        with _cookies_file_lock:
            with open(COOKIES_FILE, 'w') as f:
                json.dump(data, f, indent=2)

        logger.debug(f"[AUTO-SAVE] Cookies disimpan untuk {email}")
        return True
    except Exception as e:
        logger.warning(f"[AUTO-SAVE] Gagal simpan cookies {email}: {e}")
        return False


def _extract_cookies_from_scraper(scraper, email: str) -> bool:
    """
    Setelah login berhasil, ekstrak cookies dari scraper dan simpan ke file.
    Return True kalau berhasil simpan.
    """
    try:
        jar    = scraper.cookies
        xsrf   = (jar.get("XSRF-TOKEN",      domain=_BASE_DOMAIN) or
                  jar.get("XSRF-TOKEN",       domain="www.ivasms.com") or
                  jar.get("XSRF-TOKEN", "") or "")
        sess   = (jar.get("ivas_sms_session", domain=_BASE_DOMAIN) or
                  jar.get("ivas_sms_session",  domain="www.ivasms.com") or
                  jar.get("ivas_sms_session", "") or "")
        if xsrf and sess:
            return _save_cookies_to_file(email, xsrf, sess)
        return False
    except Exception as e:
        logger.warning(f"[AUTO-SAVE] Extract cookies error {email}: {e}")
        return False


def _check_session_health(email: str, scraper) -> bool:
    """
    Cek apakah session masih valid dengan hit /portal/numbers (ringan).
    Return True kalau masih valid.
    """
    try:
        r = scraper.get(f"{BASE_URL}/portal/numbers",
                       timeout=15, allow_redirects=True)
        if _is_session_expired(r):
            logger.warning(f"[HEALTH] {email}: session expired")
            return False
        logger.debug(f"[HEALTH] {email}: session OK ✅")
        return True
    except Exception as e:
        logger.warning(f"[HEALTH] {email}: check error — {e}")
        return False


# Guard: cegah recursive call _auto_refresh
_refresh_in_progress: dict = {}
_refresh_in_progress: dict = {}
_refresh_lock_ar = threading.Lock()

def _auto_refresh_expired_session(email: str) -> bool:
    """Auto-refresh session expired. Recursion guard aktif."""
    with _refresh_lock_ar:
        if _refresh_in_progress.get(email):
            logger.debug(f"[AUTO-REFRESH] {email}: skip (sedang proses)")
            return False
        _refresh_in_progress[email] = True
    try:
        # Step 1: coba dari cookies.json
        if _try_refresh_from_cookies_json(email):
            with _session_lock:
                sess = _session_cache.get(email)
            if sess and sess.get("ok"):
                logger.info(f"[AUTO-REFRESH] {email}: berhasil dari cookies.json ✅")
                return True

        # Step 2: coba preset cookies (tidak pernah form login — Turnstile block)
        for entry in _PRESET_COOKIES:
            if entry.get("email") == email:
                _inject_preset_cookies()
                with _session_lock:
                    sess = _session_cache.get(email)
                if sess and sess.get("ok"):
                    logger.info(f"[AUTO-REFRESH] {email}: berhasil dari preset cookies ✅")
                    return True
                break

        logger.warning(
            f"[AUTO-REFRESH] {email}: semua cookies habis/expired "
            f"— update via /set-cookies atau /setcookies di bot"
        )
        return False
    except Exception as e:
        logger.error(f"[AUTO-REFRESH] {email}: error — {e}")
        return False
    finally:
        with _refresh_lock_ar:
            _refresh_in_progress.pop(email, None)


def _background_health_monitor():
    """
    Background thread: cek kesehatan semua session setiap 30 menit.
    Kalau ada yang expired → auto-refresh.
    """
    import time as _time
    logger.info("[MONITOR] Background health monitor started")
    _time.sleep(5)  # tunggu 5 detik setelah startup

    # Startup: init timestamp agar health check tidak langsung jalan
    logger.info("[MONITOR] Cek session saat startup...")
    try:
        accounts = _get_all_accounts()
        for acc in accounts:
            em = acc["email"]
            with _session_lock:
                sess = _session_cache.get(em, {})
            if sess and sess.get("ok"):
                scraper = sess.get("scraper")
                if scraper and not _check_session_health(em, scraper):
                    logger.warning(f"[MONITOR] {em}: session expired → auto-refresh")
                    with _session_lock:
                        _session_cache[em] = {"ok": False}
                    _auto_refresh_expired_session(em)
                else:
                    # Tandai waktu cek supaya tidak langsung cek lagi
                    _last_health_check[em] = _time.time()
    except Exception as e:
        logger.warning(f"[MONITOR] Startup check error: {e}")

    while True:
        try:
            accounts = _get_all_accounts()
            for acc in accounts:
                email = acc["email"]
                with _session_lock:
                    sess = _session_cache.get(email)

                if not sess or not sess.get("ok"):
                    logger.debug(f"[MONITOR] {email}: tidak ada session → auto-refresh")
                    _auto_refresh_expired_session(email)
                    continue

                # Semua session pakai interval 30 menit
                last = _last_health_check.get(email, 0)
                if _time.time() - last < _HEALTH_CHECK_INTERVAL:
                    continue

                scraper = sess.get("scraper")
                if not scraper:
                    continue

                if not _check_session_health(email, scraper):
                    # Session expired → auto refresh
                    with _session_lock:
                        _session_cache[email] = {"ok": False}
                    _auto_refresh_expired_session(email)
                else:
                    # Session masih ok → update timestamp + save cookies
                    _last_health_check[email] = _time.time()
                    _extract_cookies_from_scraper(scraper, email)

        except Exception as e:
            logger.error(f"[MONITOR] Error: {e}")

        _time.sleep(1800)  # cek tiap 30 menit


# Start background monitor thread
_monitor_thread = threading.Thread(
    target=_background_health_monitor,
    daemon=True,
    name="cookie-health-monitor"
)
_monitor_thread.start()
logger.info("[MONITOR] Cookie health monitor thread started")


def _inject_preset_cookies():
    """Inject _PRESET_COOKIES ke session cache saat startup (non-blocking, no verify)."""
    for entry in _PRESET_COOKIES:
        email   = entry.get("email", "")
        cookies = entry.get("cookies", {})
        if not email or not cookies:
            continue
        scraper = build_scraper()
        for name, value in cookies.items():
            scraper.cookies.set(name, str(value), domain=_BASE_DOMAIN,    path="/")
            scraper.cookies.set(name, str(value), domain="www.ivasms.com", path="/")
        # Ambil CSRF dari XSRF-TOKEN cookie kalau ada
        _csrf_from_cookie = cookies.get("XSRF-TOKEN", "")
        session_entry = {
            "ok":              True,
            "scraper":         scraper,
            "csrf":            _csrf_from_cookie,
            "recv_csrf":       "",
            "live_html":       "",
            "email":           email,
            "via":             "preset_cookies",
            "verified":        None,
            "cookies_injected": list(cookies.keys()),
            "injected_at":     datetime.now().isoformat(),
        }
        with _session_lock:
            _session_cache[email] = session_entry
        logger.debug(f"[PRESET] Cookies di-inject untuk {email} ({len(cookies)} cookies)")


def login_all_accounts(force=False):
    """
    Return list session aktif semua akun.
    COOKIES-ONLY: tidak pernah login via form (Turnstile block iVAS).
    Priority: cache → cookies.json → preset_cookies
    """
    all_accounts = _get_all_accounts()

    # Fase 1: Ambil dari cache
    sessions = []
    need_refresh = []
    with _session_lock:
        for acc in all_accounts:
            cached = _session_cache.get(acc["email"])
            if not force and cached and cached.get("ok"):
                sessions.append(cached)
                logger.debug(f"[SESSION] Cache HIT {acc['email']}")
            else:
                need_refresh.append(acc)

    if sessions and not force:
        logger.debug(f"[SESSION] {len(sessions)}/{len(all_accounts)} dari cache, skip login")
        return sessions

    if not need_refresh:
        return sessions

    # Fase 2: Inject cookies dari cookies.json atau preset
    logger.info(f"[SESSION] Refresh {len(need_refresh)} akun via cookies injection...")
    for acc in need_refresh:
        email = acc["email"]

        # Coba cookies.json dulu (paling fresh — dari /set-cookies)
        if _try_refresh_from_cookies_json(email):
            with _session_lock:
                result = _session_cache.get(email)
            if result and result.get("ok"):
                sessions.append(result)
                logger.info(f"[SESSION] {email}: cookies.json ✅")
                continue

        # Coba preset cookies
        injected = False
        for entry in _PRESET_COOKIES:
            if entry.get("email") == email:
                _inject_preset_cookies()
                with _session_lock:
                    result = _session_cache.get(email)
                if result and result.get("ok"):
                    sessions.append(result)
                    injected = True
                    logger.info(f"[SESSION] {email}: preset cookies ✅")
                break

        if not injected:
            logger.warning(
                f"[SESSION] {email}: tidak ada cookies valid "
                f"— update via /set-cookies atau /setcookies di bot"
            )

    logger.debug(f"[SESSION] {len(sessions)}/{len(all_accounts)} akun aktif")
    return sessions


# ════════════════════════════════════════════════════════
# LIVE SMS
# ════════════════════════════════════════════════════════



def _is_session_expired(response):
    """Deteksi apakah iVAS sudah logout / session habis."""
    if response is None:
        return True
    url = getattr(response, 'url', '') or ''
    if '/login' in url:
        return True
    try:
        snippet = response.text[:3000].lower()
        if any(k in snippet for k in ('forgot your password', 'login to your account')):
            return True
    except Exception:
        pass
    return False


_cookies_file_lock = threading.Lock()


def _try_refresh_from_cookies_json(email):
    """
    Coba re-inject cookies dari cookies.json untuk akun yang session-nya expired.
    Return True kalau berhasil inject fresh cookies.
    """
    import json as _json
    try:
        if not os.path.exists(COOKIES_FILE):
            return False
        with open(COOKIES_FILE) as f:
            data = _json.load(f)
        if email not in data:
            return False
        entry = data[email]
        # Cek expired
        expires_str = entry.get("expires_at", "")
        if expires_str:
            try:
                from datetime import datetime as _dt
                if datetime.now() > _dt.fromisoformat(expires_str):
                    logger.warning(f"[COOKIES.JSON] {email}: expired, tidak bisa refresh")
                    return False
            except Exception:
                pass
        meta_keys = {"saved_at", "expires_at", "_readme", "_format"}
        cookies = {k: v for k, v in entry.items() if k not in meta_keys}
        if not cookies:
            return False
        scraper = build_scraper()
        for name, value in cookies.items():
            scraper.cookies.set(name, str(value), domain=_BASE_DOMAIN,    path="/")
            scraper.cookies.set(name, str(value), domain="www.ivasms.com", path="/")
        session_entry = {
            "ok": True, "scraper": scraper,
            "csrf": cookies.get("XSRF-TOKEN", ""),
            "recv_csrf": cookies.get("XSRF-TOKEN", ""),
            "live_html": "", "email": email,
            "via": "cookies_json_refresh",
            "injected_at": datetime.now().isoformat(),
        }
        with _session_lock:
            _session_cache[email] = session_entry
        logger.debug(f"[COOKIES.JSON] Re-inject {email}")
        return True
    except Exception as e:
        logger.warning(f"[COOKIES.JSON] Refresh error {email}: {e}")
        return False


def get_session(account, force=False):
    """
    Kembalikan session aktif untuk akun ini.
    Priority: cache → cookies.json → preset_cookies
    TIDAK pernah login via form (Turnstile block).
    """
    email = account["email"]
    with _session_lock:
        cached = _session_cache.get(email)
        if not force and cached and cached.get("ok"):
            return cached

    # Coba inject dari cookies.json (fresh cookies dari /set-cookies)
    if _try_refresh_from_cookies_json(email):
        with _session_lock:
            refreshed = _session_cache.get(email)
            if refreshed and refreshed.get("ok"):
                logger.info(f"[SESSION] {email}: session dari cookies.json ✅")
                return refreshed

    # Coba inject ulang dari preset cookies kalau ada
    for entry in _PRESET_COOKIES:
        if entry.get("email") == email:
            _inject_preset_cookies()
            with _session_lock:
                refreshed = _session_cache.get(email)
                if refreshed and refreshed.get("ok"):
                    logger.info(f"[SESSION] {email}: session dari preset cookies ✅")
                    return refreshed

    logger.warning(f"[SESSION] {email}: tidak ada cookies valid — update via /set-cookies")
    return {"ok": False, "error": "Cookies expired atau belum diset — pakai /set-cookies untuk update", "email": email}


def _scrape_csrf(scraper, page_url):
    """Scrape CSRF dari halaman. Dipanggil oleh _get_csrf_cached."""
    try:
        r = scraper.get(
            page_url,
            headers={
                "Accept":  "text/html,application/xhtml+xml,*/*;q=0.9",
                "Referer": BASE_URL,
            },
            timeout=25,
            allow_redirects=True,
        )

        # Kalau redirect ke login — session expired
        if "/login" in r.url or r.status_code in (401, 403):
            logger.debug(f"[CSRF] Session expired (redirect ke {r.url})")
            # Cari email dari scraper — cek di semua session cache
            expired_email = None
            with _session_lock:
                for em, sess in _session_cache.items():
                    if sess.get("scraper") is scraper:
                        expired_email = em
                        break
            if expired_email:
                logger.debug(f"[CSRF] Trigger auto-refresh untuk {expired_email}")
                refreshed = _auto_refresh_expired_session(expired_email)
                if refreshed:
                    # Ambil scraper baru dari cache yang sudah di-refresh
                    with _session_lock:
                        new_sess = _session_cache.get(expired_email, {})
                    new_scraper = new_sess.get("scraper")
                    if new_scraper and new_scraper is not scraper:
                        # Coba scrape CSRF dengan scraper baru
                        try:
                            r2 = new_scraper.get(page_url, timeout=25, allow_redirects=True)
                            if "/login" not in r2.url:
                                html2 = decode_response(r2)
                                soup2 = BeautifulSoup(html2, "html.parser")
                                meta2 = soup2.find("meta", {"name": "csrf-token"})
                                if meta2 and meta2.get("content"):
                                    logger.info(f"[CSRF] CSRF fresh dari scraper baru ✅")
                                    return meta2["content"]
                        except Exception:
                            pass
                    # Fallback: ambil csrf dari session yang di-refresh
                    csrf_fresh = new_sess.get("csrf", "")
                    if csrf_fresh:
                        return csrf_fresh
            # Last resort: pakai XSRF-TOKEN dari cookie
            xsrf = _cget(scraper.cookies, "XSRF-TOKEN") or _cget(scraper.cookies, "xsrf-token")
            if xsrf and len(xsrf) > 20:
                logger.info(f"[CSRF] Fallback ke XSRF-TOKEN cookie")
                return xsrf
            return None

        html = decode_response(r)

        # 1. meta tag — paling reliable
        soup = BeautifulSoup(html, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        if meta and meta.get("content") and len(meta["content"]) > 10:
            return meta["content"]

        # 2. input hidden _token
        inp = soup.find("input", {"name": "_token"})
        if inp and inp.get("value") and len(inp["value"]) > 10:
            return inp["value"]

        # 3. JS inline patterns
        for pat in [
            r'["\']X-CSRF-TOKEN["\']\s*:\s*["\']([A-Za-z0-9_\-+/=]{20,})["\']',
            r'["\']_token["\']\s*[,:]?\s*["\']([A-Za-z0-9_\-+/=]{20,})["\']',
            r'csrfToken\s*=\s*["\']([A-Za-z0-9_\-+/=]{20,})["\']',
            r'csrf[_-]?token["\s]*[=:]["\s]*["\']([A-Za-z0-9_\-+/=]{20,})["\']',
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                return m.group(1)

        logger.debug(f"[CSRF] Token tidak ditemukan di {page_url} (HTTP {r.status_code}, size {len(html)})")
    except Exception as e:
        logger.debug(f"[CSRF] Exception dari {page_url}: {e}")
    return None


# ── CSRF Cache — hindari GET ke iVAS tiap request ─────────────────────────────
# Key: (scraper_id, page_url) → (csrf_token, timestamp)
_csrf_cache: dict = {}
_csrf_cache_lock  = threading.Lock()
_CSRF_CACHE_TTL = 120  # 2 menit  # detik — iVAS token valid lebih lama, tapi kita refresh tiap 25s


def _get_csrf_cached(scraper, page_url):
    """
    Ambil CSRF dari cache kalau masih fresh (< TTL detik).
    Kalau expired atau tidak ada → scrape dari halaman, simpan ke cache.
    Return: csrf_string atau None
    """
    key = (id(scraper), page_url)
    now = time.time()

    with _csrf_cache_lock:
        cached = _csrf_cache.get(key)
        if cached:
            token, ts = cached
            if now - ts < _CSRF_CACHE_TTL:
                logger.debug(f"[CSRF] Cache hit {page_url} (age {now-ts:.0f}s)")
                return token
            # Expired — hapus
            del _csrf_cache[key]

    # Scrape fresh
    token = _scrape_csrf(scraper, page_url)
    if not token:
        # Fallback: pakai XSRF-TOKEN dari cookie langsung
        token = (
            _cget(scraper.cookies, "XSRF-TOKEN") or
            _cget(scraper.cookies, "xsrf-token") or
            None
        )
        if token:
            logger.debug(f"[CSRF] Scrape gagal, pakai XSRF-TOKEN cookie: {token[:20]}...")
    if token:
        with _csrf_cache_lock:
            _csrf_cache[key] = (token, now)
        logger.debug(f"[CSRF] Cached {page_url}: {token[:20]}...")
    return token



# Map endpoint iVAS → halaman yang harus dibuka untuk dapat CSRF-nya
# Setiap POST endpoint, CSRF diambil dari Referer page-nya
_CSRF_REFERER_MAP = {
    "/portal/numbers/test/export":              f"{BASE_URL}/portal/numbers/test",
    "/portal/numbers/termination/number/add":   f"{BASE_URL}/portal/numbers/test",
    "/portal/numbers/termination/details":      f"{BASE_URL}/portal/numbers/test",
    "/portal/numbers/return/number":            f"{BASE_URL}/portal/numbers",
    "/portal/numbers/return/number/bluck":      f"{BASE_URL}/portal/numbers",
    "/portal/numbers/return/allnumber/bluck":   f"{BASE_URL}/portal/numbers",
    "/portal/sms/received/getsms":              f"{BASE_URL}/portal/sms/received",
    "/portal/sms/received/getsms/number":       f"{BASE_URL}/portal/sms/received",
    "/portal/sms/received/getsms/getmessage":   f"{BASE_URL}/portal/sms/received",
}


def do_request(account, method, url, data=None, headers=None, json=None):
    """
    Buat satu request POST/GET untuk akun.
    Auto re-login kalau session expired.

    FIX ROTATING CSRF:
    iVAS pakai CSRF berbeda di setiap halaman (rotating per-page).
    Untuk setiap POST, kode ini otomatis:
      1. Cek _CSRF_REFERER_MAP → tahu halaman mana yang jadi sumber CSRF
      2. GET halaman tersebut dulu pakai scraper yang sama
      3. Ekstrak CSRF terbaru dari halaman itu
      4. Baru POST dengan CSRF yang fresh

    Untuk GET request: tidak perlu CSRF, langsung hit.
    """
    data  = dict(data) if data else {}
    email = account["email"]

    # Tentukan referer page untuk ambil CSRF (hanya untuk POST)
    csrf_source_page = None
    if method.upper() != "GET":
        # Cari di map berdasarkan path
        url_path = url.replace(BASE_URL, "")
        for endpoint_path, source_page in _CSRF_REFERER_MAP.items():
            if endpoint_path in url_path:
                csrf_source_page = source_page
                break
        # Kalau tidak ada di map, fallback ke Referer dari headers kalau ada
        if not csrf_source_page and headers:
            ref = headers.get("Referer", "")
            if ref.startswith(BASE_URL):
                csrf_source_page = ref

    for attempt in range(3):
        # Attempt >0: force refresh session dari cookies.json dulu, baru login
        if attempt > 0:
            refreshed = _try_refresh_from_cookies_json(email)
            if not refreshed:
                # cookies.json juga expired/tidak ada — force login ulang
                session = get_session(account, force=True)
            else:
                session = get_session(account, force=False)
        else:
            session = get_session(account, force=False)

        if not session or not session.get("ok"):
            logger.error(f"[REQ] Session gagal {email} attempt {attempt+1}")
            continue

        scraper = session["scraper"]

        # ── Pastikan cookies iVAS ada di scraper ──
        # Kalau scraper tidak punya ivas_sms_session, inject dari preset/cookies.json
        if not _cget(scraper.cookies, "ivas_sms_session"):
            _try_refresh_from_cookies_json(email)
            refreshed_sess = _session_cache.get(email)
            if refreshed_sess and refreshed_sess.get("ok"):
                scraper = refreshed_sess["scraper"]

        # ── CSRF: coba scrape, fallback ke XSRF-TOKEN cookie ──
        csrf = ""
        if method.upper() != "GET":
            if csrf_source_page:
                fresh_csrf = _get_csrf_cached(scraper, csrf_source_page)
                csrf = fresh_csrf or ""
            if not csrf:
                # Fallback prioritas: session recv_csrf → XSRF-TOKEN cookie → session csrf
                csrf = (
                    session.get("recv_csrf") if "/portal/sms/received" in url
                    else session.get("csrf", "")
                )
            if not csrf:
                csrf = (
                    _cget(scraper.cookies, "XSRF-TOKEN") or
                    _cget(scraper.cookies, "xsrf-token") or
                    ""
                )
                if csrf:
                    logger.debug(f"[REQ] CSRF dari XSRF-TOKEN cookie: {csrf[:20]}...")
        else:
            csrf = session.get("csrf", "")

        from urllib.parse import unquote as _unquote
        if csrf:
            csrf_decoded = _unquote(csrf)
            # Kirim CSRF via header X-CSRF-TOKEN (sesuai iVAS JS $.ajaxSetup)
            # DAN via _token di body sebagai fallback
            data["_token"] = csrf_decoded

        # Merge headers dengan X-CSRF-TOKEN
        merged_headers = dict(headers or {})
        if csrf:
            merged_headers["X-CSRF-TOKEN"] = csrf_decoded
            # iVAS juga terima via X-XSRF-TOKEN (Laravel default)
            merged_headers["X-XSRF-TOKEN"] = csrf_decoded

        try:
            if method.upper() == "GET":
                resp = scraper.get(url, headers=merged_headers, timeout=25)
            else:
                if json is not None:
                    json_with_csrf = dict(json) if json else {}
                    if csrf and "_token" not in json_with_csrf:
                        json_with_csrf["_token"] = csrf_decoded
                    resp = scraper.post(url, json=json_with_csrf, headers=merged_headers, timeout=25)
                else:
                    resp = scraper.post(url, data=data, headers=merged_headers, timeout=25)

            if _is_session_expired(resp):
                logger.warning(f"[REQ] Expired {email} attempt {attempt+1} → auto refresh...")
                # Auto-refresh lengkap: cookies.json → login ulang
                _auto_refresh_expired_session(email)
                continue

            # Handle 429 rate limit — tunggu sebelum retry
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                wait = min(retry_after, 10)
                logger.warning(f"[REQ] 429 Rate limit {email}, tunggu {wait}s...")
                time.sleep(wait)
                continue

            return resp, csrf

        except Exception as e:
            logger.error(f"[REQ] Error {email} attempt {attempt+1}: {e}")

    return None, None






# ════════════════════════════════════════════════════════
# RECEIVED SMS — 3 level AJAX
# ════════════════════════════════════════════════════════

def get_ranges(account, from_date, to_date):
    """
    Level 1 — Ambil daftar range via POST /portal/sms/received/getsms.

    CONFIRMED dari debug (Image 2):
      POST /portal/sms/received/getsms → 5734 chars, 8 range ✓
      GET  /portal/sms/received        → 69045 chars shell JS kosong, 0 range ✗

    GET dapat halaman shell 69KB yang render via JS di browser — kita tidak bisa
    eksekusi JS, jadi GET tidak akan pernah dapat data range.
    POST adalah endpoint AJAX yang return HTML fragment berisi data range langsung.

    Kalau POST return "No SMS found" → memang tidak ada SMS di tanggal itu (bukan error).
    Return: [{"name": "IVORY COAST 2055", "id": "IVORY_COAST_2055"}, ...]
    """
    ivas_from = to_ivas_date(from_date)
    ivas_to   = to_ivas_date(to_date)
    result    = []

    def _add(name, rid):
        name = name.strip()
        rid  = rid.strip() if rid else name.replace(" ", "_")
        if name and not any(r["name"] == name for r in result):
            result.append({"name": name, "id": rid})

    def _parse_ranges(html):
        """Parse semua pola range dari HTML fragment."""
        # Pass 1: onclick="toggleRange('NAMA','ID')" — confirmed dari debug
        # Ini paling akurat karena langsung dari onclick attribute
        for m in re.finditer(r"toggleRange\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", html):
            _add(m.group(1), m.group(2))

        # Pass 2: double-quote variant
        for m in re.finditer(r'toggleRange\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)', html):
            _add(m.group(1), m.group(2))

        # Pass 3: BeautifulSoup div.rng — hanya kalau pass 1&2 gagal
        if not result:
            soup = BeautifulSoup(html, "html.parser")
            for div in soup.select("div.rng"):
                # Ambil nama hanya dari span.rname, bukan seluruh text div
                # (menghindari ambil teks count/revenue dari child divs)
                rname_el = div.select_one("span.rname")
                name     = rname_el.get_text(strip=True) if rname_el else ""
                oc       = div.get("onclick", "")
                m2       = re.search(r"toggleRange[^(]*\(\s*'([^']+)'\s*,\s*'([^']+)'", oc)
                if m2:
                    _add(m2.group(1), m2.group(2))
                elif name:
                    sub = div.select_one("[id^='sp_']")
                    rid = sub["id"].replace("sp_", "") if sub else name.replace(" ", "_")
                    _add(name, rid)

        # Pass 4: id="sp_XXXX" — last resort
        if not result:
            soup2 = BeautifulSoup(html, "html.parser")
            for div in soup2.select("[id^='sp_']"):
                rid  = div["id"].replace("sp_", "")
                name = rid.replace("_", " ")
                prev = div.find_previous_sibling()
                if prev:
                    # Hanya ambil nama dari span.rname, bukan seluruh teks
                    rname_el = prev.select_one("span.rname") if hasattr(prev, "select_one") else None
                    t = rname_el.get_text(strip=True) if rname_el else prev.get_text(strip=True)
                    if t and 1 < len(t) < 60 and "USD" not in t and not t[0].isdigit():
                        name = t
                _add(name, rid)

    # ── Cache check — hindari spam ke iVAS ──────────────────────────────────
    _ck = f"ranges:{account['email']}:{from_date}:{to_date}"
    cached_ranges, hit = _ivas_cache_get(_ck, _IVAS_RANGES_TTL)
    if hit:
        logger.debug(f"[RANGES] Cache hit {account['email']}")
        return cached_ranges

    # ── Attempt 1: POST dengan YYYY-MM-DD — CONFIRMED dari iVAS date picker ──
    resp1, _ = do_request(
        account, "POST",
        f"{BASE_URL}/portal/sms/received/getsms",
        data={"from": ivas_from, "to": ivas_to},
        headers=ajax_hdrs(),
    )
    if resp1 and resp1.status_code == 200:
        html1 = decode_response(resp1)
        _parse_ranges(html1)
        logger.debug(f"[RANGES] POST (YYYY-MM-DD) → {len(result)} ranges, html={len(html1)}c")

    # ── Attempt 2: POST dengan DD/MM/YYYY langsung ────────────────────────────
    if not result:
        resp2, _ = do_request(
            account, "POST",
            f"{BASE_URL}/portal/sms/received/getsms",
            data={"from": from_date, "to": to_date},
            headers=ajax_hdrs(),
        )
        if resp2 and resp2.status_code == 200:
            html2 = decode_response(resp2)
            _parse_ranges(html2)
            logger.debug(f"[RANGES] POST (DD/MM/YYYY) → {len(result)} ranges, html={len(html2)}c")

    # ── Attempt 3: POST dengan M/D/YYYY (format lama) ────────────────────────
    if not result:
        try:
            d       = datetime.strptime(from_date, "%d/%m/%Y")
            old_fmt = f"{d.month}/{d.day}/{d.year}"
            resp3, _ = do_request(
                account, "POST",
                f"{BASE_URL}/portal/sms/received/getsms",
                data={"from": old_fmt, "to": old_fmt},
                headers=ajax_hdrs(),
            )
            if resp3 and resp3.status_code == 200:
                html3 = decode_response(resp3)
                _parse_ranges(html3)
                logger.debug(f"[RANGES] POST (M/D/YYYY) → {len(result)} ranges, html={len(html3)}c")
        except Exception:
            pass

    if not result:
        logger.debug(f"[RANGES] 0 ranges untuk {from_date} — tidak ada SMS hari itu")
    else:
        logger.debug(f"[RANGES] FINAL {len(result)} ranges: {[r['name'] for r in result]}")

    # Simpan ke cache (termasuk hasil kosong supaya tidak re-fetch)
    _ivas_cache_set(_ck, result)
    return result


def get_numbers(account, range_name, from_date, to_date, range_id=None):
    """
    Level 2 — Ambil nomor di range dari /portal/sms/received/getsms/number.

    CONFIRMED dari debug iVAS (Image 1):
      Response: toggleNumtj4D0('2250767821640','2250767821640_179490252')
      Format: toggleNum[RANDOM_SUFFIX](NOMOR, NOMOR_MSGID)

    Parameter yang dicoba (berurutan):
      1. range=RANGE_NAME (nama asli dengan spasi)
      2. range=RANGE_ID   (underscore version)
      3. range_name=RANGE_NAME (fallback key berbeda)

    Return: [{"number": "2250767821640", "num_id": "2250767821640_179490252"}, ...]
    """
    rid = range_id or range_name.replace(" ", "_")

    # ── Cache check ──────────────────────────────────────────────────────────
    _ck = f"numbers:{account['email']}:{range_name}:{from_date}:{to_date}"
    cached_nums, hit = _ivas_cache_get(_ck, _IVAS_NUMBERS_TTL)
    if hit:
        logger.debug(f"[NUMBERS] Cache hit '{range_name}'")
        return cached_nums

    def _parse_numbers(html):
        nums = []
        def _add(num, num_id=""):
            # Bersihkan: hanya digit
            d = re.sub(r'\D', '', str(num))
            if 7 <= len(d) <= 15 and not any(n["number"] == d for n in nums):
                nums.append({"number": d, "num_id": num_id or d})

        # Pass 1 (UTAMA): toggleNum[SUFFIX]('NOMOR','ID')
        # Confirmed: toggleNumtj4D0('2250767821640','2250767821640_179490252')
        # Regex: \w* bukan \w+ supaya handle toggleNum('x','y') juga
        for m in re.finditer(r"toggleNum\w*\s*\(\s*'(\d{7,15})'\s*,\s*'([^']+)'\s*\)", html):
            _add(m.group(1), m.group(2))

        # Pass 2: double-quote variant
        if not nums:
            for m in re.finditer(r'toggleNum\w*\s*\(\s*"(\d{7,15})"\s*,\s*"([^"]+)"\s*\)', html):
                _add(m.group(1), m.group(2))

        # Pass 3: toggleNumXXX(NUMBER, ID) tanpa quotes (angka langsung)
        if not nums:
            for m in re.finditer(r"toggleNum\w*\s*\(\s*(\d{7,15})\s*,\s*(\S+?)\s*\)", html):
                _add(m.group(1), m.group(2).strip("'\""))

        # Pass 4: BeautifulSoup span.nnum
        if not nums:
            soup2 = BeautifulSoup(html, "html.parser")
            for el in soup2.select("span.nnum"):
                raw = re.sub(r'\D', '', el.get_text(strip=True))
                if raw:
                    _add(raw)

        # Pass 5: div.nrow / div[onclick*='toggleNum']
        if not nums:
            soup3 = BeautifulSoup(html, "html.parser")
            for div in soup3.select("div.nrow,[onclick*='toggleNum']"):
                oc = div.get("onclick", "")
                m  = re.search(r"toggleNum\w*\s*\(\s*'?(\d{7,15})'?\s*,\s*'?([^',)]+)'?", oc)
                if m:
                    _add(m.group(1), m.group(2).strip("'\""))

        # Pass 6: angka dalam single-quotes (last resort)
        if not nums:
            for m in re.finditer(r"'(\d{7,15})'", html):
                _add(m.group(1))

        return nums

    # ── Attempt 1: range=NAMA dengan start/end datetime penuh ─────────────
    resp, _ = do_request(
        account, "POST",
        f"{BASE_URL}/portal/sms/received/getsms/number",
        data={
            "start": to_ivas_start(from_date),
            "end":   to_ivas_end(to_date),
            "from":  to_ivas_date(from_date),
            "to":    to_ivas_date(to_date),
            "range": range_name,
        },
        headers=ajax_hdrs(),
    )
    if resp and resp.status_code == 200:
        html = decode_response(resp)
        numbers = _parse_numbers(html)
        if numbers:
            logger.info(f"[NUMBERS] '{range_name}' (by nama) → {[n['number'] for n in numbers]}")
            _ivas_cache_set(_ck, numbers)
            return numbers
        logger.info(f"[NUMBERS] '{range_name}' by nama → 0 num, html[:200]={html[:200]}")

    # ── Attempt 2: range=ID (underscore) ──────────────────────────────────
    resp2, _ = do_request(
        account, "POST",
        f"{BASE_URL}/portal/sms/received/getsms/number",
        data={
            "start": to_ivas_start(from_date),
            "end":   to_ivas_end(to_date),
            "from":  to_ivas_date(from_date),
            "to":    to_ivas_date(to_date),
            "range": rid,
        },
        headers=ajax_hdrs(),
    )
    if resp2 and resp2.status_code == 200:
        html2 = decode_response(resp2)
        numbers2 = _parse_numbers(html2)
        if numbers2:
            logger.info(f"[NUMBERS] '{range_name}' (by id={rid}) → {[n['number'] for n in numbers2]}")
            _ivas_cache_set(_ck, numbers2)
            return numbers2
        logger.info(f"[NUMBERS] '{range_name}' by id={rid} → 0 num, html[:200]={html2[:200]}")

    # ── Attempt 3: range_name=NAMA (key berbeda) ──────────────────────────
    resp3, _ = do_request(
        account, "POST",
        f"{BASE_URL}/portal/sms/received/getsms/number",
        data={"start": to_ivas_date(from_date), "end": to_ivas_date(to_date), "range_name": range_name},
        headers=ajax_hdrs(),
    )
    if resp3 and resp3.status_code == 200:
        html3 = decode_response(resp3)
        numbers3 = _parse_numbers(html3)
        if numbers3:
            logger.info(f"[NUMBERS] '{range_name}' (by range_name key) → {[n['number'] for n in numbers3]}")
            _ivas_cache_set(_ck, numbers3)
            return numbers3

    logger.warning(f"[NUMBERS] '{range_name}' 0 nomor setelah 3 attempt")
    _ivas_cache_set(_ck, [])
    return []


def get_sms(account, phone_number, range_name, from_date, to_date):
    """
    Level 3 — Ambil isi SMS untuk 1 nomor dari /portal/sms/received/getsms/number/sms.

    CONFIRMED dari debug iVAS (Image 7, line 1499-1508):
      URL: /portal/sms/received/getsms
      Payload: from, to, _token  (untuk level 1)
      Payload level 3: start, end, Number, Range  (Range = NAMA, bukan ID)
      Response states:
        Loading: <div class="spinner-border">
        Error:   <p ...>Something went wrong. Please try again.</p>
        Success: <table> dengan kolom Sender | Message | Time | Revenue
                 Message cell berisi: <div class="msg-text">PESAN</div>
    """
    # Coba 2 variasi Range parameter (nama asli & ID)
    rid = range_name.replace(" ", "_")

    # ── Cache check — SMS TTL 30 detik, cukup fresh untuk polling diff ──────
    _ck = f"sms:{account['email']}:{phone_number}:{range_name}:{from_date}:{to_date}"
    cached_sms, hit = _ivas_cache_get(_ck, _IVAS_SMS_TTL)
    if hit:
        logger.debug(f"[SMS] Cache hit {phone_number} → {len(cached_sms)} pesan")
        return cached_sms

    # Payload pertama adalah yang confirmed working — taruh di depan
    # Payload lain sebagai fallback kalau yang pertama gagal
    attempts_data = [
        {"start": to_ivas_start(from_date), "end": to_ivas_end(to_date),
         "Number": phone_number, "Range": range_name},
        {"start": to_ivas_start(from_date), "end": to_ivas_end(to_date),
         "Number": phone_number, "Range": rid},
    ]

    raw = None
    soup = None
    for payload in attempts_data:
        resp, _ = do_request(
            account, "POST",
            f"{BASE_URL}/portal/sms/received/getsms/number/sms",
            data=payload,
            headers=ajax_hdrs(),
        )
        if resp is None or resp.status_code != 200:
            continue
        raw  = decode_response(resp)
        # Skip kalau response adalah halaman login
        if "/login" in getattr(resp, "url", ""):
            continue
        # Skip spinner-only (loading state dari iVAS)
        if "spinner-border" in raw and len(raw) < 500:
            logger.info(f"[SMS] {phone_number} spinner response, coba payload lain")
            continue
        # Skip "something went wrong"
        if "Something went wrong" in raw and len(raw) < 500:
            logger.info(f"[SMS] {phone_number} error response, coba payload lain")
            continue
        soup = BeautifulSoup(raw, "html.parser")
        break

    if not soup or not raw:
        logger.warning(f"[SMS] {phone_number}@{range_name} semua attempt gagal (None response)")
        return None

    def _clean(t):
        """Unescape HTML entities dan bersihkan whitespace."""
        return html_lib.unescape(t).strip()

    messages = []  # Kumpulkan SEMUA pesan, bukan hanya 1

    def _add_msg(t):
        t = _clean(t)
        if len(t) > 3 and t not in messages:
            messages.append(t)

    # ── Pass 1 (UTAMA): SEMUA div.msg-text — ambil semua row ─────────────
    for el in soup.select("div.msg-text, td.msg-text, p.msg-text, span.msg-text"):
        _add_msg(el.get_text(separator="\n", strip=True))

    # ── Pass 2: kolom Message di <table> — ambil SEMUA row ────────────────
    if not messages:
        for tbl in soup.find_all("table"):
            ths = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            col = None
            for kw in ("message", "content", "sms", "text", "body"):
                for i, h in enumerate(ths):
                    if kw in h:
                        col = i
                        break
                if col is not None:
                    break
            if col is None:
                continue
            for tr in tbl.select("tbody tr"):
                tds = tr.find_all("td")
                if len(tds) > col:
                    inner = tds[col].select_one("div.msg-text, .msg-text")
                    t = inner.get_text(separator="\n", strip=True) if inner \
                        else tds[col].get_text(separator="\n", strip=True)
                    if t and not t.isdigit():
                        _add_msg(t)

    # ── Pass 3: CSS selectors lain ────────────────────────────────────────
    if not messages:
        for sel in [
            "div.smsg", "p.smsg", "div.sms-message", "p.sms-message",
            "div.message-content", "div.msg-body",
            ".col-9.col-sm-6 p", ".col-9 p", "td p",
        ]:
            for el in soup.select(sel):
                _add_msg(el.get_text(separator="\n", strip=True))

    # ── Pass 4: table tanpa header — ambil semua row, kolom terpanjang ────
    if not messages:
        for tbl in soup.find_all("table"):
            for tr in tbl.find_all("tr"):
                tds = tr.find_all("td")
                candidates = []
                for td in tds:
                    t = _clean(td.get_text(separator=" ", strip=True))
                    if len(t) > 10 and not t.isdigit():
                        candidates.append(t)
                if candidates:
                    _add_msg(max(candidates, key=len))

    # ── Pass 5: scoring leaf elements ─────────────────────────────────────
    if not messages:
        best_score, best_txt = 0, None
        for el in soup.find_all(["p", "div", "span", "td", "li"]):
            if el.find_all(True):
                continue
            t = _clean(el.get_text(separator=" ", strip=True))
            if len(t) < 5:
                continue
            if any(skip in t.lower() for skip in ("something went wrong", "loading", "spinner", "please try again")):
                continue
            sc  = 0
            sc += 4 if re.search(r"\d{4,8}", t) else 0
            sc += 3 if len(t) > 20 else (1 if len(t) > 8 else 0)
            sc += 2 if re.search(r"[a-zA-Z]{3,}", t) else 0
            if sc > best_score:
                best_score, best_txt = sc, t
        if best_score >= 4 and best_txt:
            _add_msg(best_txt)

    # ── Pass 6: full text fallback ────────────────────────────────────────
    if not messages:
        for el in soup(["script", "style", "noscript"]):
            el.decompose()
        for line in soup.get_text(separator="\n", strip=True).splitlines():
            line = _clean(line)
            if len(line) >= 8 and re.search(r"\d{4,}", line) and re.search(r"[a-zA-Z]", line):
                if not any(skip in line.lower() for skip in ("something went wrong", "please try again")):
                    _add_msg(line)

    if messages:
        logger.info(f"[SMS] {phone_number} ✓ {len(messages)} pesan ditemukan")
        _ivas_cache_set(_ck, messages)
        return messages  # Return LIST semua pesan

    logger.warning(f"[SMS] {phone_number}@{range_name} GAGAL. HTML({len(raw)}): {raw[:300]}")
    return None



def _ivas_clean_sid(raw):
    import html as _h
    s = _h.unescape(str(raw))
    s = re.sub(r'<script[\s\S]*?</script>', '', s, flags=re.IGNORECASE)
    s = re.sub(r'<style[\s\S]*?</style>',  '', s, flags=re.IGNORECASE)
    s = re.sub(r'<[^>]+>', '', s)
    s = re.sub(r'[ \t]+', ' ', s)
    for line in s.split('\n'):
        line = line.strip()
        if line:
            return line
    return s.strip()

def _ivas_clean_msg(raw):
    import html as _h
    s = str(raw)
    s = _h.unescape(_h.unescape(s))
    s = re.sub(r'<script[\s\S]*?</script>', '', s, flags=re.IGNORECASE)
    s = re.sub(r'<style[\s\S]*?</style>',  '', s, flags=re.IGNORECASE)
    s = re.sub(r'<[^>]+>', '', s)
    s = re.sub(r'[ \t]+', ' ', s)
    return s.strip()


def fetch_received_from_session(session, from_date, to_date):
    """
    Ambil semua received SMS dari 1 akun. Return list OTP.
    Per-number fetch dibatasi 2s — lewat dari itu skip supaya total max ~2.5s.
    """
    email   = session["email"]
    # Support multi-account: cari dari semua akun termasuk yang dari cookies.json
    account = _get_account(email)
    if not account:
        account = {"email": email, "password": ""}

    ranges = get_ranges(account, from_date, to_date)
    if not ranges:
        logger.debug(f"[RECV] {email}: tidak ada range hari ini")
        return []

    tasks = []
    for rng in ranges:
        num_list = get_numbers(account, rng["name"], from_date, to_date, range_id=rng["id"])
        for n in num_list:
            tasks.append((n["number"] if isinstance(n, dict) else str(n), rng["name"]))

    if not tasks:
        logger.debug(f"[RECV] {email}: tidak ada nomor di semua range")
        return []

    results = []

    def _fetch(args):
        num, rng_name = args
        msgs = get_sms(account, num, rng_name, from_date, to_date)
        if not msgs:
            return []
        out = []
        for msg in msgs:
            if isinstance(msg, dict):
                msg_text = _ivas_clean_msg(str(msg.get("message", msg.get("otp_message", str(msg)))))
                sid_val  = _ivas_clean_sid(str(msg.get("sid", msg.get("sender", ""))))
                rcv_val  = str(msg.get("received_at", msg.get("senttime", "")))
            else:
                msg_text = _ivas_clean_msg(str(msg))
                sid_val  = rcv_val = ""
            out.append({
                "range":        rng_name,
                "phone_number": num,
                "otp_message":  msg_text,
                "sid":          sid_val,
                "received_at":  rcv_val,
                "source":       "received",
                "account":      email,
            })
        return out

    # Max 10 thread paralel, timeout 6s total — cukup untuk 20+ nomor dengan 10 worker
    _NUM_WORKERS  = min(len(tasks), 10)
    _BATCH_TIMEOUT = 6.0
    with ThreadPoolExecutor(max_workers=_NUM_WORKERS) as ex:
        futures = [ex.submit(_fetch, t) for t in tasks]
        done, pending = _cf.wait(futures, timeout=_BATCH_TIMEOUT)
        for future in done:
            try:
                results.extend(future.result())
            except Exception as e:
                logger.error(f"[RECV] Future error: {e}")
        if pending:
            logger.debug(f"[RECV] {email}: {len(pending)} nomor belum selesai — skip")
            for f in pending:
                f.cancel()

    logger.info(f"[RECV] {email}: {len(results)}/{len(tasks)} SMS berhasil")
    return results


# ════════════════════════════════════════════════════════
# MAIN FETCH — GABUNGAN SEMUA AKUN
# ════════════════════════════════════════════════════════

def fetch_all_accounts(from_date, to_date, mode="received"):
    """
    Login semua akun → ambil SMS dari SEMUA akun secara PARALEL → gabungkan.
    Hard timeout 2.5s per akun — akun yang lambat di-skip supaya response cepat.
    Deduplicate berdasarkan (phone_number, 50 karakter pertama pesan).
    """
    sessions = login_all_accounts()
    if not sessions:
        return None, "Semua akun gagal login"

    all_otp   = []
    seen_keys = set()

    def _add(item):
        key = f"{item['phone_number']}|{item['otp_message'][:50]}"
        if key not in seen_keys:
            seen_keys.add(key)
            all_otp.append(item)

    # Semua akun paralel — timeout 10s per akun (cukup untuk 20+ nomor)
    # Multi-akun berjalan bersamaan: 2 akun @ 10s = selesai dalam ~10s, bukan 20s
    _ACCT_TIMEOUT = 10.0
    if mode in ("received", "both"):
        n_workers = max(len(sessions), 1)
        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            fut_map = {ex.submit(fetch_received_from_session, s, from_date, to_date): s for s in sessions}
            done, pending = _cf.wait(fut_map, timeout=_ACCT_TIMEOUT)

            for future in done:
                try:
                    for item in future.result():
                        _add(item)
                except Exception as e:
                    logger.error(f"[MAIN] Account fetch error: {e}")

            if pending:
                slow = [fut_map[f]["email"] for f in pending]
                logger.warning(f"[MAIN] {len(pending)}/{len(sessions)} akun timeout ({_ACCT_TIMEOUT}s) — skip: {slow}")
                for f in pending:
                    f.cancel()

    logger.debug(f"[MAIN] Total gabungan: {len(all_otp)} OTP dari {len(sessions)} akun")
    return all_otp, None


# ════════════════════════════════════════════════════════
# FLASK APP
app = Flask(__name__)

# ── CORS — izinkan semua origin (support custom domain & panel) ──
@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin", "*")
    allowed = os.getenv("ALLOWED_ORIGINS", "*")
    if allowed == "*" or origin in allowed.split(","):
        response.headers["Access-Control-Allow-Origin"]  = origin if allowed != "*" else "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, DELETE, PUT"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
        response.headers["Access-Control-Max-Age"]       = "86400"
    return response

@app.before_request
def handle_options():
    if request.method == "OPTIONS":
        resp = app.make_default_options_response()
        add_cors_headers(resp)
        return resp

# Inject preset cookies saat startup
_inject_preset_cookies()



# ════════════════════════════════════════════════════════
# /debug/del-raw — Debug raw delete response dari iVAS
# ════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════
# /debug/ivas-js — Dump raw JS iVAS halaman numbers
# ════════════════════════════════════════════════════════
@app.route("/")
def welcome():
    import base64
    html = base64.b64decode("PCFET0NUWVBFIGh0bWw+CjxodG1sIGxhbmc9ImlkIj4KPGhlYWQ+CjxtZXRhIGNoYXJzZXQ9IlVURi04Ii8+CjxtZXRhIG5hbWU9InZpZXdwb3J0IiBjb250ZW50PSJ3aWR0aD1kZXZpY2Utd2lkdGgsaW5pdGlhbC1zY2FsZT0xLjAiLz4KPHRpdGxlPktZLVNISVJPIOKAlCBTTVMgT1RQIEFQSTwvdGl0bGU+CjxsaW5rIHJlbD0icHJlY29ubmVjdCIgaHJlZj0iaHR0cHM6Ly9mb250cy5nb29nbGVhcGlzLmNvbSIvPgo8bGluayByZWw9InByZWNvbm5lY3QiIGhyZWY9Imh0dHBzOi8vZm9udHMuZ3N0YXRpYy5jb20iIGNyb3Nzb3JpZ2luLz4KPGxpbmsgaHJlZj0iaHR0cHM6Ly9mb250cy5nb29nbGVhcGlzLmNvbS9jc3MyP2ZhbWlseT1JQk0rUGxleCtNb25vOndnaHRANDAwOzUwMDs2MDAmZmFtaWx5PUJyaWNvbGFnZStHcm90ZXNxdWU6b3Bzeix3Z2h0QDEyLi45Niw0MDA7NTAwOzYwMDs3MDA7ODAwJmRpc3BsYXk9c3dhcCIgcmVsPSJzdHlsZXNoZWV0Ii8+CjxzdHlsZT4KKiwqOjpiZWZvcmUsKjo6YWZ0ZXJ7Ym94LXNpemluZzpib3JkZXItYm94O21hcmdpbjowO3BhZGRpbmc6MH0KaHRtbHtzY3JvbGwtYmVoYXZpb3I6c21vb3RoO2ZvbnQtc2l6ZToxNnB4fQo6cm9vdHsKICAtLWluazojZjBlZGU4OwogIC0taW5rMjojOWE5NTkwOwogIC0taW5rMzojNTA0ZDQ4OwogIC0taW5rNDojMmEyODI1OwogIC0tcGFwZXI6IzBlMGQwYjsKICAtLWNhcmQ6IzE2MTUxMjsKICAtLWNhcmQyOiMxZDFjMTk7CiAgLS1saW5lOiMyYTI4MjU7CiAgLS1ncmVlbjojYjhmZjZlOwogIC0tZ3JlZW4yOiM3YWNjM2E7CiAgLS1yZWQ6I2ZmNmI2YjsKICAtLWJsdWU6IzZlYjhmZjsKICAtLXllbGxvdzojZmZkNjY2OwogIC0tc2VyaWY6J0JyaWNvbGFnZSBHcm90ZXNxdWUnLHNhbnMtc2VyaWY7CiAgLS1tb25vOidJQk0gUGxleCBNb25vJyxtb25vc3BhY2U7CiAgLS1yOjEwcHg7Cn0KYm9keXtiYWNrZ3JvdW5kOnZhcigtLXBhcGVyKTtjb2xvcjp2YXIoLS1pbmspO2ZvbnQtZmFtaWx5OnZhcigtLXNlcmlmKTtvdmVyZmxvdy14OmhpZGRlbjtsaW5lLWhlaWdodDoxLjV9Cjo6LXdlYmtpdC1zY3JvbGxiYXJ7d2lkdGg6M3B4fQo6Oi13ZWJraXQtc2Nyb2xsYmFyLXRyYWNre2JhY2tncm91bmQ6dmFyKC0tcGFwZXIpfQo6Oi13ZWJraXQtc2Nyb2xsYmFyLXRodW1ie2JhY2tncm91bmQ6dmFyKC0tZ3JlZW4pO2JvcmRlci1yYWRpdXM6MnB4fQphe3RleHQtZGVjb3JhdGlvbjpub25lO2NvbG9yOmluaGVyaXR9CmJ1dHRvbntjdXJzb3I6cG9pbnRlcjtib3JkZXI6bm9uZTtiYWNrZ3JvdW5kOm5vbmU7Zm9udC1mYW1pbHk6aW5oZXJpdH0KCi8qIOKUgOKUgCBOQVYg4pSA4pSAICovCiNuYXZ7CiAgcG9zaXRpb246Zml4ZWQ7dG9wOjA7bGVmdDowO3JpZ2h0OjA7ei1pbmRleDo5MDA7CiAgaGVpZ2h0OjU2cHg7CiAgZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtqdXN0aWZ5LWNvbnRlbnQ6c3BhY2UtYmV0d2VlbjsKICBwYWRkaW5nOjAgMjBweDsKICBiYWNrZ3JvdW5kOnJnYmEoMTQsMTMsMTEsLjg1KTsKICBiYWNrZHJvcC1maWx0ZXI6Ymx1cigxNnB4KTsKICBib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKTsKICB0cmFuc2l0aW9uOmJvcmRlci1jb2xvciAuM3M7Cn0KLm5hdi1icmFuZHtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxMHB4fQoubmF2LWxvZ28tbWFya3sKICB3aWR0aDozMHB4O2hlaWdodDozMHB4O2JvcmRlci1yYWRpdXM6N3B4OwogIGJhY2tncm91bmQ6dmFyKC0tZ3JlZW4pOwogIGRpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjsKICBmbGV4LXNocmluazowOwp9Ci5uYXYtbG9nby1tYXJrIHN2Z3t3aWR0aDoxOHB4O2hlaWdodDoxOHB4fQoubmF2LW5hbWV7Zm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjEzcHg7Zm9udC13ZWlnaHQ6NjAwO2xldHRlci1zcGFjaW5nOi41cHg7Y29sb3I6dmFyKC0taW5rKX0KLm5hdi1uYW1lIGJ7Y29sb3I6dmFyKC0tZ3JlZW4pfQoubmF2LXJ7ZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6NnB4fQoubmF2LWxpbmt7CiAgZm9udC1zaXplOjEzcHg7Zm9udC13ZWlnaHQ6NTAwO2NvbG9yOnZhcigtLWluazIpOwogIHBhZGRpbmc6NXB4IDEwcHg7Ym9yZGVyLXJhZGl1czo2cHg7CiAgdHJhbnNpdGlvbjpjb2xvciAuMnMsYmFja2dyb3VuZCAuMnM7Cn0KLm5hdi1saW5rOmhvdmVye2NvbG9yOnZhcigtLWluayk7YmFja2dyb3VuZDp2YXIoLS1jYXJkMil9Ci8qIDMtZG90ICovCi5kb3QtYnRuewogIHdpZHRoOjM0cHg7aGVpZ2h0OjM0cHg7Ym9yZGVyLXJhZGl1czo3cHg7CiAgYm9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtiYWNrZ3JvdW5kOnZhcigtLWNhcmQpOwogIGNvbG9yOnZhcigtLWluazIpOwogIGRpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjsKICB0cmFuc2l0aW9uOmFsbCAuMnM7cG9zaXRpb246cmVsYXRpdmU7Cn0KLmRvdC1idG46aG92ZXJ7Ym9yZGVyLWNvbG9yOnZhcigtLWdyZWVuKTtjb2xvcjp2YXIoLS1ncmVlbil9Ci5kb3QtbWVudXsKICBwb3NpdGlvbjphYnNvbHV0ZTt0b3A6Y2FsYygxMDAlICsgNnB4KTtyaWdodDowOwogIGJhY2tncm91bmQ6dmFyKC0tY2FyZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTsKICBib3JkZXItcmFkaXVzOjEycHg7cGFkZGluZzo1cHg7bWluLXdpZHRoOjE5NXB4OwogIGRpc3BsYXk6bm9uZTsKICBib3gtc2hhZG93OjAgMTZweCA0MHB4IHJnYmEoMCwwLDAsLjYpOwogIHotaW5kZXg6MTA7Cn0KLmRvdC1tZW51LnNob3d7ZGlzcGxheTpibG9jazthbmltYXRpb246cG9wIC4xNXMgZWFzZX0KQGtleWZyYW1lcyBwb3B7ZnJvbXtvcGFjaXR5OjA7dHJhbnNmb3JtOnRyYW5zbGF0ZVkoLTZweCkgc2NhbGUoLjk3KX10b3tvcGFjaXR5OjE7dHJhbnNmb3JtOm5vbmV9fQouZG0taXRlbXsKICBkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo5cHg7CiAgcGFkZGluZzo5cHggMTFweDtib3JkZXItcmFkaXVzOjdweDsKICBmb250LXNpemU6MTNweDtmb250LXdlaWdodDo1MDA7Y29sb3I6dmFyKC0taW5rMik7CiAgdHJhbnNpdGlvbjphbGwgLjE1cztjdXJzb3I6cG9pbnRlcjsKfQouZG0taXRlbTpob3ZlcntiYWNrZ3JvdW5kOnZhcigtLWNhcmQyKTtjb2xvcjp2YXIoLS1pbmspfQouZG0taWNvbnt3aWR0aDoyOHB4O2hlaWdodDoyOHB4O2JvcmRlci1yYWRpdXM6NnB4O2JhY2tncm91bmQ6dmFyKC0tY2FyZDIpO2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjtmb250LXNpemU6MTRweDtmbGV4LXNocmluazowfQouZG0tc2Vwe2hlaWdodDoxcHg7YmFja2dyb3VuZDp2YXIoLS1saW5lKTttYXJnaW46M3B4IDB9CkBtZWRpYShtYXgtd2lkdGg6NjAwcHgpey5uYXYtbGlua3tkaXNwbGF5Om5vbmV9fQoKLyog4pSA4pSAIExBWU9VVCDilIDilIAgKi8KLndyYXB7bWF4LXdpZHRoOjEwNDBweDttYXJnaW46MCBhdXRvO3BhZGRpbmc6MCAyMHB4fQoKLyog4pSA4pSAIEhFUk8g4pSA4pSAICovCi5oZXJvewogIG1pbi1oZWlnaHQ6MTAwdmg7CiAgZGlzcGxheTpmbGV4O2ZsZXgtZGlyZWN0aW9uOmNvbHVtbjsKICBqdXN0aWZ5LWNvbnRlbnQ6Y2VudGVyO2FsaWduLWl0ZW1zOmZsZXgtc3RhcnQ7CiAgcGFkZGluZzoxMDBweCAyMHB4IDYwcHg7CiAgbWF4LXdpZHRoOjEwNDBweDttYXJnaW46MCBhdXRvOwogIHBvc2l0aW9uOnJlbGF0aXZlOwp9Ci8qIGJpZyBmYWludCB0ZXh0IGJnICovCi5oZXJvLWJnLXRleHR7CiAgcG9zaXRpb246YWJzb2x1dGU7cmlnaHQ6LTIwcHg7dG9wOjUwJTt0cmFuc2Zvcm06dHJhbnNsYXRlWSgtNTAlKTsKICBmb250LWZhbWlseTp2YXIoLS1tb25vKTtmb250LXNpemU6Y2xhbXAoODBweCwxNHZ3LDE2MHB4KTtmb250LXdlaWdodDo2MDA7CiAgY29sb3I6cmdiYSgxODQsMjU1LDExMCwuMDQpOwogIGxldHRlci1zcGFjaW5nOi01cHg7cG9pbnRlci1ldmVudHM6bm9uZTt1c2VyLXNlbGVjdDpub25lO3doaXRlLXNwYWNlOm5vd3JhcDsKICBsaW5lLWhlaWdodDoxOwp9Ci5oZXJvLWNoaXB7CiAgZGlzcGxheTppbmxpbmUtZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjdweDsKICBwYWRkaW5nOjVweCAxMnB4O2JvcmRlci1yYWRpdXM6MTAwcHg7CiAgYmFja2dyb3VuZDpyZ2JhKDE4NCwyNTUsMTEwLC4wOCk7Ym9yZGVyOjFweCBzb2xpZCByZ2JhKDE4NCwyNTUsMTEwLC4xOCk7CiAgZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjExcHg7Y29sb3I6dmFyKC0tZ3JlZW4pO2xldHRlci1zcGFjaW5nOjEuMnB4OwogIHRleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTttYXJnaW4tYm90dG9tOjI0cHg7Cn0KLmNoaXAtZG90e3dpZHRoOjZweDtoZWlnaHQ6NnB4O2JvcmRlci1yYWRpdXM6NTAlO2JhY2tncm91bmQ6dmFyKC0tZ3JlZW4pO2FuaW1hdGlvbjpibGluayAycyBlYXNlLWluLW91dCBpbmZpbml0ZX0KQGtleWZyYW1lcyBibGlua3swJSwxMDAle29wYWNpdHk6MTtib3gtc2hhZG93OjAgMCAwIDAgcmdiYSgxODQsMjU1LDExMCwuNSl9NTAle29wYWNpdHk6LjY7Ym94LXNoYWRvdzowIDAgMCA1cHggcmdiYSgxODQsMjU1LDExMCwwKX19Ci5oZXJvLXRpdGxlewogIGZvbnQtc2l6ZTpjbGFtcCg0NHB4LDcuNXZ3LDg4cHgpO2ZvbnQtd2VpZ2h0OjgwMDsKICBsaW5lLWhlaWdodDouOTU7bGV0dGVyLXNwYWNpbmc6LTNweDsKICBtYXJnaW4tYm90dG9tOjIwcHg7Cn0KLmhlcm8tdGl0bGUgLnQxe2Rpc3BsYXk6YmxvY2s7Y29sb3I6dmFyKC0taW5rKX0KLmhlcm8tdGl0bGUgLnQye2Rpc3BsYXk6YmxvY2s7Y29sb3I6dmFyKC0tZ3JlZW4pfQouaGVyby1zdWJ7CiAgZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjEycHg7Y29sb3I6dmFyKC0taW5rMyk7CiAgbGV0dGVyLXNwYWNpbmc6M3B4O3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTttYXJnaW4tYm90dG9tOjIwcHg7Cn0KLmhlcm8tZGVzY3sKICBtYXgtd2lkdGg6NTAwcHg7Y29sb3I6dmFyKC0taW5rMik7Zm9udC1zaXplOjE2cHg7bGluZS1oZWlnaHQ6MS43OwogIG1hcmdpbi1ib3R0b206MzZweDsKfQouaGVyby1jdGF7ZGlzcGxheTpmbGV4O2dhcDoxMHB4O2ZsZXgtd3JhcDp3cmFwfQouYnRuLW1haW57CiAgZGlzcGxheTppbmxpbmUtZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjhweDsKICBwYWRkaW5nOjEycHggMjJweDtib3JkZXItcmFkaXVzOjhweDsKICBiYWNrZ3JvdW5kOnZhcigtLWdyZWVuKTtjb2xvcjojMGUwZDBiOwogIGZvbnQtd2VpZ2h0OjcwMDtmb250LXNpemU6MTRweDtsZXR0ZXItc3BhY2luZzouMnB4OwogIHRyYW5zaXRpb246YWxsIC4yczsKfQouYnRuLW1haW46aG92ZXJ7YmFja2dyb3VuZDojYzhmZjgwO3RyYW5zZm9ybTp0cmFuc2xhdGVZKC0ycHgpO2JveC1zaGFkb3c6MCA4cHggMjBweCByZ2JhKDE4NCwyNTUsMTEwLC4yNSl9Ci5idG4tZ2hvc3R7CiAgZGlzcGxheTppbmxpbmUtZmxleDthbGlnbi1pdGVtczpjZW50ZXI7Z2FwOjhweDsKICBwYWRkaW5nOjEycHggMjJweDtib3JkZXItcmFkaXVzOjhweDsKICBib3JkZXI6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2NvbG9yOnZhcigtLWluazIpOwogIGZvbnQtd2VpZ2h0OjYwMDtmb250LXNpemU6MTRweDsKICB0cmFuc2l0aW9uOmFsbCAuMnM7Cn0KLmJ0bi1naG9zdDpob3Zlcntib3JkZXItY29sb3I6dmFyKC0taW5rMik7Y29sb3I6dmFyKC0taW5rKTt0cmFuc2Zvcm06dHJhbnNsYXRlWSgtMnB4KX0KCi8qIOKUgOKUgCBTVEFUVVMgQkFSIOKUgOKUgCAqLwouc3RhdHVzLWJhcnsKICBkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDowOwogIGJhY2tncm91bmQ6dmFyKC0tY2FyZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjEycHg7CiAgb3ZlcmZsb3c6aGlkZGVuO2ZsZXgtd3JhcDp3cmFwOwogIG1hcmdpbjowIDIwcHg7CiAgbWF4LXdpZHRoOjEwNDBweDttYXJnaW46MCBhdXRvIDA7Cn0KLnNiLWl0ZW17CiAgZmxleDoxO21pbi13aWR0aDoxNDBweDsKICBwYWRkaW5nOjE2cHggMjBweDsKICBib3JkZXItcmlnaHQ6MXB4IHNvbGlkIHZhcigtLWxpbmUpOwogIGRpc3BsYXk6ZmxleDtmbGV4LWRpcmVjdGlvbjpjb2x1bW47Z2FwOjRweDsKfQouc2ItaXRlbTpsYXN0LWNoaWxke2JvcmRlci1yaWdodDpub25lfQouc2ItbGFiZWx7Zm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjEwcHg7Y29sb3I6dmFyKC0taW5rMyk7bGV0dGVyLXNwYWNpbmc6MS41cHg7dGV4dC10cmFuc2Zvcm06dXBwZXJjYXNlfQouc2ItdmFse2ZvbnQtc2l6ZToxNHB4O2ZvbnQtd2VpZ2h0OjYwMDtjb2xvcjp2YXIoLS1pbmspfQouc2ItZG90e2Rpc3BsYXk6aW5saW5lLWJsb2NrO3dpZHRoOjdweDtoZWlnaHQ6N3B4O2JvcmRlci1yYWRpdXM6NTAlO21hcmdpbi1yaWdodDo2cHg7dmVydGljYWwtYWxpZ246bWlkZGxlfQoub25saW5le2JhY2tncm91bmQ6dmFyKC0tZ3JlZW4pO2FuaW1hdGlvbjpibGluayAycyBpbmZpbml0ZX0KLm9mZmxpbmV7YmFja2dyb3VuZDp2YXIoLS1yZWQpfQouY2hlY2tpbmd7YmFja2dyb3VuZDp2YXIoLS15ZWxsb3cpO2FuaW1hdGlvbjpibGluayAxcyBpbmZpbml0ZX0KQG1lZGlhKG1heC13aWR0aDo2NDBweCl7LnNiLWl0ZW17bWluLXdpZHRoOmNhbGMoNTAlIC0gMXB4KX0uc2ItaXRlbTpudGgtY2hpbGQoMil7Ym9yZGVyLXJpZ2h0Om5vbmV9LnNiLWl0ZW06bnRoLWNoaWxkKDMpe2JvcmRlci10b3A6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yaWdodDoxcHggc29saWQgdmFyKC0tbGluZSl9LnNiLWl0ZW06bnRoLWNoaWxkKDQpe2JvcmRlci10b3A6MXB4IHNvbGlkIHZhcigtLWxpbmUpO2JvcmRlci1yaWdodDpub25lfX0KCi8qIOKUgOKUgCBTRUNUSU9OIOKUgOKUgCAqLwouc2VjdGlvbntwYWRkaW5nOjcycHggMH0KLnNlY3Rpb24td3JhcHttYXgtd2lkdGg6MTA0MHB4O21hcmdpbjowIGF1dG87cGFkZGluZzowIDIwcHh9Ci5zLWxhYmVse2ZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZToxMXB4O2NvbG9yOnZhcigtLWdyZWVuKTtsZXR0ZXItc3BhY2luZzoyLjVweDt0ZXh0LXRyYW5zZm9ybTp1cHBlcmNhc2U7bWFyZ2luLWJvdHRvbToxMHB4fQoucy10aXRsZXtmb250LXNpemU6Y2xhbXAoMjZweCw0dncsMzhweCk7Zm9udC13ZWlnaHQ6ODAwO2xldHRlci1zcGFjaW5nOi0xcHg7bGluZS1oZWlnaHQ6MS4xO21hcmdpbi1ib3R0b206MTRweH0KLnMtZGVzY3tjb2xvcjp2YXIoLS1pbmsyKTtmb250LXNpemU6MTVweDtsaW5lLWhlaWdodDoxLjc7bWF4LXdpZHRoOjUyMHB4O21hcmdpbi1ib3R0b206NDRweH0KLmhye2hlaWdodDoxcHg7YmFja2dyb3VuZDp2YXIoLS1saW5lKTttYXJnaW46MCAyMHB4fQoKLyog4pSA4pSAIEFCT1VUIENBUkRTIOKUgOKUgCAqLwouYWJvdXQtZ3JpZHtkaXNwbGF5OmdyaWQ7Z3JpZC10ZW1wbGF0ZS1jb2x1bW5zOnJlcGVhdChhdXRvLWZpbGwsbWlubWF4KDIyMHB4LDFmcikpO2dhcDoxNHB4fQouYWJvdXQtY2FyZHsKICBiYWNrZ3JvdW5kOnZhcigtLWNhcmQpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czp2YXIoLS1yKTsKICBwYWRkaW5nOjI0cHg7dHJhbnNpdGlvbjphbGwgLjI1cztwb3NpdGlvbjpyZWxhdGl2ZTtvdmVyZmxvdzpoaWRkZW47Cn0KLmFib3V0LWNhcmQ6OmFmdGVyewogIGNvbnRlbnQ6Jyc7cG9zaXRpb246YWJzb2x1dGU7aW5zZXQ6MDtib3JkZXItcmFkaXVzOnZhcigtLXIpOwogIGJhY2tncm91bmQ6cmFkaWFsLWdyYWRpZW50KGNpcmNsZSBhdCAwJSAwJSxyZ2JhKDE4NCwyNTUsMTEwLC4wNiksdHJhbnNwYXJlbnQgNjAlKTsKICBvcGFjaXR5OjA7dHJhbnNpdGlvbjpvcGFjaXR5IC4zcztwb2ludGVyLWV2ZW50czpub25lOwp9Ci5hYm91dC1jYXJkOmhvdmVye2JvcmRlci1jb2xvcjpyZ2JhKDE4NCwyNTUsMTEwLC4yNSk7dHJhbnNmb3JtOnRyYW5zbGF0ZVkoLTNweCl9Ci5hYm91dC1jYXJkOmhvdmVyOjphZnRlcntvcGFjaXR5OjF9Ci5hYy1lbXtmb250LXNpemU6MjZweDttYXJnaW4tYm90dG9tOjE0cHh9Ci5hYy10e2ZvbnQtc2l6ZToxNXB4O2ZvbnQtd2VpZ2h0OjcwMDtjb2xvcjp2YXIoLS1pbmspO21hcmdpbi1ib3R0b206NnB4fQouYWMtZHtmb250LXNpemU6MTNweDtjb2xvcjp2YXIoLS1pbmsyKTtsaW5lLWhlaWdodDoxLjZ9CgovKiDilIDilIAgU1RBVFMg4pSA4pSAICovCi5zdGF0cy1yb3d7ZGlzcGxheTpncmlkO2dyaWQtdGVtcGxhdGUtY29sdW1uczpyZXBlYXQoYXV0by1maWxsLG1pbm1heCgxODBweCwxZnIpKTtnYXA6MTRweDttYXJnaW4tYm90dG9tOjQ4cHh9Ci5zdGF0ewogIGJhY2tncm91bmQ6dmFyKC0tY2FyZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOnZhcigtLXIpOwogIHBhZGRpbmc6MjRweDt0ZXh0LWFsaWduOmNlbnRlcjsKfQouc3RhdC1ue2ZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZTozOHB4O2ZvbnQtd2VpZ2h0OjYwMDtjb2xvcjp2YXIoLS1ncmVlbik7bGV0dGVyLXNwYWNpbmc6LTJweDtsaW5lLWhlaWdodDoxfQouc3RhdC1se2ZvbnQtc2l6ZToxM3B4O2NvbG9yOnZhcigtLWluazIpO21hcmdpbi10b3A6NnB4fQoKLyog4pSA4pSAIERPQ1Mg4pSA4pSAICovCi5lcC1saXN0e2Rpc3BsYXk6ZmxleDtmbGV4LWRpcmVjdGlvbjpjb2x1bW47Z2FwOjEycHh9Ci5lcHtiYWNrZ3JvdW5kOnZhcigtLWNhcmQpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czp2YXIoLS1yKTtvdmVyZmxvdzpoaWRkZW47dHJhbnNpdGlvbjpib3JkZXItY29sb3IgLjJzfQouZXA6aG92ZXJ7Ym9yZGVyLWNvbG9yOnZhcigtLWxpbmUpfQouZXAtaGVhZHsKICBwYWRkaW5nOjE0cHggMThweDtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxMHB4OwogIGN1cnNvcjpwb2ludGVyO3VzZXItc2VsZWN0Om5vbmU7Cn0KLmVwLW1ldGhvZHsKICBmb250LWZhbWlseTp2YXIoLS1tb25vKTtmb250LXNpemU6MTBweDtmb250LXdlaWdodDo2MDA7CiAgcGFkZGluZzozcHggOHB4O2JvcmRlci1yYWRpdXM6NXB4O2xldHRlci1zcGFjaW5nOi44cHg7ZmxleC1zaHJpbms6MDsKfQouR0VUe2JhY2tncm91bmQ6cmdiYSgxODQsMjU1LDExMCwuMSk7Y29sb3I6dmFyKC0tZ3JlZW4pO2JvcmRlcjoxcHggc29saWQgcmdiYSgxODQsMjU1LDExMCwuMil9Ci5lcC1wYXRoe2ZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZToxM3B4O2NvbG9yOnZhcigtLWluayk7ZmxleDoxfQouZXAtc2hvcnR7Zm9udC1zaXplOjEzcHg7Y29sb3I6dmFyKC0taW5rMil9Ci5lcC1hcnJvd3tjb2xvcjp2YXIoLS1pbmszKTtmb250LXNpemU6MTFweDt0cmFuc2l0aW9uOnRyYW5zZm9ybSAuMnM7ZmxleC1zaHJpbms6MH0KLmVwLWFycm93Lm9wZW57dHJhbnNmb3JtOnJvdGF0ZSgxODBkZWcpfQouZXAtYm9keXtkaXNwbGF5Om5vbmU7Ym9yZGVyLXRvcDoxcHggc29saWQgdmFyKC0tbGluZSk7cGFkZGluZzowIDE4cHggMThweH0KLmVwLWJvZHkub3BlbntkaXNwbGF5OmJsb2NrfQoucHR7bWFyZ2luLXRvcDoxNnB4O2ZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZToxMHB4O2NvbG9yOnZhcigtLWluazMpO2xldHRlci1zcGFjaW5nOjEuNXB4O3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTttYXJnaW4tYm90dG9tOjdweH0KLnB0YWJsZXt3aWR0aDoxMDAlO2JvcmRlci1jb2xsYXBzZTpjb2xsYXBzZTtmb250LXNpemU6MTNweH0KLnB0YWJsZSB0aHt0ZXh0LWFsaWduOmxlZnQ7cGFkZGluZzo3cHggMTBweDtmb250LWZhbWlseTp2YXIoLS1tb25vKTtmb250LXNpemU6OXB4O2xldHRlci1zcGFjaW5nOjEuNXB4O3RleHQtdHJhbnNmb3JtOnVwcGVyY2FzZTtjb2xvcjp2YXIoLS1pbmszKTtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKX0KLnB0YWJsZSB0ZHtwYWRkaW5nOjlweCAxMHB4O2JvcmRlci1ib3R0b206MXB4IHNvbGlkIHJnYmEoNDIsNDAsMzcsLjUpO2NvbG9yOnZhcigtLWluazIpO3ZlcnRpY2FsLWFsaWduOnRvcDtsaW5lLWhlaWdodDoxLjV9Ci5wdGFibGUgdGQ6Zmlyc3QtY2hpbGR7Zm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Y29sb3I6dmFyKC0tYmx1ZSk7d2hpdGUtc3BhY2U6bm93cmFwfQouYnJ7ZGlzcGxheTppbmxpbmUtYmxvY2s7cGFkZGluZzoycHggN3B4O2JvcmRlci1yYWRpdXM6NHB4O2ZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZTo5cHg7bGV0dGVyLXNwYWNpbmc6LjVweH0KLmJyLXJ7YmFja2dyb3VuZDpyZ2JhKDI1NSwxMDcsMTA3LC4xKTtib3JkZXI6MXB4IHNvbGlkIHJnYmEoMjU1LDEwNywxMDcsLjIpO2NvbG9yOnZhcigtLXJlZCl9Ci5ici1ve2JhY2tncm91bmQ6cmdiYSgxMTAsMTg0LDI1NSwuMDgpO2JvcmRlcjoxcHggc29saWQgcmdiYSgxMTAsMTg0LDI1NSwuMTUpO2NvbG9yOnZhcigtLWJsdWUpfQouY29kZXsKICBiYWNrZ3JvdW5kOiMwYTA5MDg7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOjhweDsKICBwYWRkaW5nOjE0cHg7Zm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjEycHg7Y29sb3I6dmFyKC0taW5rMik7CiAgb3ZlcmZsb3cteDphdXRvO2xpbmUtaGVpZ2h0OjEuNztwb3NpdGlvbjpyZWxhdGl2ZTt3aGl0ZS1zcGFjZTpwcmU7Cn0KLmNvZGUgLmt7Y29sb3I6dmFyKC0tYmx1ZSl9Ci5jb2RlIC5ze2NvbG9yOiNhNWQ2ZmZ9Ci5jb2RlIC5reXtjb2xvcjp2YXIoLS1ncmVlbil9Ci5jb2RlIC52e2NvbG9yOnZhcigtLXllbGxvdyl9Ci5jb2RlIC5je2NvbG9yOnZhcigtLWluazMpfQouY3AtYnRuewogIHBvc2l0aW9uOmFic29sdXRlO3RvcDoxMHB4O3JpZ2h0OjEwcHg7CiAgcGFkZGluZzozcHggOXB4O2JvcmRlci1yYWRpdXM6NXB4OwogIGJhY2tncm91bmQ6dmFyKC0tY2FyZDIpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7CiAgY29sb3I6dmFyKC0taW5rMyk7Zm9udC1zaXplOjEwcHg7Zm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7CiAgdHJhbnNpdGlvbjphbGwgLjJzO2N1cnNvcjpwb2ludGVyOwp9Ci5jcC1idG46aG92ZXJ7Y29sb3I6dmFyKC0taW5rKTtib3JkZXItY29sb3I6dmFyKC0tZ3JlZW4pfQpAbWVkaWEobWF4LXdpZHRoOjYwMHB4KXsuZXAtc2hvcnR7ZGlzcGxheTpub25lfX0KCi8qIOKUgOKUgCBDT05UQUNUIOKUgOKUgCAqLwouY29udGFjdC1ncmlke2Rpc3BsYXk6Z3JpZDtncmlkLXRlbXBsYXRlLWNvbHVtbnM6cmVwZWF0KGF1dG8tZmlsbCxtaW5tYXgoMjAwcHgsMWZyKSk7Z2FwOjEycHh9Ci5jY3sKICBkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxNHB4OwogIGJhY2tncm91bmQ6dmFyKC0tY2FyZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOnZhcigtLXIpOwogIHBhZGRpbmc6MjBweDt0ZXh0LWRlY29yYXRpb246bm9uZTsKICB0cmFuc2l0aW9uOmFsbCAuMjVzOwp9Ci5jYzpob3Zlcntib3JkZXItY29sb3I6cmdiYSgxODQsMjU1LDExMCwuMjUpO3RyYW5zZm9ybTp0cmFuc2xhdGVZKC0zcHgpO2JhY2tncm91bmQ6dmFyKC0tY2FyZDIpfQouY2MtaWNvbnt3aWR0aDo0MnB4O2hlaWdodDo0MnB4O2JvcmRlci1yYWRpdXM6OXB4O2Rpc3BsYXk6ZmxleDthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjtmb250LXNpemU6MjBweDtmbGV4LXNocmluazowfQouYmctdGd7YmFja2dyb3VuZDpyZ2JhKDExMCwxODQsMjU1LC4xKX0KLmJnLXdhe2JhY2tncm91bmQ6cmdiYSgxODQsMjU1LDExMCwuMDgpfQouYmctZGV2e2JhY2tncm91bmQ6cmdiYSgyNTUsMjE0LDEwMiwuMDgpfQouY2MtdHtmb250LXNpemU6MTRweDtmb250LXdlaWdodDo3MDA7Y29sb3I6dmFyKC0taW5rKX0KLmNjLXN7Zm9udC1zaXplOjEycHg7Y29sb3I6dmFyKC0taW5rMik7bWFyZ2luLXRvcDoycHh9CgovKiDilIDilIAgRk9PVEVSIOKUgOKUgCAqLwpmb290ZXJ7CiAgYm9yZGVyLXRvcDoxcHggc29saWQgdmFyKC0tbGluZSk7cGFkZGluZzoyOHB4IDIwcHg7CiAgdGV4dC1hbGlnbjpjZW50ZXI7Cn0KLmZvb3QtbmFtZXtmb250LWZhbWlseTp2YXIoLS1tb25vKTtmb250LXNpemU6MTRweDtmb250LXdlaWdodDo2MDA7Y29sb3I6dmFyKC0taW5rKTttYXJnaW4tYm90dG9tOjZweH0KLmZvb3QtbmFtZSBie2NvbG9yOnZhcigtLWdyZWVuKX0KLmZvb3Qtc3Vie2ZvbnQtc2l6ZToxM3B4O2NvbG9yOnZhcigtLWluazMpfQouZm9vdC1zdWIgYXtjb2xvcjp2YXIoLS1pbmsyKX0KLmZvb3Qtc3ViIGE6aG92ZXJ7Y29sb3I6dmFyKC0tZ3JlZW4pfQoKLyog4pSA4pSAIE1PREFMIOKUgOKUgCAqLwoub3ZlcmxheXsKICBwb3NpdGlvbjpmaXhlZDtpbnNldDowO2JhY2tncm91bmQ6cmdiYSgwLDAsMCwuNzUpOwogIGJhY2tkcm9wLWZpbHRlcjpibHVyKDhweCk7ei1pbmRleDoxMDAwOwogIGRpc3BsYXk6bm9uZTthbGlnbi1pdGVtczpjZW50ZXI7anVzdGlmeS1jb250ZW50OmNlbnRlcjtwYWRkaW5nOjIwcHg7Cn0KLm92ZXJsYXkuc2hvd3tkaXNwbGF5OmZsZXh9Ci5tb2RhbHsKICBiYWNrZ3JvdW5kOnZhcigtLWNhcmQpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoxNnB4OwogIHBhZGRpbmc6MjhweDttYXgtd2lkdGg6NDQwcHg7d2lkdGg6MTAwJTtwb3NpdGlvbjpyZWxhdGl2ZTsKICBhbmltYXRpb246cG9wIC4xOHMgZWFzZTsKfQoubW9kYWwteHsKICBwb3NpdGlvbjphYnNvbHV0ZTt0b3A6MTRweDtyaWdodDoxNHB4OwogIHdpZHRoOjMwcHg7aGVpZ2h0OjMwcHg7Ym9yZGVyLXJhZGl1czo2cHg7CiAgYmFja2dyb3VuZDp2YXIoLS1jYXJkMik7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTsKICBjb2xvcjp2YXIoLS1pbmsyKTtmb250LXNpemU6MTRweDsKICBkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2p1c3RpZnktY29udGVudDpjZW50ZXI7CiAgdHJhbnNpdGlvbjphbGwgLjJzO2N1cnNvcjpwb2ludGVyOwp9Ci5tb2RhbC14OmhvdmVye2NvbG9yOnZhcigtLXJlZCk7Ym9yZGVyLWNvbG9yOnZhcigtLXJlZCl9Ci5tb2RhbC10e2ZvbnQtc2l6ZToxOHB4O2ZvbnQtd2VpZ2h0OjgwMDttYXJnaW4tYm90dG9tOjZweH0KLm1vZGFsLWR7Zm9udC1zaXplOjE0cHg7Y29sb3I6dmFyKC0taW5rMik7bGluZS1oZWlnaHQ6MS42O21hcmdpbi1ib3R0b206MjBweH0KLmRldi1jYXJkewogIGJhY2tncm91bmQ6dmFyKC0tY2FyZDIpO2JvcmRlcjoxcHggc29saWQgdmFyKC0tbGluZSk7Ym9yZGVyLXJhZGl1czoxMHB4OwogIHBhZGRpbmc6MTZweDtkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxNHB4Owp9Ci5kZXYtYXZ7CiAgd2lkdGg6NDZweDtoZWlnaHQ6NDZweDtib3JkZXItcmFkaXVzOjEwcHg7CiAgYmFja2dyb3VuZDpsaW5lYXItZ3JhZGllbnQoMTM1ZGVnLHZhcigtLWdyZWVuKSwjNmViOGZmKTsKICBkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2p1c3RpZnktY29udGVudDpjZW50ZXI7CiAgZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjE2cHg7Zm9udC13ZWlnaHQ6NzAwO2NvbG9yOiMwZTBkMGI7CiAgZmxleC1zaHJpbms6MDsKfQouZGV2LW57Zm9udC1zaXplOjE1cHg7Zm9udC13ZWlnaHQ6NzAwO2NvbG9yOnZhcigtLWluayl9Ci5kZXYtcntmb250LWZhbWlseTp2YXIoLS1tb25vKTtmb250LXNpemU6MTFweDtjb2xvcjp2YXIoLS1pbmsyKTttYXJnaW4tdG9wOjJweH0KCi8qIOKUgOKUgCBBTklNIOKUgOKUgCAqLwoucmV2ZWFse29wYWNpdHk6MTt0cmFuc2Zvcm06bm9uZX0KLnJldmVhbC5pbntvcGFjaXR5OjE7dHJhbnNmb3JtOm5vbmV9Cjwvc3R5bGU+CjwvaGVhZD4KPGJvZHk+Cgo8IS0tIE5BViAtLT4KPG5hdiBpZD0ibmF2Ij4KICA8ZGl2IGNsYXNzPSJuYXYtYnJhbmQiPgogICAgPGRpdiBjbGFzcz0ibmF2LWxvZ28tbWFyayI+CiAgICAgIDxzdmcgdmlld0JveD0iMCAwIDE4IDE4IiBmaWxsPSJub25lIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0zIDN2MTJNMyA5bDUtNk0zIDlsNSA2IiBzdHJva2U9IiMwZTBkMGIiIHN0cm9rZS13aWR0aD0iMi4yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiLz4KICAgICAgICA8cGF0aCBkPSJNMTEgM2wyLjUgNC41TDE2IDNNMTMuNSA3LjVWMTUiIHN0cm9rZT0iIzBlMGQwYiIgc3Ryb2tlLXdpZHRoPSIyLjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgogICAgICA8L3N2Zz4KICAgIDwvZGl2PgogICAgPHNwYW4gY2xhc3M9Im5hdi1uYW1lIj5LWS08Yj5TSElSTzwvYj48L3NwYW4+CiAgPC9kaXY+CiAgPGRpdiBjbGFzcz0ibmF2LXIiPgogICAgPGEgY2xhc3M9Im5hdi1saW5rIiBocmVmPSIjYWJvdXQiPlRlbnRhbmc8L2E+CiAgICA8YSBjbGFzcz0ibmF2LWxpbmsiIGhyZWY9IiNkb2NzIj5Eb2NzPC9hPgogICAgPGEgY2xhc3M9Im5hdi1saW5rIiBocmVmPSIjY29udGFjdCI+S29udGFrPC9hPgogICAgPGJ1dHRvbiBjbGFzcz0iZG90LWJ0biIgaWQ9ImRvdEJ0biIgb25jbGljaz0idG9nZ2xlRG90KGV2ZW50KSI+CiAgICAgIDxzdmcgd2lkdGg9IjE0IiBoZWlnaHQ9IjE0IiB2aWV3Qm94PSIwIDAgMTQgMTQiIGZpbGw9Im5vbmUiPgogICAgICAgIDxjaXJjbGUgY3g9IjciIGN5PSIyLjUiIHI9IjEuNCIgZmlsbD0iY3VycmVudENvbG9yIi8+CiAgICAgICAgPGNpcmNsZSBjeD0iNyIgY3k9IjciIHI9IjEuNCIgZmlsbD0iY3VycmVudENvbG9yIi8+CiAgICAgICAgPGNpcmNsZSBjeD0iNyIgY3k9IjExLjUiIHI9IjEuNCIgZmlsbD0iY3VycmVudENvbG9yIi8+CiAgICAgIDwvc3ZnPgogICAgICA8ZGl2IGNsYXNzPSJkb3QtbWVudSIgaWQ9ImRvdE1lbnUiPgogICAgICAgIDxhIGNsYXNzPSJkbS1pdGVtIiBocmVmPSIjZG9jcyI+PHNwYW4gY2xhc3M9ImRtLWljb24iPvCfk5o8L3NwYW4+RG9rdW1lbnRhc2k8L2E+CiAgICAgICAgPGEgY2xhc3M9ImRtLWl0ZW0iIGhyZWY9IiNhYm91dCI+PHNwYW4gY2xhc3M9ImRtLWljb24iPvCflI08L3NwYW4+VGVudGFuZyBBUEk8L2E+CiAgICAgICAgPGEgY2xhc3M9ImRtLWl0ZW0iIGhyZWY9IiNjb250YWN0Ij48c3BhbiBjbGFzcz0iZG0taWNvbiI+8J+SrDwvc3Bhbj5IdWJ1bmdpIEthbWk8L2E+CiAgICAgICAgPGRpdiBjbGFzcz0iZG0tc2VwIj48L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJkbS1pdGVtIiBvbmNsaWNrPSJvcGVuTW9kYWwoJ2Rldk1vZGFsJykiPjxzcGFuIGNsYXNzPSJkbS1pY29uIj7wn5GkPC9zcGFuPkRldmVsb3BlcjwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImRtLWl0ZW0iIG9uY2xpY2s9ImNoZWNrU3RhdHVzKHRydWUpIj48c3BhbiBjbGFzcz0iZG0taWNvbiI+8J+fojwvc3Bhbj5DZWsgU3RhdHVzPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iZG0tc2VwIj48L2Rpdj4KICAgICAgICA8YSBjbGFzcz0iZG0taXRlbSIgaHJlZj0iaHR0cHM6Ly93d3cuaXZhc21zLmNvbSIgdGFyZ2V0PSJfYmxhbmsiPjxzcGFuIGNsYXNzPSJkbS1pY29uIj7wn5SXPC9zcGFuPmlWQVMgU01TPC9hPgogICAgICAgIDxhIGNsYXNzPSJkbS1pdGVtIiBocmVmPSJodHRwczovL3ZlcmNlbC5jb20iIHRhcmdldD0iX2JsYW5rIj48c3BhbiBjbGFzcz0iZG0taWNvbiI+4payPC9zcGFuPlZlcmNlbDwvYT4KICAgICAgPC9kaXY+CiAgICA8L2J1dHRvbj4KICA8L2Rpdj4KPC9uYXY+Cgo8IS0tIEhFUk8gLS0+CjxzZWN0aW9uIHN0eWxlPSJwb3NpdGlvbjpyZWxhdGl2ZTtvdmVyZmxvdzpoaWRkZW4iPgogIDxkaXYgY2xhc3M9Imhlcm8gcmV2ZWFsIiBpZD0iaGVybyI+CiAgICA8ZGl2IGNsYXNzPSJoZXJvLWJnLXRleHQiPkFQSTwvZGl2PgogICAgPGRpdiBjbGFzcz0iaGVyby1jaGlwIj48c3BhbiBjbGFzcz0iY2hpcC1kb3QiPjwvc3Bhbj5TTVMgwrcgT1RQIMK3IEFQSTwvZGl2PgogICAgPGgxIGNsYXNzPSJoZXJvLXRpdGxlIj4KICAgICAgPHNwYW4gY2xhc3M9InQxIj5LWS1TSElSTzwvc3Bhbj4KICAgICAgPHNwYW4gY2xhc3M9InQyIj5PRkZJQ0lBTDwvc3Bhbj4KICAgIDwvaDE+CiAgICA8cCBjbGFzcz0iaGVyby1zdWIiPk11bHRpLUFjY291bnQgwrcgTXVsdGktUmFuZ2UgwrcgUmVhbC10aW1lPC9wPgogICAgPHAgY2xhc3M9Imhlcm8tZGVzYyI+QVBJIGJ1YXQgYW1iaWwgT1RQIGRhcmkgaVZBUyBTTVMg4oCUIHN1cHBvcnQgYmFueWFrIGFrdW4gc2VrYWxpZ3VzLCBzZW11YSByYW5nZSAmIG5lZ2FyYSwgdGluZ2dhbCByZXF1ZXN0IGxhbmdzdW5nIGRhcGF0IGtvZGVueWEuPC9wPgogICAgPGRpdiBjbGFzcz0iaGVyby1jdGEiPgogICAgICA8YSBocmVmPSIjZG9jcyIgY2xhc3M9ImJ0bi1tYWluIj4KICAgICAgICA8c3ZnIHdpZHRoPSIxNSIgaGVpZ2h0PSIxNSIgdmlld0JveD0iMCAwIDE1IDE1IiBmaWxsPSJub25lIj48cGF0aCBkPSJNMiAzLjVoMTFNMiA3LjVoN00yIDExLjVoOSIgc3Ryb2tlPSJjdXJyZW50Q29sb3IiIHN0cm9rZS13aWR0aD0iMS42IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48L3N2Zz4KICAgICAgICBMaWhhdCBEb2t1bWVudGFzaQogICAgICA8L2E+CiAgICAgIDxidXR0b24gY2xhc3M9ImJ0bi1naG9zdCIgb25jbGljaz0iY2hlY2tTdGF0dXModHJ1ZSkiPgogICAgICAgIDxzdmcgd2lkdGg9IjE1IiBoZWlnaHQ9IjE1IiB2aWV3Qm94PSIwIDAgMTUgMTUiIGZpbGw9Im5vbmUiPjxjaXJjbGUgY3g9IjcuNSIgY3k9IjcuNSIgcj0iNS41IiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIxLjUiLz48cGF0aCBkPSJNNy41IDQuNXYzLjVsMiAxLjUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2Utd2lkdGg9IjEuNSIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIi8+PC9zdmc+CiAgICAgICAgQ2VrIFN0YXR1cyBMaXZlCiAgICAgIDwvYnV0dG9uPgogICAgPC9kaXY+CiAgPC9kaXY+Cjwvc2VjdGlvbj4KCjwhLS0gU1RBVFVTIEJBUiAtLT4KPGRpdiBjbGFzcz0id3JhcCIgc3R5bGU9InBhZGRpbmctYm90dG9tOjAiPgogIDxkaXYgY2xhc3M9InN0YXR1cy1iYXIgcmV2ZWFsIj4KICAgIDxkaXYgY2xhc3M9InNiLWl0ZW0iPgogICAgICA8ZGl2IGNsYXNzPSJzYi1sYWJlbCI+U3RhdHVzIEFQSTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJzYi12YWwiPjxzcGFuIGNsYXNzPSJzYi1kb3QgY2hlY2tpbmciIGlkPSJzRG90Ij48L3NwYW4+PHNwYW4gaWQ9InNUZXh0Ij5NZW5nZWNlay4uLjwvc3Bhbj48L2Rpdj4KICAgIDwvZGl2PgogICAgPGRpdiBjbGFzcz0ic2ItaXRlbSI+CiAgICAgIDxkaXYgY2xhc3M9InNiLWxhYmVsIj5pVkFTIExvZ2luPC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InNiLXZhbCIgaWQ9InNMb2dpbiI+4oCUPC9kaXY+CiAgICA8L2Rpdj4KICAgIDxkaXYgY2xhc3M9InNiLWl0ZW0iPgogICAgICA8ZGl2IGNsYXNzPSJzYi1sYWJlbCI+RGV2ZWxvcGVyPC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9InNiLXZhbCI+S2lraSBGYWl6YWw8L2Rpdj4KICAgIDwvZGl2PgogICAgPGRpdiBjbGFzcz0ic2ItaXRlbSI+CiAgICAgIDxkaXYgY2xhc3M9InNiLWxhYmVsIj5WZXJzaTwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJzYi12YWwiIHN0eWxlPSJmb250LWZhbWlseTp2YXIoLS1tb25vKTtjb2xvcjp2YXIoLS1ncmVlbik7Zm9udC1zaXplOjEzcHgiPnYyLjA8L2Rpdj4KICAgIDwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxkaXYgY2xhc3M9ImhyIiBzdHlsZT0ibWFyZ2luLXRvcDo2NHB4Ij48L2Rpdj4KCjwhLS0gQUJPVVQgLS0+CjxzZWN0aW9uIGNsYXNzPSJzZWN0aW9uIiBpZD0iYWJvdXQiPgogIDxkaXYgY2xhc3M9InNlY3Rpb24td3JhcCI+CiAgICA8ZGl2IGNsYXNzPSJzLWxhYmVsIj4vLyBUZW50YW5nPC9kaXY+CiAgICA8aDIgY2xhc3M9InMtdGl0bGUgcmV2ZWFsIj5BcGEgaXR1IEtZLVNISVJPIEFQST88L2gyPgogICAgPHAgY2xhc3M9InMtZGVzYyByZXZlYWwiPkFQSSBpbmkgbnlhbWJ1bmcgbGFuZ3N1bmcga2UgaVZBUyBTTVMsIHN1cHBvcnQgbXVsdGktYWt1biBiaWFyIG1ha2luIGJhbnlhayBub21vciB5YW5nIGJpc2EgZGlwYW50YXUuIENvY29rIGJhbmdldCBidWF0IGZvcndhcmQgT1RQIGtlIFRlbGVncmFtIGJvdCBhdGF1IGtlcGVybHVhbiBsYWluIHlhbmcgYnV0dWgga29kZSBTTVMgbWFzdWsuPC9wPgoKICAgIDxkaXYgY2xhc3M9InN0YXRzLXJvdyByZXZlYWwiPgogICAgICA8ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJzdGF0LW4iIGlkPSJzdFJhbmdlcyI+4oCUPC9kaXY+PGRpdiBjbGFzcz0ic3RhdC1sIj5SYW5nZSBBa3RpZjwvZGl2PjwvZGl2PgogICAgICA8ZGl2IGNsYXNzPSJzdGF0Ij48ZGl2IGNsYXNzPSJzdGF0LW4iIGlkPSJzdE51bWJlcnMiPuKAlDwvZGl2PjxkaXYgY2xhc3M9InN0YXQtbCI+Tm9tb3IgVGVyc2VkaWE8L2Rpdj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0ic3RhdC1uIj44PC9kaXY+PGRpdiBjbGFzcz0ic3RhdC1sIj5FbmRwb2ludCBBUEk8L2Rpdj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0ic3RhdCI+PGRpdiBjbGFzcz0ic3RhdC1uIj7iiJ48L2Rpdj48ZGl2IGNsYXNzPSJzdGF0LWwiPk5lZ2FyYSBTdXBwb3J0PC9kaXY+PC9kaXY+CiAgICA8L2Rpdj4KCiAgICA8ZGl2IGNsYXNzPSJhYm91dC1ncmlkIHJldmVhbCI+CiAgICAgIDxkaXYgY2xhc3M9ImFib3V0LWNhcmQiPjxkaXYgY2xhc3M9ImFjLWVtIj7imqE8L2Rpdj48ZGl2IGNsYXNzPSJhYy10Ij5SZWFsLXRpbWU8L2Rpdj48ZGl2IGNsYXNzPSJhYy1kIj5PVFAgeWFuZyBtYXN1ayBsYW5nc3VuZyBiaXNhIGRpYW1iaWwgdGFucGEgZGVsYXksIHNlbXVhIHJhbmdlIHNla2FsaWd1cy48L2Rpdj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iYWJvdXQtY2FyZCI+PGRpdiBjbGFzcz0iYWMtZW0iPvCfkaU8L2Rpdj48ZGl2IGNsYXNzPSJhYy10Ij5NdWx0aS1Ba3VuPC9kaXY+PGRpdiBjbGFzcz0iYWMtZCI+QmlzYSBsb2dpbiBrZSBiYW55YWsgYWt1biBpVkFTIHNla2FsaWd1cywgc2VtdWEgcmFuZ2UgZGFyaSBzZW11YSBha3VuIGRpZ2FidW5nIGphZGkgc2F0dSByZXNwb25zZS48L2Rpdj48L2Rpdj4KICAgICAgPGRpdiBjbGFzcz0iYWJvdXQtY2FyZCI+PGRpdiBjbGFzcz0iYWMtZW0iPvCfjI08L2Rpdj48ZGl2IGNsYXNzPSJhYy10Ij5NdWx0aSBOZWdhcmE8L2Rpdj48ZGl2IGNsYXNzPSJhYy1kIj5Jdm9yeSBDb2FzdCwgWmltYmFid2UsIFRvZ28sIE1hZGFnYXNjYXIg4oCUIHNlbXVhIHJhbmdlIHlhbmcgYWRhIGRpIGFrdW4gbG8gbWFzdWsgc2VtdWEuPC9kaXY+PC9kaXY+CiAgICAgIDxkaXYgY2xhc3M9ImFib3V0LWNhcmQiPjxkaXYgY2xhc3M9ImFjLWVtIj7wn6SWPC9kaXY+PGRpdiBjbGFzcz0iYWMtdCI+Qm90LXJlYWR5PC9kaXY+PGRpdiBjbGFzcz0iYWMtZCI+UmVzcG9uc2UgSlNPTiBiZXJzaWggZGFuIGtvbnNpc3RlbiwgbGFuZ3N1bmcgYmlzYSBkaXBha2FpIHNhbWEgVGVsZWdyYW0gYm90IHRhbnBhIHByZXByb2Nlc3NpbmcuPC9kaXY+PC9kaXY+CiAgICA8L2Rpdj4KICA8L2Rpdj4KPC9zZWN0aW9uPgoKPGRpdiBjbGFzcz0iaHIiPjwvZGl2PgoKPCEtLSBET0NTIC0tPgo8c2VjdGlvbiBjbGFzcz0ic2VjdGlvbiIgaWQ9ImRvY3MiPgogIDxkaXYgY2xhc3M9InNlY3Rpb24td3JhcCI+CiAgICA8ZGl2IGNsYXNzPSJzLWxhYmVsIj4vLyBEb2t1bWVudGFzaTwvZGl2PgogICAgPGgyIGNsYXNzPSJzLXRpdGxlIHJldmVhbCI+U2VtdWEgRW5kcG9pbnQ8L2gyPgogICAgPHAgY2xhc3M9InMtZGVzYyByZXZlYWwiPkJhc2UgVVJMOiA8Y29kZSBzdHlsZT0iZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Y29sb3I6dmFyKC0tZ3JlZW4pO2ZvbnQtc2l6ZToxM3B4O2JhY2tncm91bmQ6dmFyKC0tY2FyZCk7cGFkZGluZzoycHggOHB4O2JvcmRlci1yYWRpdXM6NXB4Ij5odHRwczovL2FwaWt5c2hpcm8udmVyY2VsLmFwcDwvY29kZT48L3A+CgogICAgPGRpdiBjbGFzcz0iZXAtbGlzdCByZXZlYWwiPgoKICAgICAgPCEtLSAvc21zIC0tPgogICAgICA8ZGl2IGNsYXNzPSJlcCI+CiAgICAgICAgPGRpdiBjbGFzcz0iZXAtaGVhZCIgb25jbGljaz0idG9nZ2xlRXAodGhpcykiPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLW1ldGhvZCBHRVQiPkdFVDwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1wYXRoIj4vc21zPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLXNob3J0Ij5BbWJpbCBPVFAgYmVyZGFzYXJrYW4gdGFuZ2dhbDwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1hcnJvdyI+4pa+PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImVwLWJvZHkiPgogICAgICAgICAgPGRpdiBjbGFzcz0icHQiPlBhcmFtZXRlcjwvZGl2PgogICAgICAgICAgPHRhYmxlIGNsYXNzPSJwdGFibGUiPgogICAgICAgICAgICA8dHI+PHRoPk5hbWE8L3RoPjx0aD5UaXBlPC90aD48dGg+U3RhdHVzPC90aD48dGg+S2V0ZXJhbmdhbjwvdGg+PC90cj4KICAgICAgICAgICAgPHRyPjx0ZD5kYXRlPC90ZD48dGQ+c3RyaW5nPC90ZD48dGQ+PHNwYW4gY2xhc3M9ImJyIGJyLXIiPldBSklCPC9zcGFuPjwvdGQ+PHRkPkZvcm1hdCBERC9NTS9ZWVlZIOKAlCB0YW5nZ2FsIHlhbmcgZGljZWs8L3RkPjwvdHI+CiAgICAgICAgICAgIDx0cj48dGQ+bW9kZTwvdGQ+PHRkPnN0cmluZzwvdGQ+PHRkPjxzcGFuIGNsYXNzPSJiciBici1vIj5PUFNJT05BTDwvc3Bhbj48L3RkPjx0ZD48Y29kZT5yZWNlaXZlZDwvY29kZT4gLyA8Y29kZT5saXZlPC9jb2RlPiAvIDxjb2RlPmJvdGg8L2NvZGU+IOKAlCBkZWZhdWx0OiByZWNlaXZlZDwvdGQ+PC90cj4KICAgICAgICAgIDwvdGFibGU+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwdCI+Q29udG9oIFJlcXVlc3Q8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9ImNvZGUiIHN0eWxlPSJwb3NpdGlvbjpyZWxhdGl2ZSI+PGJ1dHRvbiBjbGFzcz0iY3AtYnRuIiBvbmNsaWNrPSJjcCh0aGlzKSI+Y29weTwvYnV0dG9uPkdFVCAvc21zPzxzcGFuIGNsYXNzPSJreSI+ZGF0ZTwvc3Bhbj49PHNwYW4gY2xhc3M9InMiPjA3LzAzLzIwMjY8L3NwYW4+JjxzcGFuIGNsYXNzPSJreSI+bW9kZTwvc3Bhbj49PHNwYW4gY2xhc3M9InMiPnJlY2VpdmVkPC9zcGFuPjwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0icHQiPkNvbnRvaCBSZXNwb25zZTwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0iY29kZSI+ewogIDxzcGFuIGNsYXNzPSJreSI+InN0YXR1cyI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+InN1Y2Nlc3MiPC9zcGFuPiwKICA8c3BhbiBjbGFzcz0ia3kiPiJtb2RlIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4icmVjZWl2ZWQiPC9zcGFuPiwKICA8c3BhbiBjbGFzcz0ia3kiPiJ0b3RhbCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0idiI+NTwvc3Bhbj4sCiAgPHNwYW4gY2xhc3M9Imt5Ij4iYWNjb3VudHNfdXNlZCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0idiI+Mjwvc3Bhbj4sCiAgPHNwYW4gY2xhc3M9Imt5Ij4ib3RwX21lc3NhZ2VzIjwvc3Bhbj46IFsKICAgIHsKICAgICAgPHNwYW4gY2xhc3M9Imt5Ij4icmFuZ2UiPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiJJVk9SWSBDT0FTVCAzODc4Ijwvc3Bhbj4sCiAgICAgIDxzcGFuIGNsYXNzPSJreSI+InBob25lX251bWJlciI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+IjIyNTA3MTEyMjA5NzAiPC9zcGFuPiwKICAgICAgPHNwYW4gY2xhc3M9Imt5Ij4ib3RwX21lc3NhZ2UiPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiJZb3VyIFdoYXRzQXBwIGNvZGU6IDMzOC02NDAiPC9zcGFuPiwKICAgICAgPHNwYW4gY2xhc3M9Imt5Ij4ic291cmNlIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4icmVjZWl2ZWQiPC9zcGFuPiwKICAgICAgPHNwYW4gY2xhc3M9Imt5Ij4iYWNjb3VudCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+IjxhIGhyZWY9Ii9jZG4tY2dpL2wvZW1haWwtcHJvdGVjdGlvbiIgY2xhc3M9Il9fY2ZfZW1haWxfXyIgZGF0YS1jZmVtYWlsPSI1YjNhMzAyZTM1NmExYjNjMzYzYTMyMzc3NTM4MzQzNiI+W2VtYWlsJiMxNjA7cHJvdGVjdGVkXTwvYT4iPC9zcGFuPgogICAgfQogIF0KfTwvZGl2PgogICAgICAgIDwvZGl2PgogICAgICA8L2Rpdj4KCiAgICAgIDwhLS0gL2hlYWx0aCAtLT4KICAgICAgPGRpdiBjbGFzcz0iZXAiPgogICAgICAgIDxkaXYgY2xhc3M9ImVwLWhlYWQiIG9uY2xpY2s9InRvZ2dsZUVwKHRoaXMpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1tZXRob2QgR0VUIj5HRVQ8L3NwYW4+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtcGF0aCI+L2hlYWx0aDwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1zaG9ydCI+Q2VrIHN0YXR1cyBsb2dpbiBzZW11YSBha3VuPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLWFycm93Ij7ilr48L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iZXAtYm9keSI+CiAgICAgICAgICA8cCBzdHlsZT0iY29sb3I6dmFyKC0taW5rMik7Zm9udC1zaXplOjE0cHg7bWFyZ2luLXRvcDoxNHB4Ij5DZWsgYXBha2FoIEFQSSBiZXJoYXNpbCBsb2dpbiBrZSBpVkFTLiBLYWxhdSA8Y29kZSBzdHlsZT0iY29sb3I6dmFyKC0tZ3JlZW4pIj5sb2dpbjogInN1Y2Nlc3MiPC9jb2RlPiBiZXJhcnRpIHNpYXAgdGVyaW1hIHJlcXVlc3QuPC9wPgogICAgICAgICAgPGRpdiBjbGFzcz0icHQiPkNvbnRvaCBSZXNwb25zZTwvZGl2PgogICAgICAgICAgPGRpdiBjbGFzcz0iY29kZSI+ewogIDxzcGFuIGNsYXNzPSJreSI+InN0YXR1cyI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+Im9rIjwvc3Bhbj4sCiAgPHNwYW4gY2xhc3M9Imt5Ij4ibG9naW4iPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiJzdWNjZXNzIjwvc3Bhbj4sCiAgPHNwYW4gY2xhc3M9Imt5Ij4iYWNjb3VudHNfb2siPC9zcGFuPjogPHNwYW4gY2xhc3M9InYiPjI8L3NwYW4+LAogIDxzcGFuIGNsYXNzPSJreSI+ImFjY291bnRzX3RvdGFsIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJ2Ij4yPC9zcGFuPiwKICA8c3BhbiBjbGFzcz0ia3kiPiJkZXRhaWxzIjwvc3Bhbj46IFsKICAgIHsgPHNwYW4gY2xhc3M9Imt5Ij4iZW1haWwiPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiI8YSBocmVmPSIvY2RuLWNnaS9sL2VtYWlsLXByb3RlY3Rpb24iIGNsYXNzPSJfX2NmX2VtYWlsX18iIGRhdGEtY2ZlbWFpbD0iZWU4Zjg1OWI4MGRmYWU4OTgzOGY4NzgyYzA4ZDgxODMiPltlbWFpbCYjMTYwO3Byb3RlY3RlZF08L2E+Ijwvc3Bhbj4sIDxzcGFuIGNsYXNzPSJreSI+ImxvZ2luIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4ic3VjY2VzcyI8L3NwYW4+IH0sCiAgICB7IDxzcGFuIGNsYXNzPSJreSI+ImVtYWlsIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4iPGEgaHJlZj0iL2Nkbi1jZ2kvbC9lbWFpbC1wcm90ZWN0aW9uIiBjbGFzcz0iX19jZl9lbWFpbF9fIiBkYXRhLWNmZW1haWw9IjA2Njc2ZDczNjgzNDQ2NjE2YjY3NmY2YTI4NjU2OTZiIj5bZW1haWwmIzE2MDtwcm90ZWN0ZWRdPC9hPiI8L3NwYW4+LCA8c3BhbiBjbGFzcz0ia3kiPiJsb2dpbiI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+InN1Y2Nlc3MiPC9zcGFuPiB9CiAgXQp9PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgICAgPCEtLSAvYWNjb3VudHMgLS0+CiAgICAgIDxkaXYgY2xhc3M9ImVwIj4KICAgICAgICA8ZGl2IGNsYXNzPSJlcC1oZWFkIiBvbmNsaWNrPSJ0b2dnbGVFcCh0aGlzKSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtbWV0aG9kIEdFVCI+R0VUPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLXBhdGgiPi9hY2NvdW50czwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1zaG9ydCI+TGlzdCBha3VuIHRlcmRhZnRhcjwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1hcnJvdyI+4pa+PC9zcGFuPgogICAgICAgIDwvZGl2PgogICAgICAgIDxkaXYgY2xhc3M9ImVwLWJvZHkiPgogICAgICAgICAgPHAgc3R5bGU9ImNvbG9yOnZhcigtLWluazIpO2ZvbnQtc2l6ZToxNHB4O21hcmdpbi10b3A6MTRweCI+TGloYXQgYmVyYXBhIGFrdW4geWFuZyB0ZXJkYWZ0YXIgZGkgQVBJLiBQYXNzd29yZCB0aWRhayBkaXRhbXBpbGthbi48L3A+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwdCI+Q29udG9oIFJlc3BvbnNlPC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJjb2RlIj57CiAgPHNwYW4gY2xhc3M9Imt5Ij4idG90YWwiPC9zcGFuPjogPHNwYW4gY2xhc3M9InYiPjI8L3NwYW4+LAogIDxzcGFuIGNsYXNzPSJreSI+ImFjY291bnRzIjwvc3Bhbj46IFsKICAgIHsgPHNwYW4gY2xhc3M9Imt5Ij4iaW5kZXgiPC9zcGFuPjogPHNwYW4gY2xhc3M9InYiPjE8L3NwYW4+LCA8c3BhbiBjbGFzcz0ia3kiPiJlbWFpbCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+IjxhIGhyZWY9Ii9jZG4tY2dpL2wvZW1haWwtcHJvdGVjdGlvbiIgY2xhc3M9Il9fY2ZfZW1haWxfXyIgZGF0YS1jZmVtYWlsPSJiZGRjZDZjOGQzOGNmZGRhZDBkY2Q0ZDE5M2RlZDJkMCI+W2VtYWlsJiMxNjA7cHJvdGVjdGVkXTwvYT4iPC9zcGFuPiB9LAogICAgeyA8c3BhbiBjbGFzcz0ia3kiPiJpbmRleCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0idiI+Mjwvc3Bhbj4sIDxzcGFuIGNsYXNzPSJreSI+ImVtYWlsIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4iPGEgaHJlZj0iL2Nkbi1jZ2kvbC9lbWFpbC1wcm90ZWN0aW9uIiBjbGFzcz0iX19jZl9lbWFpbF9fIiBkYXRhLWNmZW1haWw9IjlhZmJmMWVmZjRhOGRhZmRmN2ZiZjNmNmI0ZjlmNWY3Ij5bZW1haWwmIzE2MDtwcm90ZWN0ZWRdPC9hPiI8L3NwYW4+IH0KICBdCn08L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8IS0tIC90ZXN0IC0tPgogICAgICA8ZGl2IGNsYXNzPSJlcCI+CiAgICAgICAgPGRpdiBjbGFzcz0iZXAtaGVhZCIgb25jbGljaz0idG9nZ2xlRXAodGhpcykiPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLW1ldGhvZCBHRVQiPkdFVDwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1wYXRoIj4vdGVzdDwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1zaG9ydCI+Q2VrIHNlbXVhIHJhbmdlICYgbm9tb3I8L3NwYW4+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtYXJyb3ciPuKWvjwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJlcC1ib2R5Ij4KICAgICAgICAgIDxkaXYgY2xhc3M9InB0Ij5QYXJhbWV0ZXI8L2Rpdj4KICAgICAgICAgIDx0YWJsZSBjbGFzcz0icHRhYmxlIj4KICAgICAgICAgICAgPHRyPjx0aD5OYW1hPC90aD48dGg+VGlwZTwvdGg+PHRoPlN0YXR1czwvdGg+PHRoPktldGVyYW5nYW48L3RoPjwvdHI+CiAgICAgICAgICAgIDx0cj48dGQ+ZGF0ZTwvdGQ+PHRkPnN0cmluZzwvdGQ+PHRkPjxzcGFuIGNsYXNzPSJiciBici1vIj5PUFNJT05BTDwvc3Bhbj48L3RkPjx0ZD5Gb3JtYXQgREQvTU0vWVlZWSDigJQgZGVmYXVsdDogaGFyaSBpbmk8L3RkPjwvdHI+CiAgICAgICAgICA8L3RhYmxlPgogICAgICAgICAgPGRpdiBjbGFzcz0icHQiPkNvbnRvaCBSZXF1ZXN0PC9kaXY+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJjb2RlIiBzdHlsZT0icG9zaXRpb246cmVsYXRpdmUiPjxidXR0b24gY2xhc3M9ImNwLWJ0biIgb25jbGljaz0iY3AodGhpcykiPmNvcHk8L2J1dHRvbj5HRVQgL3Rlc3Q/PHNwYW4gY2xhc3M9Imt5Ij5kYXRlPC9zcGFuPj08c3BhbiBjbGFzcz0icyI+MDcvMDMvMjAyNjwvc3Bhbj48L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8IS0tIC90ZXN0L3NtcyAtLT4KICAgICAgPGRpdiBjbGFzcz0iZXAiPgogICAgICAgIDxkaXYgY2xhc3M9ImVwLWhlYWQiIG9uY2xpY2s9InRvZ2dsZUVwKHRoaXMpIj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1tZXRob2QgR0VUIj5HRVQ8L3NwYW4+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtcGF0aCI+L3Rlc3Qvc21zPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLXNob3J0Ij5DZWsgT1RQIHVudHVrIDEgbm9tb3Igc3Blc2lmaWs8L3NwYW4+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtYXJyb3ciPuKWvjwvc3Bhbj4KICAgICAgICA8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJlcC1ib2R5Ij4KICAgICAgICAgIDxkaXYgY2xhc3M9InB0Ij5QYXJhbWV0ZXI8L2Rpdj4KICAgICAgICAgIDx0YWJsZSBjbGFzcz0icHRhYmxlIj4KICAgICAgICAgICAgPHRyPjx0aD5OYW1hPC90aD48dGg+VGlwZTwvdGg+PHRoPlN0YXR1czwvdGg+PHRoPktldGVyYW5nYW48L3RoPjwvdHI+CiAgICAgICAgICAgIDx0cj48dGQ+ZGF0ZTwvdGQ+PHRkPnN0cmluZzwvdGQ+PHRkPjxzcGFuIGNsYXNzPSJiciBici1vIj5PUFNJT05BTDwvc3Bhbj48L3RkPjx0ZD5Gb3JtYXQgREQvTU0vWVlZWTwvdGQ+PC90cj4KICAgICAgICAgICAgPHRyPjx0ZD5yYW5nZTwvdGQ+PHRkPnN0cmluZzwvdGQ+PHRkPjxzcGFuIGNsYXNzPSJiciBici1yIj5XQUpJQjwvc3Bhbj48L3RkPjx0ZD5OYW1hIHJhbmdlLCBjb250b2g6IElWT1JZIENPQVNUIDM4Nzg8L3RkPjwvdHI+CiAgICAgICAgICAgIDx0cj48dGQ+bnVtYmVyPC90ZD48dGQ+c3RyaW5nPC90ZD48dGQ+PHNwYW4gY2xhc3M9ImJyIGJyLXIiPldBSklCPC9zcGFuPjwvdGQ+PHRkPk5vbW9yIHRlbGVwb24sIGNvbnRvaDogMjI1MDcxMTIyMDk3MDwvdGQ+PC90cj4KICAgICAgICAgIDwvdGFibGU+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwdCI+Q29udG9oIFJlcXVlc3Q8L2Rpdj4KICAgICAgICAgIDxkaXYgY2xhc3M9ImNvZGUiIHN0eWxlPSJwb3NpdGlvbjpyZWxhdGl2ZSI+PGJ1dHRvbiBjbGFzcz0iY3AtYnRuIiBvbmNsaWNrPSJjcCh0aGlzKSI+Y29weTwvYnV0dG9uPkdFVCAvdGVzdC9zbXM/PHNwYW4gY2xhc3M9Imt5Ij5kYXRlPC9zcGFuPj08c3BhbiBjbGFzcz0icyI+MDcvMDMvMjAyNjwvc3Bhbj4mPHNwYW4gY2xhc3M9Imt5Ij5yYW5nZTwvc3Bhbj49PHNwYW4gY2xhc3M9InMiPklWT1JZIENPQVNUIDM4Nzg8L3NwYW4+JjxzcGFuIGNsYXNzPSJreSI+bnVtYmVyPC9zcGFuPj08c3BhbiBjbGFzcz0icyI+MjI1MDcxMTIyMDk3MDwvc3Bhbj48L2Rpdj4KICAgICAgICA8L2Rpdj4KICAgICAgPC9kaXY+CgogICAgICA8IS0tIC9kZWJ1ZyBlbmRwb2ludHMgLS0+CiAgICAgIDxkaXYgY2xhc3M9ImVwIj4KICAgICAgICA8ZGl2IGNsYXNzPSJlcC1oZWFkIiBvbmNsaWNrPSJ0b2dnbGVFcCh0aGlzKSI+CiAgICAgICAgICA8c3BhbiBjbGFzcz0iZXAtbWV0aG9kIEdFVCI+R0VUPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLXBhdGgiPi9kZWJ1Zy9yYW5nZXMtcmF3ICZuYnNwOyAvZGVidWcvbnVtYmVycyAmbmJzcDsgL2RlYnVnL3Ntczwvc3Bhbj4KICAgICAgICAgIDxzcGFuIGNsYXNzPSJlcC1zaG9ydCI+RGVidWcgZW5kcG9pbnRzPC9zcGFuPgogICAgICAgICAgPHNwYW4gY2xhc3M9ImVwLWFycm93Ij7ilr48L3NwYW4+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdiBjbGFzcz0iZXAtYm9keSI+CiAgICAgICAgICA8cCBzdHlsZT0iY29sb3I6dmFyKC0taW5rMik7Zm9udC1zaXplOjE0cHg7bWFyZ2luLXRvcDoxNHB4Ij5UaWdhIGVuZHBvaW50IGtodXN1cyBidWF0IGRlYnVnIGthbGF1IGFkYSB5YW5nIHRpZGFrIGtlZGV0ZWtzaSBhdGF1IFNNUyB0aWRhayBtYXN1ay48L3A+CiAgICAgICAgICA8ZGl2IGNsYXNzPSJwdCI+RW5kcG9pbnQgRGVidWc8L2Rpdj4KICAgICAgICAgIDx0YWJsZSBjbGFzcz0icHRhYmxlIj4KICAgICAgICAgICAgPHRyPjx0aD5FbmRwb2ludDwvdGg+PHRoPlBhcmFtZXRlciBXYWppYjwvdGg+PHRoPkZ1bmdzaTwvdGg+PC90cj4KICAgICAgICAgICAgPHRyPjx0ZD4vZGVidWcvcmFuZ2VzLXJhdzwvdGQ+PHRkPmRhdGU8L3RkPjx0ZD5SYXcgSFRNTCBkYXJpIGlWQVMgYnVhdCBjZWsga2VuYXBhIHJhbmdlIHRpZGFrIG11bmN1bDwvdGQ+PC90cj4KICAgICAgICAgICAgPHRyPjx0ZD4vZGVidWcvbnVtYmVyczwvdGQ+PHRkPmRhdGUsIHJhbmdlPC90ZD48dGQ+Q2VrIG5vbW9yIGRhcmkgcmFuZ2UgdGVydGVudHUgYmVzZXJ0YSByYXcgcmVzcG9uc2U8L3RkPjwvdHI+CiAgICAgICAgICAgIDx0cj48dGQ+L2RlYnVnL3NtczwvdGQ+PHRkPmRhdGUsIHJhbmdlLCBudW1iZXI8L3RkPjx0ZD5DZWsgcmF3IHJlc3BvbnNlIFNNUyB1bnR1ayBub21vciB0ZXJ0ZW50dTwvdGQ+PC90cj4KICAgICAgICAgIDwvdGFibGU+CiAgICAgICAgPC9kaXY+CiAgICAgIDwvZGl2PgoKICAgIDwvZGl2PjwhLS0gZW5kIGVwLWxpc3QgLS0+CgogICAgPCEtLSBNdWx0aS1hY2NvdW50IGd1aWRlIC0tPgogICAgPGRpdiBzdHlsZT0ibWFyZ2luLXRvcDozMnB4O2JhY2tncm91bmQ6dmFyKC0tY2FyZCk7Ym9yZGVyOjFweCBzb2xpZCB2YXIoLS1saW5lKTtib3JkZXItcmFkaXVzOnZhcigtLXIpO3BhZGRpbmc6MjRweCIgY2xhc3M9InJldmVhbCI+CiAgICAgIDxkaXYgY2xhc3M9InMtbGFiZWwiIHN0eWxlPSJtYXJnaW4tYm90dG9tOjEwcHgiPi8vIENhcmEgVGFtYmFoIEFrdW48L2Rpdj4KICAgICAgPHAgc3R5bGU9ImNvbG9yOnZhcigtLWluazIpO2ZvbnQtc2l6ZToxNHB4O2xpbmUtaGVpZ2h0OjEuNzttYXJnaW4tYm90dG9tOjE0cHgiPkJ1a2EgPGNvZGUgc3R5bGU9ImZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2NvbG9yOnZhcigtLWdyZWVuKSI+YXBwLnB5PC9jb2RlPiwgY2FyaSBiYWdpYW4gPGNvZGUgc3R5bGU9ImZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2NvbG9yOnZhcigtLWdyZWVuKSI+bG9hZF9hY2NvdW50cygpPC9jb2RlPiwgdGFtYmFoIGFrdW4gYmFydSBkaSBsaXN0OjwvcD4KICAgICAgPGRpdiBjbGFzcz0iY29kZSI+cmV0dXJuIFsKICAgIHs8c3BhbiBjbGFzcz0ia3kiPiJlbWFpbCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+IjxhIGhyZWY9Ii9jZG4tY2dpL2wvZW1haWwtcHJvdGVjdGlvbiIgY2xhc3M9Il9fY2ZfZW1haWxfXyIgZGF0YS1jZmVtYWlsPSJlNDg1OGY5MThhZDVhNDgzODk4NThkODhjYTg3OGI4OSI+W2VtYWlsJiMxNjA7cHJvdGVjdGVkXTwvYT4iPC9zcGFuPiwgPHNwYW4gY2xhc3M9Imt5Ij4icGFzc3dvcmQiPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiJwYXNzd29yZDEiPC9zcGFuPn0sCiAgICB7PHNwYW4gY2xhc3M9Imt5Ij4iZW1haWwiPC9zcGFuPjogPHNwYW4gY2xhc3M9InMiPiI8YSBocmVmPSIvY2RuLWNnaS9sL2VtYWlsLXByb3RlY3Rpb24iIGNsYXNzPSJfX2NmX2VtYWlsX18iIGRhdGEtY2ZlbWFpbD0iMzE1MDVhNDQ1ZjAzNzE1NjVjNTA1ODVkMWY1MjVlNWMiPltlbWFpbCYjMTYwO3Byb3RlY3RlZF08L2E+Ijwvc3Bhbj4sIDxzcGFuIGNsYXNzPSJreSI+InBhc3N3b3JkIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4icGFzc3dvcmQyIjwvc3Bhbj59LCAgPHNwYW4gY2xhc3M9ImMiPiMg4oaQIHRhbWJhaCBkaSBzaW5pPC9zcGFuPgogICAgezxzcGFuIGNsYXNzPSJreSI+ImVtYWlsIjwvc3Bhbj46IDxzcGFuIGNsYXNzPSJzIj4iPGEgaHJlZj0iL2Nkbi1jZ2kvbC9lbWFpbC1wcm90ZWN0aW9uIiBjbGFzcz0iX19jZl9lbWFpbF9fIiBkYXRhLWNmZW1haWw9IjJmNGU0NDVhNDExYzZmNDg0MjRlNDY0MzAxNGM0MDQyIj5bZW1haWwmIzE2MDtwcm90ZWN0ZWRdPC9hPiI8L3NwYW4+LCA8c3BhbiBjbGFzcz0ia3kiPiJwYXNzd29yZCI8L3NwYW4+OiA8c3BhbiBjbGFzcz0icyI+InBhc3N3b3JkMyI8L3NwYW4+fSwgIDxzcGFuIGNsYXNzPSJjIj4jIOKGkCBhdGF1IGRpIHNpbmk8L3NwYW4+Cl08L2Rpdj4KICAgICAgPHAgc3R5bGU9ImNvbG9yOnZhcigtLWluazIpO2ZvbnQtc2l6ZToxM3B4O21hcmdpbi10b3A6MTJweCI+QXRhdSBwYWthaSBlbnZpcm9ubWVudCB2YXJpYWJsZSBkaSBWZXJjZWw6IDxjb2RlIHN0eWxlPSJmb250LWZhbWlseTp2YXIoLS1tb25vKTtjb2xvcjp2YXIoLS15ZWxsb3cpIj5JVkFTX0FDQ09VTlRTID0gZW1haWwxOnBhc3MxLGVtYWlsMjpwYXNzMjwvY29kZT48L3A+CiAgICA8L2Rpdj4KCiAgPC9kaXY+Cjwvc2VjdGlvbj4KCjxkaXYgY2xhc3M9ImhyIj48L2Rpdj4KCjwhLS0gQ09OVEFDVCAtLT4KPHNlY3Rpb24gY2xhc3M9InNlY3Rpb24iIGlkPSJjb250YWN0Ij4KICA8ZGl2IGNsYXNzPSJzZWN0aW9uLXdyYXAiPgogICAgPGRpdiBjbGFzcz0icy1sYWJlbCI+Ly8gSHVidW5naSBLYW1pPC9kaXY+CiAgICA8aDIgY2xhc3M9InMtdGl0bGUgcmV2ZWFsIj5BZGEgeWFuZyBtYXUgZGl0YW55YT88L2gyPgogICAgPHAgY2xhc3M9InMtZGVzYyByZXZlYWwiPkJ1ZywgcmVxdWVzdCBmaXR1ciwgYXRhdSBzZWtlZGFyIG1hdSBrZW5hbGFuIOKAlCBsYW5nc3VuZyBhamEga29udGFrIGRldmVsb3Blcm55YS48L3A+CiAgICA8ZGl2IGNsYXNzPSJjb250YWN0LWdyaWQgcmV2ZWFsIj4KICAgICAgPGEgaHJlZj0iaHR0cHM6Ly90Lm1lL3VzZXJuYW1lX2tpa2kiIHRhcmdldD0iX2JsYW5rIiBjbGFzcz0iY2MiPgogICAgICAgIDxkaXYgY2xhc3M9ImNjLWljb24gYmctdGciPuKciO+4jzwvZGl2PgogICAgICAgIDxkaXY+PGRpdiBjbGFzcz0iY2MtdCI+VGVsZWdyYW08L2Rpdj48ZGl2IGNsYXNzPSJjYy1zIj5AS2lraUZhaXphbDwvZGl2PjwvZGl2PgogICAgICA8L2E+CiAgICAgIDxhIGhyZWY9Imh0dHBzOi8vd2EubWUvNjJ4eHh4eHh4eCIgdGFyZ2V0PSJfYmxhbmsiIGNsYXNzPSJjYyI+CiAgICAgICAgPGRpdiBjbGFzcz0iY2MtaWNvbiBiZy13YSI+8J+SrDwvZGl2PgogICAgICAgIDxkaXY+PGRpdiBjbGFzcz0iY2MtdCI+V2hhdHNBcHA8L2Rpdj48ZGl2IGNsYXNzPSJjYy1zIj5DaGF0IHZpYSBXQTwvZGl2PjwvZGl2PgogICAgICA8L2E+CiAgICAgIDxkaXYgY2xhc3M9ImNjIiBvbmNsaWNrPSJvcGVuTW9kYWwoJ2Rldk1vZGFsJykiIHN0eWxlPSJjdXJzb3I6cG9pbnRlciI+CiAgICAgICAgPGRpdiBjbGFzcz0iY2MtaWNvbiBiZy1kZXYiPvCfkaQ8L2Rpdj4KICAgICAgICA8ZGl2PjxkaXYgY2xhc3M9ImNjLXQiPkRldmVsb3BlcjwvZGl2PjxkaXYgY2xhc3M9ImNjLXMiPktpa2kgRmFpemFsPC9kaXY+PC9kaXY+CiAgICAgIDwvZGl2PgogICAgPC9kaXY+CiAgPC9kaXY+Cjwvc2VjdGlvbj4KCjwhLS0gRk9PVEVSIC0tPgo8Zm9vdGVyPgogIDxkaXYgY2xhc3M9ImZvb3QtbmFtZSI+S1ktPGI+U0hJUk88L2I+IE9GRklDSUFMPC9kaXY+CiAgPGRpdiBjbGFzcz0iZm9vdC1zdWIiPgogICAgTWFkZSBieSA8YSBocmVmPSIjIj5LaWtpIEZhaXphbDwvYT4gJm5ic3A7wrcmbmJzcDsKICAgIFBvd2VyZWQgYnkgPGEgaHJlZj0iaHR0cHM6Ly93d3cuaXZhc21zLmNvbSIgdGFyZ2V0PSJfYmxhbmsiPmlWQVMgU01TPC9hPiAmbmJzcDvCtyZuYnNwOwogICAgSG9zdGVkIG9uIDxhIGhyZWY9Imh0dHBzOi8vdmVyY2VsLmNvbSIgdGFyZ2V0PSJfYmxhbmsiPlZlcmNlbDwvYT4KICA8L2Rpdj4KPC9mb290ZXI+Cgo8IS0tIE1PREFMIERFVkVMT1BFUiAtLT4KPGRpdiBjbGFzcz0ib3ZlcmxheSIgaWQ9ImRldk1vZGFsIiBvbmNsaWNrPSJpZihldmVudC50YXJnZXQ9PT10aGlzKWNsb3NlTW9kYWwoJ2Rldk1vZGFsJykiPgogIDxkaXYgY2xhc3M9Im1vZGFsIj4KICAgIDxidXR0b24gY2xhc3M9Im1vZGFsLXgiIG9uY2xpY2s9ImNsb3NlTW9kYWwoJ2Rldk1vZGFsJykiPuKclTwvYnV0dG9uPgogICAgPGRpdiBjbGFzcz0ibW9kYWwtdCI+RGV2ZWxvcGVyPC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJtb2RhbC1kIj5PcmFuZyBkaSBiYWxpayBLWS1TSElSTyBBUEkuIEthbGF1IGFkYSBtYXNhbGFoIGxhbmdzdW5nIHRlbWJhayBhamEuPC9kaXY+CiAgICA8ZGl2IGNsYXNzPSJkZXYtY2FyZCI+CiAgICAgIDxkaXYgY2xhc3M9ImRldi1hdiI+S0Y8L2Rpdj4KICAgICAgPGRpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJkZXYtbiI+S2lraSBGYWl6YWw8L2Rpdj4KICAgICAgICA8ZGl2IGNsYXNzPSJkZXYtciI+Ly8gQmFja2VuZCDCtyBBUEkgRW5naW5lZXI8L2Rpdj4KICAgICAgPC9kaXY+CiAgICA8L2Rpdj4KICA8L2Rpdj4KPC9kaXY+Cgo8IS0tIE1PREFMIFNUQVRVUyAtLT4KPGRpdiBjbGFzcz0ib3ZlcmxheSIgaWQ9InN0YXR1c01vZGFsIiBvbmNsaWNrPSJpZihldmVudC50YXJnZXQ9PT10aGlzKWNsb3NlTW9kYWwoJ3N0YXR1c01vZGFsJykiPgogIDxkaXYgY2xhc3M9Im1vZGFsIj4KICAgIDxidXR0b24gY2xhc3M9Im1vZGFsLXgiIG9uY2xpY2s9ImNsb3NlTW9kYWwoJ3N0YXR1c01vZGFsJykiPuKclTwvYnV0dG9uPgogICAgPGRpdiBjbGFzcz0ibW9kYWwtdCI+U3RhdHVzIExpdmU8L2Rpdj4KICAgIDxkaXYgaWQ9InN0YXR1c01vZGFsQm9keSIgc3R5bGU9ImNvbG9yOnZhcigtLWluazIpO2ZvbnQtc2l6ZToxNHB4Ij5NZW5nZWNlay4uLjwvZGl2PgogIDwvZGl2Pgo8L2Rpdj4KCjxzY3JpcHQgZGF0YS1jZmFzeW5jPSJmYWxzZSIgc3JjPSIvY2RuLWNnaS9zY3JpcHRzLzVjNWRkNzI4L2Nsb3VkZmxhcmUtc3RhdGljL2VtYWlsLWRlY29kZS5taW4uanMiPjwvc2NyaXB0PjxzY3JpcHQ+CmNvbnN0IEFQSSA9ICdodHRwczovL2FwaWt5c2hpcm8udmVyY2VsLmFwcCc7CgovLyDilIDilIAgTUVOVSBET1Qg4pSA4pSACmZ1bmN0aW9uIHRvZ2dsZURvdChlKXsKICBlLnN0b3BQcm9wYWdhdGlvbigpOwogIGNvbnN0IG0gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZG90TWVudScpOwogIG0uY2xhc3NMaXN0LnRvZ2dsZSgnc2hvdycpOwp9CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoJ2NsaWNrJywgZnVuY3Rpb24oKXsKICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZG90TWVudScpLmNsYXNzTGlzdC5yZW1vdmUoJ3Nob3cnKTsKfSk7CgovLyDilIDilIAgTU9EQUwg4pSA4pSACmZ1bmN0aW9uIG9wZW5Nb2RhbChpZCl7CiAgY29uc3QgZWwgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZChpZCk7CiAgaWYoZWwpIGVsLmNsYXNzTGlzdC5hZGQoJ3Nob3cnKTsKICBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnZG90TWVudScpLmNsYXNzTGlzdC5yZW1vdmUoJ3Nob3cnKTsKfQpmdW5jdGlvbiBjbG9zZU1vZGFsKGlkKXsKICBjb25zdCBlbCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKGlkKTsKICBpZihlbCkgZWwuY2xhc3NMaXN0LnJlbW92ZSgnc2hvdycpOwp9CgovLyDilIDilIAgRU5EUE9JTlQgVE9HR0xFIOKUgOKUgApmdW5jdGlvbiB0b2dnbGVFcChoZWFkKXsKICBjb25zdCBib2R5ID0gaGVhZC5uZXh0RWxlbWVudFNpYmxpbmc7CiAgY29uc3QgYXJyICA9IGhlYWQucXVlcnlTZWxlY3RvcignLmVwLWFycm93Jyk7CiAgaWYoIWJvZHkgfHwgIWFycikgcmV0dXJuOwogIGJvZHkuY2xhc3NMaXN0LnRvZ2dsZSgnb3BlbicpOwogIGFyci5jbGFzc0xpc3QudG9nZ2xlKCdvcGVuJyk7Cn0KCi8vIOKUgOKUgCBDT1BZIENPREUg4pSA4pSACmZ1bmN0aW9uIGNwKGJ0bil7CiAgY29uc3QgYmxvY2sgPSBidG4uY2xvc2VzdCgnLmNvZGUnKTsKICBjb25zdCB0ZXh0ICA9IGJsb2NrLmlubmVyVGV4dC5yZXBsYWNlKC9eY29weVxuLywnJykucmVwbGFjZSgvXuKck1xuLywnJykudHJpbSgpOwogIG5hdmlnYXRvci5jbGlwYm9hcmQud3JpdGVUZXh0KHRleHQpLnRoZW4oZnVuY3Rpb24oKXsKICAgIGJ0bi50ZXh0Q29udGVudCA9ICfinJMnOwogICAgYnRuLnN0eWxlLmNvbG9yID0gJ3ZhcigtLWdyZWVuKSc7CiAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uKCl7IGJ0bi50ZXh0Q29udGVudCA9ICdjb3B5JzsgYnRuLnN0eWxlLmNvbG9yID0gJyc7IH0sIDIwMDApOwogIH0pLmNhdGNoKGZ1bmN0aW9uKCl7fSk7Cn0KCi8vIOKUgOKUgCBUT0RBWSBTVFJJTkcg4pSA4pSACmZ1bmN0aW9uIHRvZGF5U3RyKCl7CiAgY29uc3QgZCA9IG5ldyBEYXRlKCk7CiAgcmV0dXJuIFN0cmluZyhkLmdldERhdGUoKSkucGFkU3RhcnQoMiwnMCcpICsgJy8nICsgU3RyaW5nKGQuZ2V0TW9udGgoKSsxKS5wYWRTdGFydCgyLCcwJykgKyAnLycgKyBkLmdldEZ1bGxZZWFyKCk7Cn0KCi8vIOKUgOKUgCBTVEFUVVMgQ0hFQ0sg4pSA4pSACmFzeW5jIGZ1bmN0aW9uIGNoZWNrU3RhdHVzKG9wZW5Qb3B1cCl7CiAgY29uc3QgZG90ICAgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnc0RvdCcpOwogIGNvbnN0IHR4dCAgID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3NUZXh0Jyk7CiAgY29uc3QgbG9naW4gPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnc0xvZ2luJyk7CiAgY29uc3QgYm9keSAgPSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnc3RhdHVzTW9kYWxCb2R5Jyk7CgogIC8vIFNldCBjaGVja2luZyBzdGF0ZQogIGlmKGRvdCkgICB7IGRvdC5jbGFzc05hbWUgPSAnc2ItZG90IGNoZWNraW5nJzsgfQogIGlmKHR4dCkgICB0eHQudGV4dENvbnRlbnQgPSAnTWVuZ2VjZWsuLi4nOwogIGlmKGxvZ2luKSBsb2dpbi50ZXh0Q29udGVudCA9ICcuLi4nOwogIGlmKGJvZHkpICBib2R5LmlubmVySFRNTCA9ICc8c3BhbiBzdHlsZT0iY29sb3I6dmFyKC0taW5rMikiPk1lbmdodWJ1bmdpIHNlcnZlci4uLjwvc3Bhbj4nOwoKICBpZihvcGVuUG9wdXApIG9wZW5Nb2RhbCgnc3RhdHVzTW9kYWwnKTsKCiAgdHJ5IHsKICAgIGNvbnN0IGNvbnRyb2xsZXIgPSBuZXcgQWJvcnRDb250cm9sbGVyKCk7CiAgICBjb25zdCB0aW1lciA9IHNldFRpbWVvdXQoZnVuY3Rpb24oKXsgY29udHJvbGxlci5hYm9ydCgpOyB9LCAxNTAwMCk7CiAgICBjb25zdCByZXMgID0gYXdhaXQgZmV0Y2goQVBJICsgJy9oZWFsdGgnLCB7IHNpZ25hbDogY29udHJvbGxlci5zaWduYWwgfSk7CiAgICBjbGVhclRpbWVvdXQodGltZXIpOwogICAgY29uc3QgZGF0YSA9IGF3YWl0IHJlcy5qc29uKCk7CiAgICBjb25zdCBvayAgID0gZGF0YS5sb2dpbiA9PT0gJ3N1Y2Nlc3MnIHx8IGRhdGEuc3RhdHVzID09PSAnb2snOwoKICAgIGlmKG9rKXsKICAgICAgaWYoZG90KSAgIGRvdC5jbGFzc05hbWUgPSAnc2ItZG90IG9ubGluZSc7CiAgICAgIGlmKHR4dCkgICB0eHQudGV4dENvbnRlbnQgPSAnT25saW5lJzsKICAgICAgaWYobG9naW4pIGxvZ2luLnRleHRDb250ZW50ID0gJ+KchSBMb2dpbiBPSyc7CgogICAgICBjb25zdCBhY2NvdW50c09rICAgID0gZGF0YS5hY2NvdW50c19vayB8fCAxOwogICAgICBjb25zdCBhY2NvdW50c1RvdGFsID0gZGF0YS5hY2NvdW50c190b3RhbCB8fCAxOwogICAgICBjb25zdCBkZXRhaWxzICAgICAgID0gKGRhdGEuZGV0YWlscyB8fCBbXSkubWFwKGZ1bmN0aW9uKGQpewogICAgICAgIHJldHVybiAnPGRpdiBzdHlsZT0iZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6OHB4O3BhZGRpbmc6OHB4IDA7Ym9yZGVyLWJvdHRvbToxcHggc29saWQgdmFyKC0tbGluZSk7Zm9udC1zaXplOjEzcHgiPicKICAgICAgICAgICsgJzxzcGFuIHN0eWxlPSJ3aWR0aDo3cHg7aGVpZ2h0OjdweDtib3JkZXItcmFkaXVzOjUwJTtiYWNrZ3JvdW5kOicgKyAoZC5sb2dpbj09PSdzdWNjZXNzJz8ndmFyKC0tZ3JlZW4pJzondmFyKC0tcmVkKScpICsgJztkaXNwbGF5OmlubGluZS1ibG9jaztmbGV4LXNocmluazowIj48L3NwYW4+JwogICAgICAgICAgKyAnPHNwYW4gc3R5bGU9ImZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2NvbG9yOnZhcigtLWluazIpIj4nICsgZC5lbWFpbCArICc8L3NwYW4+JwogICAgICAgICAgKyAnPHNwYW4gc3R5bGU9Im1hcmdpbi1sZWZ0OmF1dG87Y29sb3I6JyArIChkLmxvZ2luPT09J3N1Y2Nlc3MnPyd2YXIoLS1ncmVlbiknOid2YXIoLS1yZWQpJykgKyAnO2ZvbnQtd2VpZ2h0OjcwMCI+JyArIChkLmxvZ2luPT09J3N1Y2Nlc3MnPydPSyc6J0dBR0FMJykgKyAnPC9zcGFuPicKICAgICAgICAgICsgJzwvZGl2Pic7CiAgICAgIH0pLmpvaW4oJycpOwoKICAgICAgaWYoYm9keSkgYm9keS5pbm5lckhUTUwgPQogICAgICAgICc8ZGl2IHN0eWxlPSJkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDoxMHB4O3BhZGRpbmc6MTRweDtiYWNrZ3JvdW5kOnJnYmEoMTg0LDI1NSwxMTAsLjA2KTtib3JkZXI6MXB4IHNvbGlkIHJnYmEoMTg0LDI1NSwxMTAsLjE1KTtib3JkZXItcmFkaXVzOjlweDttYXJnaW4tYm90dG9tOjE0cHgiPicKICAgICAgICArICc8c3BhbiBjbGFzcz0ic2ItZG90IG9ubGluZSIgc3R5bGU9ImZsZXgtc2hyaW5rOjAiPjwvc3Bhbj4nCiAgICAgICAgKyAnPGRpdj48ZGl2IHN0eWxlPSJmb250LXdlaWdodDo3MDA7Y29sb3I6dmFyKC0tZ3JlZW4pIj5BUEkgT25saW5lIOKchTwvZGl2PicKICAgICAgICArICc8ZGl2IHN0eWxlPSJmb250LXNpemU6MTJweDtjb2xvcjp2YXIoLS1pbmsyKTttYXJnaW4tdG9wOjJweCI+JyArIGFjY291bnRzT2sgKyAnLycgKyBhY2NvdW50c1RvdGFsICsgJyBha3VuIGFrdGlmIMK3IGlWQVMgdGVyaHVidW5nPC9kaXY+PC9kaXY+PC9kaXY+JwogICAgICAgICsgJzxkaXY+JyArIGRldGFpbHMgKyAnPC9kaXY+JwogICAgICAgICsgJzxkaXYgc3R5bGU9ImZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZToxMXB4O2NvbG9yOnZhcigtLWluazMpO21hcmdpbi10b3A6MTJweCI+RGljZWs6ICcgKyBuZXcgRGF0ZSgpLnRvTG9jYWxlVGltZVN0cmluZygnaWQtSUQnKSArICc8L2Rpdj4nOwoKICAgICAgLy8gVXBkYXRlIHN0YXRzIGZyb20gL3Rlc3QKICAgICAgdHJ5IHsKICAgICAgICBjb25zdCBjMiA9IG5ldyBBYm9ydENvbnRyb2xsZXIoKTsKICAgICAgICBjb25zdCB0MiA9IHNldFRpbWVvdXQoZnVuY3Rpb24oKXsgYzIuYWJvcnQoKTsgfSwgMjAwMDApOwogICAgICAgIGNvbnN0IHRkID0gYXdhaXQgZmV0Y2goQVBJICsgJy90ZXN0P2RhdGU9JyArIHRvZGF5U3RyKCksIHsgc2lnbmFsOiBjMi5zaWduYWwgfSk7CiAgICAgICAgY2xlYXJUaW1lb3V0KHQyKTsKICAgICAgICBjb25zdCBkZCA9IGF3YWl0IHRkLmpzb24oKTsKICAgICAgICBjb25zdCByICA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdzdFJhbmdlcycpOwogICAgICAgIGNvbnN0IG4gID0gZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3N0TnVtYmVycycpOwogICAgICAgIGlmKHIgJiYgZGQudG90YWxfcmFuZ2VzICAhPT0gdW5kZWZpbmVkKSByLnRleHRDb250ZW50ID0gZGQudG90YWxfcmFuZ2VzOwogICAgICAgIGlmKG4gJiYgZGQudG90YWxfbnVtYmVycyAhPT0gdW5kZWZpbmVkKSBuLnRleHRDb250ZW50ID0gZGQudG90YWxfbnVtYmVyczsKICAgICAgfSBjYXRjaChlKSB7fQoKICAgIH0gZWxzZSB7CiAgICAgIHRocm93IG5ldyBFcnJvcignbG9naW4gZ2FnYWwnKTsKICAgIH0KCiAgfSBjYXRjaChlKSB7CiAgICBpZihkb3QpICAgZG90LmNsYXNzTmFtZSA9ICdzYi1kb3Qgb2ZmbGluZSc7CiAgICBpZih0eHQpICAgdHh0LnRleHRDb250ZW50ID0gJ09mZmxpbmUnOwogICAgaWYobG9naW4pIGxvZ2luLnRleHRDb250ZW50ID0gJ+KdjCBHYWdhbCc7CgogICAgaWYoYm9keSkgYm9keS5pbm5lckhUTUwgPQogICAgICAnPGRpdiBzdHlsZT0iZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTBweDtwYWRkaW5nOjE0cHg7YmFja2dyb3VuZDpyZ2JhKDI1NSwxMDcsMTA3LC4wNik7Ym9yZGVyOjFweCBzb2xpZCByZ2JhKDI1NSwxMDcsMTA3LC4xNSk7Ym9yZGVyLXJhZGl1czo5cHgiPicKICAgICAgKyAnPHNwYW4gY2xhc3M9InNiLWRvdCBvZmZsaW5lIiBzdHlsZT0iZmxleC1zaHJpbms6MCI+PC9zcGFuPicKICAgICAgKyAnPGRpdj48ZGl2IHN0eWxlPSJmb250LXdlaWdodDo3MDA7Y29sb3I6dmFyKC0tcmVkKSI+QVBJIE9mZmxpbmUg4p2MPC9kaXY+JwogICAgICArICc8ZGl2IHN0eWxlPSJmb250LXNpemU6MTJweDtjb2xvcjp2YXIoLS1pbmsyKTttYXJnaW4tdG9wOjJweCI+R2FnYWwga29uZWsga2Ugc2VydmVyIGF0YXUgaVZBUyBsb2dvdXQ8L2Rpdj48L2Rpdj48L2Rpdj4nCiAgICAgICsgJzxkaXYgc3R5bGU9ImZvbnQtZmFtaWx5OnZhcigtLW1vbm8pO2ZvbnQtc2l6ZToxMXB4O2NvbG9yOnZhcigtLWluazMpO21hcmdpbi10b3A6MTJweCI+RGljZWs6ICcgKyBuZXcgRGF0ZSgpLnRvTG9jYWxlVGltZVN0cmluZygnaWQtSUQnKSArICc8L2Rpdj4nOwogIH0KfQoKLy8g4pSA4pSAIEFVVE8gU1RBVFVTIE9OIExPQUQg4pSA4pSACndpbmRvdy5hZGRFdmVudExpc3RlbmVyKCdsb2FkJywgZnVuY3Rpb24oKXsKICAvLyBDZWsgc3RhdHVzIG90b21hdGlzIHNhYXQgYnVrYQogIHNldFRpbWVvdXQoZnVuY3Rpb24oKXsgY2hlY2tTdGF0dXMoZmFsc2UpOyB9LCA4MDApOwogIC8vIEF1dG8gcmVmcmVzaCBzZXRpYXAgMzAgZGV0aWsKICBzZXRJbnRlcnZhbChmdW5jdGlvbigpeyBjaGVja1N0YXR1cyhmYWxzZSk7IH0sIDMwMDAwKTsKfSk7Cjwvc2NyaXB0PjxzY3JpcHQ+CmNvbnN0IEFQSSA9ICdodHRwczovL2FwaWt5c2hpcm8udmVyY2VsLmFwcCc7CgovLyDilIDilIAgTUVOVSBET1Qg4pSA4pSACmZ1bmN0aW9uIHRvZ2dsZURvdChlKXsKICBlLnN0b3BQcm9wYWdhdGlvbigpOwogIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdkb3RNZW51JykuY2xhc3NMaXN0LnRvZ2dsZSgnc2hvdycpOwp9CmRvY3VtZW50LmFkZEV2ZW50TGlzdGVuZXIoJ2NsaWNrJywoKT0+ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ2RvdE1lbnUnKS5jbGFzc0xpc3QucmVtb3ZlKCdzaG93JykpOwoKLy8g4pSA4pSAIE1PREFMIOKUgOKUgApmdW5jdGlvbiBvcGVuTW9kYWwoaWQpe2RvY3VtZW50LmdldEVsZW1lbnRCeUlkKGlkKS5jbGFzc0xpc3QuYWRkKCdzaG93Jyl9CmZ1bmN0aW9uIGNsb3NlTW9kYWwoaWQpe2RvY3VtZW50LmdldEVsZW1lbnRCeUlkKGlkKS5jbGFzc0xpc3QucmVtb3ZlKCdzaG93Jyl9CgovLyDilIDilIAgRU5EUE9JTlQgVE9HR0xFIOKUgOKUgApmdW5jdGlvbiB0b2dnbGVFcChoZWFkKXsKICBjb25zdCBib2R5PWhlYWQubmV4dEVsZW1lbnRTaWJsaW5nLCBhcnI9aGVhZC5xdWVyeVNlbGVjdG9yKCcuZXAtYXJyb3cnKTsKICBib2R5LmNsYXNzTGlzdC50b2dnbGUoJ29wZW4nKTsgYXJyLmNsYXNzTGlzdC50b2dnbGUoJ29wZW4nKTsKfQoKLy8g4pSA4pSAIENPUFkgQ09ERSDilIDilIAKZnVuY3Rpb24gY3AoYnRuKXsKICBjb25zdCBibG9jaz1idG4uY2xvc2VzdCgnLmNvZGUnKTsKICBjb25zdCB0ZXh0PWJsb2NrLmlubmVyVGV4dC5yZXBsYWNlKC9eY29weVxuLywnJykudHJpbSgpOwogIG5hdmlnYXRvci5jbGlwYm9hcmQud3JpdGVUZXh0KHRleHQpLnRoZW4oKCk9PnsKICAgIGJ0bi50ZXh0Q29udGVudD0n4pyTJzsgYnRuLnN0eWxlLmNvbG9yPSd2YXIoLS1ncmVlbiknOwogICAgc2V0VGltZW91dCgoKT0+e2J0bi50ZXh0Q29udGVudD0nY29weSc7YnRuLnN0eWxlLmNvbG9yPScnfSwyMDAwKTsKICB9KTsKfQoKLy8g4pSA4pSAIFNUQVRVUyBDSEVDSyDilIDilIAKYXN5bmMgZnVuY3Rpb24gY2hlY2tTdGF0dXMob3BlblBvcHVwPWZhbHNlKXsKICBjb25zdCBkb3Q9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3NEb3QnKTsKICBjb25zdCB0eHQ9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3NUZXh0Jyk7CiAgY29uc3QgbG9naW49ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3NMb2dpbicpOwogIGNvbnN0IGJvZHk9ZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3N0YXR1c01vZGFsQm9keScpOwoKICBkb3QuY2xhc3NOYW1lPSdzYi1kb3QgY2hlY2tpbmcnOyB0eHQudGV4dENvbnRlbnQ9J01lbmdlY2VrLi4uJzsgbG9naW4udGV4dENvbnRlbnQ9Jy4uLic7CiAgaWYoYm9keSkgYm9keS5pbm5lckhUTUw9JzxzcGFuIHN0eWxlPSJjb2xvcjp2YXIoLS1pbmsyKSI+TWVuZ2h1YnVuZ2kgc2VydmVyLi4uPC9zcGFuPic7CiAgaWYob3BlblBvcHVwKSBvcGVuTW9kYWwoJ3N0YXR1c01vZGFsJyk7CgogIHRyeSB7CiAgICBjb25zdCByZXM9YXdhaXQgZmV0Y2goQVBJKycvaGVhbHRoJyx7c2lnbmFsOkFib3J0U2lnbmFsLnRpbWVvdXQoMTQwMDApfSk7CiAgICBjb25zdCBkYXRhPWF3YWl0IHJlcy5qc29uKCk7CiAgICBjb25zdCBvaz1kYXRhLmxvZ2luPT09J3N1Y2Nlc3MnfHxkYXRhLnN0YXR1cz09PSdvayc7CgogICAgaWYob2spewogICAgICBkb3QuY2xhc3NOYW1lPSdzYi1kb3Qgb25saW5lJzsgdHh0LnRleHRDb250ZW50PSdPbmxpbmUnOyBsb2dpbi50ZXh0Q29udGVudD0n4pyFIExvZ2luIE9LJzsKICAgICAgY29uc3QgZGV0YWlscz0oZGF0YS5kZXRhaWxzfHxbXSkubWFwKGQ9PmAKICAgICAgICA8ZGl2IHN0eWxlPSJkaXNwbGF5OmZsZXg7YWxpZ24taXRlbXM6Y2VudGVyO2dhcDo4cHg7cGFkZGluZzo2cHggMDtib3JkZXItYm90dG9tOjFweCBzb2xpZCB2YXIoLS1saW5lKTtmb250LXNpemU6MTNweCI+CiAgICAgICAgICA8c3BhbiBzdHlsZT0id2lkdGg6N3B4O2hlaWdodDo3cHg7Ym9yZGVyLXJhZGl1czo1MCU7YmFja2dyb3VuZDoke2QubG9naW49PT0nc3VjY2Vzcyc/J3ZhcigtLWdyZWVuKSc6J3ZhcigtLXJlZCknfTtkaXNwbGF5OmlubGluZS1ibG9jaztmbGV4LXNocmluazowIj48L3NwYW4+CiAgICAgICAgICA8c3BhbiBzdHlsZT0iZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Y29sb3I6dmFyKC0taW5rMikiPiR7ZC5lbWFpbH08L3NwYW4+CiAgICAgICAgICA8c3BhbiBzdHlsZT0ibWFyZ2luLWxlZnQ6YXV0bztjb2xvcjoke2QubG9naW49PT0nc3VjY2Vzcyc/J3ZhcigtLWdyZWVuKSc6J3ZhcigtLXJlZCknfTtmb250LXdlaWdodDo2MDAiPiR7ZC5sb2dpbj09PSdzdWNjZXNzJz8nT0snOidHQUdBTCd9PC9zcGFuPgogICAgICAgIDwvZGl2PmApLmpvaW4oJycpOwogICAgICBpZihib2R5KSBib2R5LmlubmVySFRNTD1gCiAgICAgICAgPGRpdiBzdHlsZT0iZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTBweDtwYWRkaW5nOjE0cHg7YmFja2dyb3VuZDpyZ2JhKDE4NCwyNTUsMTEwLC4wNik7Ym9yZGVyOjFweCBzb2xpZCByZ2JhKDE4NCwyNTUsMTEwLC4xNSk7Ym9yZGVyLXJhZGl1czo5cHg7bWFyZ2luLWJvdHRvbToxNHB4Ij4KICAgICAgICAgIDxzcGFuIHN0eWxlPSJ3aWR0aDoxMHB4O2hlaWdodDoxMHB4O2JvcmRlci1yYWRpdXM6NTAlO2JhY2tncm91bmQ6dmFyKC0tZ3JlZW4pO2FuaW1hdGlvbjpibGluayAycyBpbmZpbml0ZTtmbGV4LXNocmluazowIj48L3NwYW4+CiAgICAgICAgICA8ZGl2PjxkaXYgc3R5bGU9ImZvbnQtd2VpZ2h0OjcwMDtjb2xvcjp2YXIoLS1ncmVlbikiPkFQSSBPbmxpbmU8L2Rpdj48ZGl2IHN0eWxlPSJmb250LXNpemU6MTJweDtjb2xvcjp2YXIoLS1pbmsyKTttYXJnaW4tdG9wOjJweCI+JHtkYXRhLmFjY291bnRzX29rfHwxfSBha3VuIGFrdGlmIMK3IGlWQVMgdGVyaHVidW5nPC9kaXY+PC9kaXY+CiAgICAgICAgPC9kaXY+CiAgICAgICAgPGRpdj4ke2RldGFpbHN9PC9kaXY+CiAgICAgICAgPGRpdiBzdHlsZT0iZm9udC1mYW1pbHk6dmFyKC0tbW9ubyk7Zm9udC1zaXplOjExcHg7Y29sb3I6dmFyKC0taW5rMyk7bWFyZ2luLXRvcDoxMHB4Ij5DaGVja2VkOiAke25ldyBEYXRlKCkudG9Mb2NhbGVUaW1lU3RyaW5nKCdpZC1JRCcpfTwvZGl2PmA7CgogICAgICAvLyB1cGRhdGUgc3RhdHMKICAgICAgdHJ5ewogICAgICAgIGNvbnN0IHRkPWF3YWl0IGZldGNoKEFQSSsnL3Rlc3Q/ZGF0ZT0nK3RvZGF5U3RyKCkse3NpZ25hbDpBYm9ydFNpZ25hbC50aW1lb3V0KDIwMDAwKX0pOwogICAgICAgIGNvbnN0IGRkPWF3YWl0IHRkLmpzb24oKTsKICAgICAgICBpZihkZC50b3RhbF9yYW5nZXMpIGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdzdFJhbmdlcycpLnRleHRDb250ZW50PWRkLnRvdGFsX3JhbmdlczsKICAgICAgICBpZihkZC50b3RhbF9udW1iZXJzKSBkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgnc3ROdW1iZXJzJykudGV4dENvbnRlbnQ9ZGQudG90YWxfbnVtYmVyczsKICAgICAgfWNhdGNoKGUpe30KCiAgICB9IGVsc2UgdGhyb3cgbmV3IEVycm9yKCdnYWdhbCcpOwoKICB9IGNhdGNoKGUpewogICAgZG90LmNsYXNzTmFtZT0nc2ItZG90IG9mZmxpbmUnOyB0eHQudGV4dENvbnRlbnQ9J09mZmxpbmUnOyBsb2dpbi50ZXh0Q29udGVudD0n4p2MIEdhZ2FsJzsKICAgIGlmKGJvZHkpIGJvZHkuaW5uZXJIVE1MPWAKICAgICAgPGRpdiBzdHlsZT0iZGlzcGxheTpmbGV4O2FsaWduLWl0ZW1zOmNlbnRlcjtnYXA6MTBweDtwYWRkaW5nOjE0cHg7YmFja2dyb3VuZDpyZ2JhKDI1NSwxMDcsMTA3LC4wNik7Ym9yZGVyOjFweCBzb2xpZCByZ2JhKDI1NSwxMDcsMTA3LC4xNSk7Ym9yZGVyLXJhZGl1czo5cHgiPgogICAgICAgIDxzcGFuIHN0eWxlPSJ3aWR0aDoxMHB4O2hlaWdodDoxMHB4O2JvcmRlci1yYWRpdXM6NTAlO2JhY2tncg==").decode("utf-8")
    return Response(html, mimetype="text/html")


@app.route("/tg/status")
def tg_status():
    """Cek konfigurasi Telegram notifier."""
    queue_size = 0
    with _tg_queue_lock:
        queue_size = len(_tg_queue)
    return jsonify({
        "enabled"       : _TG_ENABLED,
        "bot_token"     : (_TG_BOT_TOKEN[:12] + "...") if _TG_BOT_TOKEN else None,
        "chat_ids"      : _TG_CHAT_IDS,
        "sent_cache"    : len(_tg_sent_cache),
        "poll_cache"    : len(_poll_sms_cache),
        "poll_done"     : _poll_initial_done,
        "queue_pending" : queue_size,
        "poll_interval" : _POLL_INTERVAL,
    })


@app.route("/health")
def health():
    """Cek status login semua akun."""
    sessions = login_all_accounts()
    account_status = []

    for acc in ACCOUNTS:
        session = next((s for s in sessions if s["email"] == acc["email"]), None)
        account_status.append({
            "email":  acc["email"],
            "login":  "success" if session else "failed",
        })

    total_ok = sum(1 for a in account_status if a["login"] == "success")
    return jsonify({
        "status":       "ok" if total_ok > 0 else "error",
        "login":        "success" if total_ok > 0 else "failed",
        "accounts_ok":  total_ok,
        "accounts_total": len(ACCOUNTS),
        "details":      account_status,
    }), 200 if total_ok > 0 else 500


@app.route("/accounts")
def list_accounts():
    """List semua akun aktif: dari ACCOUNTS default + cookies.json (multi-account)."""
    all_accs = _get_all_accounts()
    with _session_lock:
        cache_copy = dict(_session_cache)
    result = []
    for i, acc in enumerate(all_accs):
        sess = cache_copy.get(acc["email"], {})
        result.append({
            "index":      i + 1,
            "email":      acc["email"],
            "active":     sess.get("ok", False),
            "via":        sess.get("via", "no_session"),
            "source":     "default" if any(a["email"] == acc["email"] for a in ACCOUNTS) else "cookies.json",
        })
    active = sum(1 for r in result if r["active"])
    return jsonify({
        "total":   len(result),
        "active":  active,
        "accounts": result,
        "hint": "Tambah akun baru: POST /set-cookies dengan email + cookies baru"
    })


@app.route("/sms")
def get_sms_endpoint():
    date_str     = request.args.get("date")
    mode         = request.args.get("mode", "received")
    filter_email = request.args.get("account", "").strip().lower()

    if mode not in ("live", "received", "both"):
        return jsonify({"error": "mode harus: live, received, atau both"}), 400

    today = datetime.now().strftime("%d/%m/%Y")
    from_date = today
    to_date   = today

    if mode != "live":
        if not date_str:
            return jsonify({"error": "Parameter date wajib (DD/MM/YYYY)"}), 400
        try:
            datetime.strptime(date_str, "%d/%m/%Y")
            from_date = date_str
            to_date   = request.args.get("to_date", date_str)
        except ValueError:
            return jsonify({"error": "Format date tidak valid, gunakan DD/MM/YYYY"}), 400

    # Coalescing — request /sms dengan parameter sama hanya hit iVAS 1x
    _sms_key = f"sms:{from_date}:{to_date}:{mode}:{filter_email}"
    cached_sms = _cache_get(_sms_key)
    if cached_sms is not None:
        return cached_sms

    def _do_sms():
        msgs, e = fetch_all_accounts(from_date, to_date, mode)
        if msgs is None:
            raise RuntimeError(e or "Fetch gagal")
        return msgs

    try:
        otp_messages = _coalesced(f"sms_fetch:{from_date}:{mode}", _do_sms)
        err = None
    except RuntimeError as e:
        otp_messages = None
        err = str(e)

    if otp_messages is None:
        return jsonify({"error": err}), 500

    # Filter per akun kalau ada param ?account=
    if filter_email:
        otp_messages = [m for m in otp_messages if m.get("account","").lower() == filter_email]

    # Normalize ke format unified (sama dengan /live/* endpoints)
    normalized = []
    for item in otp_messages:
        n = {
            "range":       item.get("range", ""),
            "number":      item.get("phone_number", item.get("number", "")),
            "sid":         item.get("sid", item.get("sender", "")),
            "message":     item.get("otp_message", item.get("message", "")),
            "received_at": item.get("received_at", ""),
            "account":     item.get("account", ""),
            "source":      item.get("source", "received"),
        }
        normalized.append(n)

    _sms_resp = jsonify({
        "status":        "success",
        "mode":          mode,
        "from_date":     from_date,
        "to_date":       to_date,
        "total":         len(normalized),
        "accounts_used": len(ACCOUNTS),
        "sms":           normalized,
    })
    _cache_set(_sms_key, _sms_resp)
    return _sms_resp


def _raw_post(acc, url, data):
    """Helper: POST request, return (resp, body_text)."""
    resp, _ = do_request(acc, "POST", url, data=data, headers=ajax_hdrs(RECV_URL))
    if resp is None:
        return None, "NULL RESPONSE"
    body = decode_response(resp)
    return resp, body


def _req_info(resp, body):
    """Satu blok info header untuk debug output."""
    if resp is None:
        return "  Status  : NULL\n  Body    : (no response)\n"
    return (
        f"  Status       : {resp.status_code}\n"
        f"  Final URL    : {getattr(resp, 'url', '?')}\n"
        f"  Content-Type : {resp.headers.get('Content-Type', '?')}\n"
        f"  Body Length  : {len(body)} chars\n"
    )


# ════════════════════════════════════════════════════════

def _fetch_datatables(account, base_url, search="", length=100,
                      col_data=None, col_name=None, fallback_fields=None):
    """
    Fetch DataTables JSON dari iVAS.
    col_data / col_name: list string nama kolom (harus sama panjang).
    Return (list_of_rows_as_dict, recordsTotal).
    """
    if col_data is None:
        col_data = ["range", "test_number"]
        col_name = ["terminations.range", "terminations.test_number"]
    if fallback_fields is None:
        fallback_fields = ["range","test_number","term","A2P","Limit_Range",
                           "limit_did_a2p","limit_cli_did_a2p","created_at","action"]
    col_qs = "".join(
        f"&columns[{i}][data]={d}&columns[{i}][name]={n}"
        for i, (d, n) in enumerate(zip(col_data, col_name))
    )
    qs = (
        f"draw=1{col_qs}"
        "&order[0][column]=0&order[0][dir]=asc"
        f"&start=0&length={length}"
        f"&search[value]={search}&search[regex]=false"
    )
    hdrs = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          base_url,
    }
    resp, _ = do_request(account, "GET", f"{base_url}?{qs}", headers=hdrs)
    if resp is None or resp.status_code != 200:
        return [], 0
    try:
        data  = resp.json()
        rows  = data.get("data", [])
        total = data.get("recordsTotal", len(rows))
        if rows and isinstance(rows[0], list):
            rows = [dict(zip(fallback_fields, r)) for r in rows]
        return rows, total
    except Exception:
        return [], 0


def _fetch_my_numbers(account, search="", length=100):
    """
    Fetch My Numbers dari /portal/numbers.
    Confirmed dari debug: field Number (kapital), range, A2P, LimitA2P,
    limit_did_a2p, limit_cli_a2p. number_id dari ReturnNumberToSystem(ID).
    """
    col_data = ["Number", "range", "A2P", "LimitA2P", "limit_did_a2p", "limit_cli_a2p", "number_id", "action"]
    col_name = ["Number", "range", "A2P",  "LimitA2P", "limit_did_a2p", "limit_cli_a2p", "number_id", "action"]
    fallback = ["Number", "range", "A2P",  "LimitA2P", "limit_did_a2p", "limit_cli_a2p", "number_id", "action"]
    rows, total = _fetch_datatables(
        account, f"{BASE_URL}/portal/numbers",
        search=search, length=length,
        col_data=col_data, col_name=col_name, fallback_fields=fallback,
    )
    return rows, total


def _get_number_id(row):
    """
    Ambil number_id dari row untuk delete/return.
    CONFIRMED dari JS iVAS: ID ada di checkbox value="ID" di field number_id.
    Priority: checkbox value > data-id > TerminationDetials > ReturnNumberToSystem
    """
    # Priority 1: parse value= dari semua field kandidat (CONFIRMED Image 5)
    # Format: <input name="select_id[]" class="..." value="4088582159">
    for fkey in ("number_id", "select_id", "id"):
        raw = str(row.get(fkey, "") or "")
        if not raw:
            continue
        m = re.search(r'value=["\']?(\d+)["\']?', raw)
        if m:
            return m.group(1)
        if raw.strip().isdigit():
            return raw.strip()
    number_id_field = str(row.get("number_id", "") or "")
    m = re.search(r'value=["\']?(\d+)["\']?', number_id_field)
    if m:
        return m.group(1)
    if number_id_field.strip().isdigit():
        return number_id_field.strip()

    action = str(row.get("action", "") or "")

    # Priority 2: data-id="ID"
    m = re.search(r'data-id=["\']?(\d+)["\']?', action)
    if m:
        return m.group(1)

    # Priority 3: TerminationDetials('ID')
    m = re.search(r"TerminationDetials\s*\(\s*['\"]?(\d+)['\"]?\s*\)", action)
    if m:
        return m.group(1)

    # Priority 4: ReturnNumberToSystem('ID') — fallback
    m = re.search(r"ReturnNumberToSystem\s*\(\s*['\"]?(\d+)['\"]?\s*\)", action)
    if m:
        return m.group(1)

    # Priority 5: field id / DT_RowId
    for key in ("id", "DT_RowId"):
        v = str(row.get(key, "")).strip()
        if v and v.isdigit():
            return v

    return ""


# ════════════════════════════════════════════════════════
# HELPER — parse iVAS response jadi success/message
# ════════════════════════════════════════════════════════

def _parse_ivas_resp(resp):
    """Return (success:bool, message:str, raw:str)"""
    if resp is None:
        return False, "No response", ""
    raw = decode_response(resp)
    try:
        jr      = resp.json()
        message = str(jr.get("message", jr.get("msg", jr.get("error", str(jr)))))
        st      = jr.get("status", jr.get("success", ""))
        # Cek status field dulu
        success = str(st).lower() in ("success","ok","true","1") or st is True or st == 1
        # Kalau status tidak ada/unknown, cek message — iVAS kadang hanya return message
        if not success:
            msg_low = message.lower()
            success = any(k in msg_low for k in (
                "berhasil", "success", "returned", "added", "deleted",
                "good job", "successfully", "done"
            ))
        return success, message, raw
    except Exception:
        raw_low = raw.lower()
        if any(k in raw_low for k in ("berhasil","success","added","returned","deleted","good job")):
            return True, "OK", raw
        return resp.status_code in (200, 201), f"HTTP {resp.status_code}", raw





# ════════════════════════════════════════════════════════
# /numbers/test-list — semua akun paralel
# ════════════════════════════════════════════════════════

@app.route("/numbers/test-list")
def numbers_test_list():
    """
    GET /numbers/test-list
    List Test Numbers dari /portal/numbers/test — SEMUA akun paralel.
    Params: search, limit (default 100), account (opsional, filter 1 akun)
    """
    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    search    = request.args.get("search", "")
    limit     = int(request.args.get("limit", 100))
    acc_email = request.args.get("account", "")
    targets   = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions

    all_numbers, errors = [], []
    lock = __import__("threading").Lock()

    def _fetch_one(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            return
        try:
            rows, total = _fetch_datatables(
                account, f"{BASE_URL}/portal/numbers/test",
                search=search, length=limit
            )
            result = []
            for row in rows:
                test_num = re.sub(r"<[^>]+>", "", str(row.get("test_number",""))).strip()
                if not test_num:
                    continue
                result.append({
                    "account":           email,
                    "number_id":         _get_number_id(row),
                    "range_name":        re.sub(r"<[^>]+>","",str(row.get("range",""))).strip(),
                    "test_number":       test_num,
                    "term":              str(row.get("term","")),
                    "rate_a2p":          str(row.get("A2P","")),
                    "limit_range":       str(row.get("Limit_Range","")),
                    "limit_did_a2p":     str(row.get("limit_did_a2p","")),
                    "limit_cli_did_a2p": str(row.get("limit_cli_did_a2p","")),
                    "created_at":        str(row.get("created_at","")),
                })
            with lock:
                all_numbers.extend(result)
            logger.info(f"[TEST-LIST] {email}: {len(rows)} nomor (total iVAS: {total})")
        except Exception as e:
            with lock:
                errors.append({"account": email, "error": str(e)})
            logger.error(f"[TEST-LIST] Error {email}: {e}")

    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        list(ex.map(_fetch_one, targets))

    return jsonify({
        "status":       "ok",
        "accounts_ok":  len(targets) - len(errors),
        "accounts_fail": len(errors),
        "total":        len(all_numbers),
        "numbers":      all_numbers,
        "errors":       errors,
    })


# ════════════════════════════════════════════════════════
# /numbers/my-list — semua akun paralel
# ════════════════════════════════════════════════════════

@app.route("/numbers/my-list")
def numbers_my_list():
    """
    GET /numbers/my-list
    List My Numbers dari /portal/numbers — SEMUA akun paralel.
    Params: search, limit (default 100), account (opsional)
    """
    search    = request.args.get("search", "")
    limit     = int(request.args.get("limit", 100))
    acc_email = request.args.get("account", "")

    # Cache key — kalau request sama dalam 30 detik, return dari cache
    _ck = f"mylist:{acc_email}:{search}:{limit}"
    cached = _cache_get(_ck)
    if cached is not None:
        return cached

    # Coalescing — kalau ada request identik sedang berjalan, tunggu hasilnya
    # Ini mencegah 10 bot hit bersamaan → 10x login ke iVAS
    def _do_fetch():
        sessions = login_all_accounts()
        if not sessions:
            raise RuntimeError("Login gagal semua akun")
        return sessions

    try:
        sessions = _coalesced(f"login:{acc_email or 'all'}", _do_fetch)
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 500
    targets   = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions

    all_numbers, errors = [], []
    lock = __import__("threading").Lock()

    def _fetch_one(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            return
        try:
            rows, total = _fetch_my_numbers(account, search=search, length=limit)
            result = []
            for row in rows:
                raw_num    = re.sub(r"<[^>]+>","",str(row.get("Number", row.get("number","")))).strip()
                range_name = re.sub(r"<[^>]+>","",str(row.get("range",""))).strip()
                if not raw_num:
                    continue
                result.append({
                    "account":       email,
                    "number_id":     _get_number_id(row),
                    "number":        raw_num,
                    "range_name":    range_name,
                    "rate_a2p":      str(row.get("A2P","")).strip(),
                    "limit_range":   str(row.get("LimitA2P", row.get("Limit_Range",""))).strip(),
                    "limit_did_a2p": str(row.get("limit_did_a2p","")).strip(),
                    "limit_cli_a2p": str(row.get("limit_cli_a2p","")).strip(),
                    "created_at":    str(row.get("created_at","")).strip(),
                })
            with lock:
                all_numbers.extend(result)
            logger.info(f"[MY-LIST] {email}: {len(rows)} nomor (total iVAS: {total})")
        except Exception as e:
            with lock:
                errors.append({"account": email, "error": str(e)})
            logger.error(f"[MY-LIST] Error {email}: {e}")

    with ThreadPoolExecutor(max_workers=len(targets)) as ex:
        list(ex.map(_fetch_one, targets))

    resp = jsonify({
        "status":        "ok",
        "accounts_ok":   len(targets) - len(errors),
        "accounts_fail": len(errors),
        "total":         len(all_numbers),
        "numbers":       all_numbers,
        "errors":        errors,
    })
    _cache_set(_ck, resp)
    return resp


# ════════════════════════════════════════════════════════
# /numbers/add — tambah nomor ke My Numbers
# ════════════════════════════════════════════════════════

@app.route("/numbers/add", methods=["GET","POST"])
def add_number():
    """
    Tambah nomor dari Test Numbers ke My Numbers — support semua akun paralel.

    CONFIRMED dari JS iVAS:
      POST /portal/numbers/termination/number/add  data: { id: termination_id }

    Mode penggunaan:
      1. range_name saja → fetch semua nomor di range itu, add ke semua akun
      2. termination_id  → add 1 nomor spesifik ke semua akun
      3. number          → auto-resolve ke termination_id, add ke semua akun

    Params:
      range_name     : nama range, misal "PAKISTAN 34" → add semua nomor di range (DIREKOMENDASIKAN)
      termination_id : ID spesifik dari Test Numbers
      number         : nomor telepon spesifik → auto-resolve
      account        : (opsional) filter 1 akun, default: semua akun paralel
      limit          : max nomor per akun kalau pakai range_name (default 500)
      dry_run        : "1" → preview saja, tidak eksekusi

    Contoh:
      /numbers/add?range_name=PAKISTAN 34
      /numbers/add?range_name=PAKISTAN 34&account=email@x.com
      /numbers/add?range_name=PAKISTAN 34&dry_run=1
      /numbers/add?termination_id=82774
      /numbers/add?number=923008264692
    """
    import time as _time

    if request.method == "GET":
        range_name     = request.args.get("range_name", "").strip()
        termination_id = request.args.get("termination_id", "").strip()
        number         = request.args.get("number", "").strip()
        acc_email      = request.args.get("account", "").strip()
        limit          = int(request.args.get("limit", 500))
        dry_run        = request.args.get("dry_run", "0").strip() == "1"
    else:
        d              = request.get_json(silent=True) or {}
        range_name     = (d.get("range_name","")     or request.form.get("range_name","")).strip()
        termination_id = (d.get("termination_id","") or request.form.get("termination_id","")).strip()
        number         = (d.get("number","")         or request.form.get("number","")).strip()
        acc_email      = (d.get("account","")        or request.form.get("account","")).strip()
        limit          = int(d.get("limit", request.form.get("limit", 500)))
        dry_run        = str(d.get("dry_run", request.form.get("dry_run","0"))).strip() == "1"

    if not range_name and not termination_id and not number:
        return jsonify({
            "error":    "Parameter range_name, termination_id, atau number wajib",
            "contoh_1": "/numbers/add?range_name=PAKISTAN 34",
            "contoh_2": "/numbers/add?termination_id=82774",
            "contoh_3": "/numbers/add?number=923008264692",
            "tip":      "Pakai range_name untuk add semua nomor dalam 1 range ke semua akun sekaligus",
        }), 400

    _cache_invalidate("mylist:")  # hapus cache setelah perubahan
    _ivas_cache_invalidate()          # invalidate iVAS cache (ranges/numbers/sms)
    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    targets = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions
    if not targets:
        return jsonify({"error": f"Akun '{acc_email}' tidak ditemukan atau login gagal"}), 404

    add_url  = f"{BASE_URL}/portal/numbers/termination/number/add"
    add_hdrs = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{BASE_URL}/portal/numbers/test",
        "Origin":           BASE_URL,
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    }

    all_results  = []
    all_errors   = []
    all_skipped  = []
    all_previews = []
    lock = threading.Lock()

    def _process_account(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            return

        # ── Kumpulkan items yang akan di-add ──────────────────────────────────
        items = []   # list of {"termination_id": str, "number": str, "range": str}

        if range_name:
            # Fetch Test Numbers dengan search range_name
            try:
                rows, total_ivas = _fetch_datatables(
                    account,
                    f"{BASE_URL}/portal/numbers/test",
                    search=range_name,
                    length=limit,
                )
            except Exception as e:
                with lock:
                    all_errors.append({"account": email, "error": f"Fetch Test Numbers gagal: {e}"})
                return

            # Ambil semua row yang range-nya mengandung range_name (contains, bukan exact)
            # → Fix: iVAS kadang format berbeda (spasi, kapital) jadi pakai 'in' bukan '=='
            rn_lower = range_name.lower().strip()
            for row in rows:
                rng_raw = str(row.get("range",""))
                rng     = re.sub(r"<[^>]+>","",rng_raw).strip()

                # Match: exact ATAU contains (toleran terhadap perbedaan format)
                if rng.lower().strip() == rn_lower:  # exact match only
                    # Resolve termination_id: coba semua field yang mungkin
                    tid = (
                        str(row.get("id","")).strip()
                        or str(row.get("DT_RowId","")).strip()
                        or _get_number_id(row)
                        or ""
                    )
                    # Bersihkan prefix DT_RowId kalau ada (misal "row_82774" → "82774")
                    if tid and not tid.isdigit():
                        m = re.search(r"(\d+)", tid)
                        tid = m.group(1) if m else ""

                    num = re.sub(r"<[^>]+>","",str(row.get("test_number",""))).strip()

                    if tid:
                        items.append({"termination_id": tid, "number": num, "range": rng})

            logger.info(f"[ADD] {email}: range='{range_name}' → iVAS_total={total_ivas} matched={len(items)}")

            if not items:
                # Debug: tampilkan sample ranges yang ditemukan
                sample_ranges = list(set(
                    re.sub(r"<[^>]+>","",str(r.get("range",""))).strip()
                    for r in rows[:10]
                ))
                with lock:
                    all_errors.append({
                        "account":       email,
                        "error":         f"Tidak ada nomor untuk range '{range_name}'",
                        "total_fetched": len(rows),
                        "total_iVAS":    total_ivas,
                        "sample_ranges_found": sample_ranges,
                        "tip":           "Cek sample_ranges_found untuk nama range yang benar",
                    })
                return

        elif termination_id:
            items = [{"termination_id": termination_id, "number": "", "range": ""}]

        elif number:
            # Resolve termination_id dari nomor telepon
            try:
                rows, _ = _fetch_datatables(
                    account, f"{BASE_URL}/portal/numbers/test",
                    search=number, length=200
                )
                for row in rows:
                    raw_num = re.sub(r"<[^>]+>","",str(row.get("test_number",""))).strip()
                    if re.sub(r"\D","",raw_num) == re.sub(r"\D","",number):
                        tid = (
                            str(row.get("id","")).strip()
                            or str(row.get("DT_RowId","")).strip()
                            or _get_number_id(row)
                            or ""
                        )
                        if tid and not tid.isdigit():
                            m = re.search(r"(\d+)", tid)
                            tid = m.group(1) if m else ""
                        if tid:
                            rng = re.sub(r"<[^>]+>","",str(row.get("range",""))).strip()
                            items.append({"termination_id": tid, "number": raw_num, "range": rng})
                            break
            except Exception as e:
                with lock:
                    all_errors.append({"account": email, "error": f"Resolve number gagal: {e}"})
                return

            if not items:
                with lock:
                    all_errors.append({
                        "account": email,
                        "error":   f"Nomor {number} tidak ditemukan di Test Numbers akun ini",
                    })
                return

        # ── Dry run ───────────────────────────────────────────────────────────
        if dry_run:
            with lock:
                all_previews.append({
                    "account":   email,
                    "found":     len(items),
                    "numbers":   items[:30],
                })
            return

        # ── Eksekusi add satu per satu ────────────────────────────────────────
        for item in items:
            tid = item["termination_id"]
            num = item["number"]
            try:
                resp, _ = do_request(account, "POST", add_url,
                                     data={"id": tid}, headers=add_hdrs)
                success, message, _ = _parse_ivas_resp(resp)
                entry = {
                    "account":        email,
                    "termination_id": tid,
                    "number":         num,
                    "range":          item.get("range",""),
                    "success":        success,
                    "message":        message,
                    "http_status":    resp.status_code if resp else None,
                }
                with lock:
                    if success:
                        all_results.append(entry)
                    elif "too many" in message.lower() or "maximum" in message.lower():
                        all_skipped.append(entry)
                        logger.warning(f"[ADD] {email}: stop di tid={tid}: {message}")
                        break
                    else:
                        all_errors.append(entry)
                logger.info(f"[ADD] {email}: tid={tid} {'✅' if success else '❌'} {message}")
                _time.sleep(0.25)
            except Exception as e:
                with lock:
                    all_errors.append({
                        "account": email, "termination_id": tid,
                        "number": num, "success": False, "error": str(e),
                    })
                logger.error(f"[ADD] {email}: error tid={tid}: {e}")

    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as ex:
        list(ex.map(_process_account, targets))

    if dry_run:
        return jsonify({
            "status":      "dry_run",
            "range_name":  range_name or number or termination_id,
            "accounts":    len(all_previews),
            "total_found": sum(p["found"] for p in all_previews),
            "previews":    all_previews,
            "tip":         "Hapus &dry_run=1 untuk eksekusi",
        })

    return jsonify({
        "status":          "ok" if all_results else "error",
        "range_name":      range_name or "",
        "number":          number or "",
        "termination_id":  termination_id or "(resolve per-akun)",
        "accounts_ok":     len(set(r["account"] for r in all_results)),
        "accounts_fail":   len(set(e["account"] for e in all_errors if "account" in e and "termination_id" not in e)),
        "added":           len(all_results),
        "failed":          len(all_errors),
        "skipped":         len(all_skipped),
        "results":         all_results,
        "errors":          all_errors,
        "skipped_details": all_skipped,
    }), 200 if all_results else 400


# ════════════════════════════════════════════════════════
# /numbers/add-by-range — alias ke /numbers/add?range_name=...
# ════════════════════════════════════════════════════════

@app.route("/numbers/add-by-range", methods=["GET","POST"])
def add_numbers_by_range():
    """
    Alias untuk /numbers/add — tambah semua nomor dalam 1 range ke semua akun paralel.

    Params:
      range_name : nama range — WAJIB
      account    : (opsional) filter 1 akun
      limit      : (opsional) max nomor per akun (default 500)
      dry_run    : (opsional) "1" → preview saja

    Contoh:
      /numbers/add-by-range?range_name=PAKISTAN 34
      /numbers/add-by-range?range_name=AFGHANISTAN 1000&account=email@x.com
      /numbers/add-by-range?range_name=PAKISTAN 34&dry_run=1
    """
    return add_number()


@app.route("/numbers/delete", methods=["GET","POST"])
def delete_number():
    """
    Return/hapus nomor ke sistem dari My Numbers.

    CONFIRMED dari discovery:
      POST /portal/numbers/termination/details
      data: { id: number_id }  ← number_id dari row My Numbers

    Params:
      number_id : ID dari row My Numbers (dari /numbers/my-list field number_id)
      number    : (opsional) nomor telepon — dipakai resolve number_id otomatis
      account   : (opsional) filter 1 akun, default: semua akun
    Contoh:
      /numbers/delete?number_id=3490323892
      /numbers/delete?number=51910550499
      /numbers/delete?number_id=3490323892&account=email@x.com
    """
    if request.method == "GET":
        number_id = request.args.get("number_id","").strip()
        number    = request.args.get("number","").strip()
        acc_email = request.args.get("account","").strip()
    else:
        d         = request.get_json(silent=True) or {}
        number_id = (d.get("number_id","") or request.form.get("number_id","")).strip()
        number    = (d.get("number","")    or request.form.get("number","")).strip()
        acc_email = (d.get("account","")   or request.form.get("account","")).strip()

    if not number_id and not number:
        return jsonify({
            "error":    "Parameter number_id atau number wajib",
            "contoh_1": "/numbers/delete?number_id=3490323892",
            "contoh_2": "/numbers/delete?number=51910550499",
            "tip":      "Cek /numbers/my-list untuk lihat number_id",
        }), 400

    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    targets = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions
    found_map = {}  # {email: {"number_id": str, "range_name": str}}
    lock = threading.Lock()

    # ── Resolve number_id dari nomor telepon ─────────────────────────────────
    if number_id:
        for s in targets:
            found_map[s["email"]] = {"number_id": number_id, "range_name": ""}
    else:
        def _search(session):
            email   = session["email"]
            account = _get_account(email)
            if not account:
                return
            # Cari di My Numbers
            rows, _ = _fetch_my_numbers(account, search=number, length=500)
            for row in rows:
                raw_num = re.sub(r"<[^>]+>","",str(row.get("Number",row.get("number","")))).strip()
                if re.sub(r"\D","",raw_num) == re.sub(r"\D","",number):
                    nid = _get_number_id(row)
                    if nid:
                        with lock:
                            found_map[email] = {
                                "number_id":  nid,
                                "range_name": re.sub(r"<[^>]+>","",str(row.get("range",""))).strip(),
                            }
                        return
        with ThreadPoolExecutor(max_workers=max(len(targets),1)) as ex:
            list(ex.map(_search, targets))

    if not found_map:
        return jsonify({
            "status": "error",
            "error":  f"number_id tidak ditemukan untuk number={number}. Cek /numbers/my-list",
        }), 404

    # ── POST delete ───────────────────────────────────────────────────────────
    # CONFIRMED: POST /portal/numbers/termination/details  data: { id: number_id }
    results, errors = [], []

    def _delete(session):
        email   = session["email"]
        account = _get_account(email)
        info    = found_map.get(email)
        if not account or not info:
            return
        nid = info["number_id"]
        try:
            # CONFIRMED dari JS iVAS: POST /portal/numbers/return/number {NumberID: id}
            resp, _ = do_request(
                account, "POST",
                f"{BASE_URL}/portal/numbers/return/number",
                data={"NumberID": nid},
                headers={
                    "Accept":           "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer":          f"{BASE_URL}/portal/numbers",
                    "Origin":           BASE_URL,
                },
            )
            success, message, raw = _parse_ivas_resp(resp)
            entry = {
                "account":    email,
                "success":    success,
                "number":     number or "",
                "number_id":  nid,
                "range_name": info.get("range_name",""),
                "message":    message,
            }
            with lock:
                (results if success else errors).append(entry)
            logger.info(f"[DELETE] {email}: nid={nid} success={success} msg={message}")
        except Exception as e:
            with lock:
                errors.append({"account":email,"success":False,"number_id":nid,"error":str(e)})

    delete_targets = [s for s in targets if s["email"] in found_map]
    with ThreadPoolExecutor(max_workers=max(len(delete_targets),1)) as ex:
        list(ex.map(_delete, delete_targets))

    return jsonify({
        "status":        "ok" if results else "error",
        "deleted_count": len(results),
        "failed_count":  len(errors),
        "number":        number or number_id,
        "results":       results,
        "errors":        errors,
    })



# ════════════════════════════════════════════════════════
# /numbers/delete-by-range — delete semua nomor dalam 1 range (BULK)
# ════════════════════════════════════════════════════════

@app.route("/numbers/delete-by-range", methods=["GET","POST"])
def delete_numbers_by_range():
    """
    Delete/return semua nomor dalam 1 range ke sistem menggunakan bulk endpoint.
    CONFIRMED dari JS iVAS:
      Bulk : POST /portal/numbers/return/number/bluck  {NumberID[]: [id, id, ...]}
      Single fallback: POST /portal/numbers/return/number  {NumberID: id}

    Params:
      range_name : nama range, misal "PAKISTAN 34"
      account    : (opsional) filter 1 akun, default: semua akun
      limit      : max nomor per akun (default 500)
    Contoh:
      /numbers/delete-by-range?range_name=PAKISTAN 34
      /numbers/delete-by-range?range_name=PAKISTAN 34&account=email@x.com
    """
    if request.method == "GET":
        range_name = request.args.get("range_name", "").strip()
        acc_email  = request.args.get("account", "").strip()
        limit      = int(request.args.get("limit", 500))
    else:
        d          = request.get_json(silent=True) or {}
        range_name = (d.get("range_name","") or request.form.get("range_name","")).strip()
        acc_email  = (d.get("account","")    or request.form.get("account","")).strip()
        limit      = int(d.get("limit", request.form.get("limit", 500)))

    if not range_name:
        return jsonify({
            "error":  "Parameter range_name wajib",
            "contoh": "/numbers/delete-by-range?range_name=PAKISTAN 34",
        }), 400

    _cache_invalidate("mylist:")  # hapus cache setelah perubahan
    _ivas_cache_invalidate()          # invalidate iVAS cache (ranges/numbers/sms)
    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    targets = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions
    results, errors = [], []
    lock = threading.Lock()

    def _process_account(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            return

        # Step 1: Fetch My Numbers
        try:
            rows, _ = _fetch_my_numbers(account, search=range_name, length=limit)
        except Exception as e:
            with lock:
                errors.append({"account": email, "error": f"Fetch failed: {e}"})
            return

        ids = []
        for row in rows:
            row_range = re.sub(r"<[^>]+>", "", str(row.get("range", ""))).strip()
            rn_low = range_name.lower().strip()
            rr_low = row_range.lower().strip()
            if rn_low != rr_low and rn_low not in rr_low and rr_low not in rn_low:
                continue
            nid = _get_number_id(row)
            if nid:
                ids.append(str(nid))

        if not ids:
            logger.info(f"[DEL-RANGE] {email}: 0 nomor di range \'{range_name}\'")
            return

        logger.info(f"[DEL-RANGE] {email}: {len(ids)} nomor → delete")

        # Ambil session
        sess_obj = get_session(account)
        if not sess_obj or not sess_obj.get("ok"):
            with lock:
                errors.append({"account": email, "error": "Session invalid"})
            return

        scraper = sess_obj["scraper"]

        # Ambil CSRF FRESH dari halaman /portal/numbers (bukan dari cache login)
        numbers_page = f"{BASE_URL}/portal/numbers"
        csrf = _get_csrf_cached(scraper, numbers_page)
        if not csrf:
            # Fallback ke session csrf kalau scrape gagal
            csrf = sess_obj.get("csrf", "")
        logger.info(f"[DEL-RANGE] {email}: CSRF fresh = {csrf[:20] if csrf else 'EMPTY'}...")

        hdrs_del = {
            "Accept":           "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN":     csrf,
            "Referer":          numbers_page,
            "Origin":           BASE_URL,
        }

        ok_count = 0
        bulk_ok  = False

        try:
            # Bulk: NumberID[]=id1&NumberID[]=id2&_token=csrf
            payload = [("NumberID[]", nid) for nid in ids] + [("_token", csrf)]
            resp    = scraper.post(
                f"{BASE_URL}/portal/numbers/return/number/bluck",
                data=payload, headers=hdrs_del, timeout=60,
            )
            s_bulk, msg_bulk, _ = _parse_ivas_resp(resp)

            if s_bulk:
                ok_count = len(ids)
                bulk_ok  = True
                logger.info(f"[DEL-RANGE] {email}: bulk OK — {len(ids)} dihapus ✅")
            else:
                raw_p = resp.text[:300] if resp else "no resp"
                logger.warning(f"[DEL-RANGE] {email}: bulk gagal ({msg_bulk}) raw={raw_p!r} → single")

        except Exception as e:
            logger.warning(f"[DEL-RANGE] {email}: bulk error ({e}) → single")

        # Fallback single — refresh CSRF tiap 10 nomor
        if not bulk_ok:
            ok_count = 0
            for i, nid in enumerate(ids):
                try:
                    # Refresh CSRF tiap 10 nomor supaya tidak expired
                    if i % 10 == 0:
                        fresh = _get_csrf_cached(scraper, numbers_page)
                        if fresh:
                            csrf = fresh
                            hdrs_del["X-CSRF-TOKEN"] = csrf
                    r = scraper.post(
                        f"{BASE_URL}/portal/numbers/return/number",
                        data={"NumberID": nid, "_token": csrf},
                        headers=hdrs_del, timeout=25,
                    )
                    s, msg_s, _ = _parse_ivas_resp(r)
                    if s:
                        ok_count += 1
                        logger.info(f"[DEL-RANGE] [{i+1}/{len(ids)}] {nid} ✅")
                    else:
                        logger.warning(f"[DEL-RANGE] [{i+1}/{len(ids)}] {nid} ❌ {msg_s}")
                except Exception as ex:
                    logger.warning(f"[DEL-RANGE] [{i+1}/{len(ids)}] {nid} err: {ex}")
            logger.info(f"[DEL-RANGE] {email}: single selesai {ok_count}/{len(ids)}")

        success = ok_count > 0
        message = f"{'bulk' if bulk_ok else 'single'}: {ok_count}/{len(ids)} dihapus"

        entry = {
            "account":       email,
            "success":       success,
            "range_name":    range_name,
            "ids_found":     len(ids),
            "deleted_count": ok_count,
            "message":       message,
        }
        with lock:
            (results if success else errors).append(entry)

    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as ex:
        list(ex.map(_process_account, targets))

    total_found   = sum(r.get("ids_found", 0) for r in results + errors)
    total_deleted = sum(r.get("deleted_count", 0) for r in results)
    return jsonify({
        "status":        "ok" if results else "error",
        "range_name":    range_name,
        "total_found":   total_found,
        "deleted_count": total_deleted,   # field yang dibaca bot
        "success_count": total_deleted,   # alias
        "failed_count":  total_found - total_deleted,
        "results":       results,
        "errors":        errors,
    })


# ════════════════════════════════════════════════════════
# /numbers/return-all — return SEMUA nomor di akun sekaligus
# ════════════════════════════════════════════════════════

@app.route("/numbers/return-all", methods=["GET","POST"])
def return_all_numbers():
    """
    Return SEMUA nomor ke sistem sekaligus (bulk).
    CONFIRMED dari JS: POST /portal/numbers/return/allnumber/bluck

    Params:
      account : (opsional) filter 1 akun, default: semua akun
    Contoh:
      /numbers/return-all
      /numbers/return-all?account=email@x.com
    """
    if request.method == "GET":
        acc_email = request.args.get("account", "").strip()
    else:
        d = request.get_json(silent=True) or {}
        acc_email = (d.get("account","") or request.form.get("account","")).strip()

    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    targets = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions
    results, errors = [], []
    lock = threading.Lock()

    def _return_all(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            return
        try:
            resp, _ = do_request(
                account, "POST",
                f"{BASE_URL}/portal/numbers/return/allnumber/bluck",
                data={},
                headers={
                    "Accept":           "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer":          f"{BASE_URL}/portal/numbers",
                    "Origin":           BASE_URL,
                },
            )
            success, message, _ = _parse_ivas_resp(resp)
            entry = {"account": email, "success": success, "message": message}
            with lock:
                (results if success else errors).append(entry)
            logger.info(f"[RETURN-ALL] {email}: success={success} msg={message}")
        except Exception as e:
            with lock:
                errors.append({"account": email, "success": False, "error": str(e)})

    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as ex:
        list(ex.map(_return_all, targets))

    return jsonify({
        "status":        "ok" if results else "error",
        "success_count": len(results),
        "failed_count":  len(errors),
        "results":       results,
        "errors":        errors,
    })


# ════════════════════════════════════════════════════════
# /numbers/export  — trigger export + poll progress + download
# /numbers/download — download file hasil export
# ════════════════════════════════════════════════════════
#
# CONFIRMED dari source JS iVAS:
#   Step 1: POST /portal/numbers/test/export
#   Step 2: GET  /portal/numbers/test-numbers/progress
#           → { progress, file_name, is_complete }
#   Step 3: GET  /portal/numbers/test-numbers/download/{file_name}
#
# ════════════════════════════════════════════════════════

def _get_fresh_csrf_from_test_page(account):
    """
    Ambil CSRF token segar langsung dari halaman /portal/numbers/test.
    CONFIRMED dari JS iVAS (script #28): X-CSRF-TOKEN diambil dari
    meta[name="csrf-token"] halaman test — bukan dari session login awal.
    Return (csrf_token_string, scraper_session) atau (None, None).
    """
    session = get_session(account)
    if not session or not session.get("ok"):
        return None, None
    scraper = session["scraper"]
    try:
        resp = scraper.get(
            f"{BASE_URL}/portal/numbers/test",
            headers={"Referer": BASE_URL, "Accept": "text/html,application/xhtml+xml,*/*;q=0.9"},
        )
        html  = decode_response(resp)
        soup  = BeautifulSoup(html, "html.parser")
        # Priority 1: <meta name="csrf-token" content="...">
        meta = soup.find("meta", {"name": "csrf-token"})
        if meta and meta.get("content"):
            csrf = meta["content"]
            logger.info(f"[CSRF] fresh dari meta csrf-token: {csrf[:20]}...")
            return csrf, scraper
        # Priority 2: input hidden _token
        inp = soup.find("input", {"name": "_token"})
        if inp and inp.get("value"):
            csrf = inp["value"]
            logger.info(f"[CSRF] fresh dari input _token: {csrf[:20]}...")
            return csrf, scraper
        # Priority 3: JS inline — X-CSRF-TOKEN: '....'
        m = re.search(r"['\"]X-CSRF-TOKEN['\"]\s*:\s*['\"]([A-Za-z0-9_\-+/=]{20,})['\"]", html)
        if m:
            csrf = m.group(1)
            logger.info(f"[CSRF] fresh dari JS inline: {csrf[:20]}...")
            return csrf, scraper
        logger.warning("[CSRF] tidak ditemukan di halaman test")
        return None, scraper
    except Exception as e:
        logger.error(f"[CSRF] Error ambil fresh csrf: {e}")
        return None, None


def _do_export_stream(account, scraper, csrf):
    """
    Eksekusi full export flow menggunakan scraper & csrf yang sudah fresh.
    Ini penting karena iVAS track progress per-session browser.

    CONFIRMED dari JS iVAS script #28:
      POST /portal/numbers/test/export
        headers: X-CSRF-TOKEN dari meta csrf-token halaman test
      → success: checkProgress() via setInterval
      GET /portal/numbers/test-numbers/progress
        → {progress, file_name, is_complete}
      → jika is_complete & file_name != null:
        downloadFile(file_name)

    CONFIRMED dari JS script #27 + HTML keyword [download]:
      Download URL ada dua kemungkinan:
        1. /portal/numbers/test-numbers/download/{file_name}  (dari progress response)
        2. /portal/numbers/test-numbers/download/1            (tombol HTML, angka ID)
      Kita coba keduanya.

    Return: (file_name_str_or_None, scraper)
    """
    import time as _time

    hdrs_post = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN":     csrf,
        "Referer":          f"{BASE_URL}/portal/numbers/test",
        "Origin":           BASE_URL,
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    }
    hdrs_get = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{BASE_URL}/portal/numbers/test",
    }

    # Step 1: POST trigger — pakai scraper yang sama (session sama = server tahu export milik siapa)
    try:
        r = scraper.post(
            f"{BASE_URL}/portal/numbers/test/export",
            data={"_token": csrf},
            headers=hdrs_post,
        )
        logger.info(f"[EXPORT_STREAM] trigger HTTP={r.status_code} body={r.text[:100]}")
        if r.status_code not in (200, 201, 202):
            logger.error(f"[EXPORT_STREAM] trigger gagal {r.status_code}")
            return None, scraper
    except Exception as e:
        logger.error(f"[EXPORT_STREAM] trigger error: {e}")
        return None, scraper

    # Step 2: Poll — scraper yang sama, bukan do_request (agar session konsisten)
    _time.sleep(2)
    file_name   = None
    deadline    = _time.time() + 90  # max 90 detik
    poll_no     = 0
    while _time.time() < deadline:
        _time.sleep(3)
        poll_no += 1
        try:
            pr = scraper.get(
                f"{BASE_URL}/portal/numbers/test-numbers/progress",
                headers=hdrs_get,
            )
            pj          = pr.json()
            file_name   = pj.get("file_name")
            is_complete = pj.get("is_complete", False)
            progress    = pj.get("progress", 0)
            logger.info(f"[EXPORT_STREAM] poll#{poll_no} progress={progress}% file={file_name} done={is_complete}")
            if is_complete and file_name:
                break
            if is_complete and not file_name:
                # Selesai tapi file_name null — coba pakai /download/1
                logger.warning("[EXPORT_STREAM] is_complete=True tapi file_name=null, coba /download/1")
                file_name = "1"
                break
        except Exception as e:
            logger.warning(f"[EXPORT_STREAM] poll#{poll_no} error: {e}")

    return file_name, scraper


def _download_export_file(scraper, file_name, account):
    """
    Download file hasil export.
    CONFIRMED dari HTML iVAS: ada dua URL kemungkinan, dicoba berurutan:
      1. /portal/numbers/test-numbers/download/{file_name}
      2. /portal/numbers/test-numbers/download/1   (tombol HTML pakai ID=1)

    Return: (response_object_or_None, url_yang_berhasil)
    """
    candidates = []

    # Kalau file_name bukan angka, coba nama file dulu, lalu ID 1
    if file_name and file_name != "1":
        candidates.append(f"{BASE_URL}/portal/numbers/test-numbers/download/{file_name}")
    # Selalu coba /download/1 (dari HTML iVAS: href="...download/1")
    candidates.append(f"{BASE_URL}/portal/numbers/test-numbers/download/1")

    hdrs = {
        "Accept":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
        "Referer": f"{BASE_URL}/portal/numbers/test",
    }

    for url in candidates:
        try:
            # Coba pakai scraper yang sama (session konsisten)
            r = scraper.get(url, headers=hdrs, allow_redirects=True) if scraper else None
            if r and r.status_code == 200 and len(r.content) > 100:
                logger.info(f"[DOWNLOAD_FILE] OK scraper: {url} {len(r.content)}b")
                return r, url
            # Fallback: do_request
            r2, _ = do_request(account, "GET", url, headers=hdrs)
            if r2 and r2.status_code == 200 and len(r2.content) > 100:
                logger.info(f"[DOWNLOAD_FILE] OK do_request: {url} {len(r2.content)}b")
                return r2, url
            logger.warning(f"[DOWNLOAD_FILE] gagal {url}: HTTP={r.status_code if r else 'None'}")
        except Exception as e:
            logger.warning(f"[DOWNLOAD_FILE] error {url}: {e}")

    return None, None


def _do_export_and_download(account, scraper, csrf, wait_secs=5):
    """
    CONFIRMED dari debug:
      - "Export already in progress" (HTTP 400) = file sudah ada, langsung download
      - Download /test-numbers/download/1 HTTP 200 + 17MB tanpa perlu trigger baru
      - Scraper yang sama bisa download file dari export MANAPUN (tidak harus trigger sendiri)

    Flow:
      1. Coba download dulu — kalau ada (>1000 bytes), langsung return
      2. Kalau tidak ada → POST trigger export (terima 200 atau 400 "already in progress")
      3. Retry download setiap 3 detik max 60 detik dengan scraper yang sama

    Return: (response_or_None, url_yang_dipakai)
    """
    import time as _time

    hdrs_dl = {
        "Accept":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
        "Referer": f"{BASE_URL}/portal/numbers/test",
    }
    hdrs_post = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN":     csrf,
        "Referer":          f"{BASE_URL}/portal/numbers/test",
        "Origin":           BASE_URL,
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    }

    def _try_download():
        """Coba download ID 1, 2, 3 — return (response, url) atau (None, None)"""
        for did in ["1", "2", "3"]:
            url = f"{BASE_URL}/portal/numbers/test-numbers/download/{did}"
            try:
                fr = scraper.get(url, headers=hdrs_dl, allow_redirects=True)
                ct = fr.headers.get("Content-Type", "")
                sz = len(fr.content)
                logger.info(f"[EXPORT_DL] try /download/{did} → HTTP={fr.status_code} CT={ct[:40]} size={sz}")
                if fr.status_code == 200 and sz > 1000 and ("spreadsheet" in ct or "excel" in ct or "openxml" in ct or "octet" in ct):
                    return fr, url
                # Redirect ke login = session expired
                if "/login" in fr.url:
                    logger.error(f"[EXPORT_DL] session expired → {fr.url}")
                    return None, None
            except Exception as e:
                logger.warning(f"[EXPORT_DL] /download/{did} error: {e}")
        return None, None

    # Step 1: Coba download dulu — mungkin file sudah ada dari export sebelumnya
    fr, url = _try_download()
    if fr is not None:
        logger.info(f"[EXPORT_DL] ✅ file sudah ada sebelum trigger: {url} {len(fr.content)}b")
        return fr, url

    # Step 2: Trigger export
    try:
        r = scraper.post(
            f"{BASE_URL}/portal/numbers/test/export",
            data={"_token": csrf},
            headers=hdrs_post,
        )
        body = r.text[:150]
        logger.info(f"[EXPORT_DL] trigger HTTP={r.status_code} body={body}")

        # HTTP 400 "Export already in progress" = normal, file akan ada
        # HTTP 200 success = export baru dimulai
        # HTTP lain = error sungguhan
        if r.status_code not in (200, 201, 202, 400):
            logger.error(f"[EXPORT_DL] trigger error {r.status_code}: {body}")
            return None, None

        # Kalau 400 tapi bukan "already in progress" = error lain
        if r.status_code == 400:
            try:
                msg = r.json().get("message", "")
                if "already in progress" not in msg.lower() and "progress" not in msg.lower():
                    logger.error(f"[EXPORT_DL] trigger 400 bukan 'already in progress': {msg}")
                    return None, None
                logger.info(f"[EXPORT_DL] Export already in progress — lanjut download")
            except Exception:
                pass

    except Exception as e:
        logger.error(f"[EXPORT_DL] trigger exception: {e}")
        return None, None

    # Step 3: Tunggu lalu retry download
    _time.sleep(wait_secs)
    deadline = _time.time() + 60
    attempt  = 0

    while _time.time() < deadline:
        attempt += 1
        fr, url = _try_download()
        if fr is not None:
            logger.info(f"[EXPORT_DL] ✅ attempt#{attempt} OK: {url} {len(fr.content)}b")
            return fr, url
        logger.info(f"[EXPORT_DL] attempt#{attempt} belum ada, retry 3s...")
        _time.sleep(3)

    logger.error(f"[EXPORT_DL] ❌ timeout 60s setelah {attempt} attempts")
    return None, None



    return None, None


# ════════════════════════════════════════════════════════
# /numbers/raw-debug — debug SEMUA akun
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /numbers/my-list-debug — debug kolom /portal/numbers
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /discover — iVAS Endpoint Discovery
# Crawl semua halaman iVAS, extract AJAX URL + payload dari JS/HTML
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /numbers/delete-bulk — return SELECTED numbers (by list of number_id)
# ════════════════════════════════════════════════════════
#
# CONFIRMED dari JS iVAS (BluckReturn):
#   POST /portal/numbers/return/number/bluck
#   data: { NumberID: id }  ← id = array dari checkbox value
#
# ════════════════════════════════════════════════════════

@app.route("/numbers/delete-bulk", methods=["GET","POST"])
def delete_bulk():
    """
    Return beberapa nomor sekaligus ke sistem (bulk by list of number_id).

    CONFIRMED dari JS iVAS (BluckReturn):
      POST /portal/numbers/return/number/bluck
      data: NumberID[] = [id1, id2, ...]

    Params:
      number_ids : comma-separated list of number_id, misal "3600511398,3600511424"
      account    : (opsional) email akun, default akun pertama

    Contoh:
      /numbers/delete-bulk?number_ids=3600511398,3600511424
      /numbers/delete-bulk?number_ids=3600511398&account=email@x.com
    """
    if request.method == "GET":
        number_ids_raw = request.args.get("number_ids", "").strip()
        acc_email      = request.args.get("account", "").strip()
    else:
        d              = request.get_json(silent=True) or {}
        number_ids_raw = (d.get("number_ids","") or request.form.get("number_ids","")).strip()
        acc_email      = (d.get("account","")    or request.form.get("account","")).strip()

    if not number_ids_raw:
        return jsonify({
            "error":  "Parameter number_ids wajib (comma-separated)",
            "contoh": "/numbers/delete-bulk?number_ids=3600511398,3600511424",
            "tip":    "Cek /numbers/my-list untuk lihat number_id",
        }), 400

    ids = [x.strip() for x in number_ids_raw.split(",") if x.strip()]
    if not ids:
        return jsonify({"error": "number_ids kosong atau format salah"}), 400

    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"error": "Login gagal semua akun"}), 500

    target  = next((s for s in sessions if s["email"] == acc_email), sessions[0])
    email   = target["email"]
    account = _get_account(email)

    hdrs = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"{BASE_URL}/portal/numbers",
        "Origin":           BASE_URL,
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    }

    try:
        # Build payload: NumberID[] = id1, NumberID[] = id2, ...
        sess_obj = get_session(account)
        if not sess_obj or not sess_obj.get("ok"):
            return jsonify({"error": "Session invalid"}), 500

        scraper = sess_obj["scraper"]
        csrf    = sess_obj["csrf"]
        payload = [("NumberID[]", nid) for nid in ids] + [("_token", csrf)]

        resp = scraper.post(
            f"{BASE_URL}/portal/numbers/return/number/bluck",
            data=payload, headers=hdrs, timeout=25,
        )
        success, message, raw = _parse_ivas_resp(resp)
        logger.info(f"[DELETE-BULK] {email}: ids={ids} success={success} msg={message}")

        return jsonify({
            "status":       "ok" if success else "error",
            "success":      success,
            "message":      message,
            "account":      email,
            "number_ids":   ids,
            "count":        len(ids),
            "http_status":  resp.status_code if resp else None,
        }), 200 if success else 400

    except Exception as e:
        logger.error(f"[DELETE-BULK] Error: {e}")
        return jsonify({"error": str(e)}), 500


# ════════════════════════════════════════════════════════
# /account/reload-code — reload account code (WhatsApp bot code)
# ════════════════════════════════════════════════════════
#
# CONFIRMED dari JS iVAS (ReloadAccountCode):
#   POST /portal/reloadAccountCode
#   dataType: json
#   Response: { code: "NEWCODE" }
#
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /debug/delete  — test semua variasi delete/return
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /debug/export  — test export + progress + download
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /debug/js-export — dump FULL JS iVAS terkait export/progress/download
# Tujuan: cari parameter tersembunyi di checkProgress(), updateProgressBar(),
#         downloadFile(), dan URL progress yang benar
# ════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════
# /numbers/all — semua nomor semua akun, tanpa potong, group by range
# ════════════════════════════════════════════════════════

@app.route("/numbers/all")
def numbers_all():
    """
    Tampilkan SEMUA nomor dari SEMUA akun — tidak dipotong, auto-detect range.

    Params:
      account   : (opsional) filter 1 akun spesifik
      range     : (opsional) filter 1 range name
      group     : "range" (default) | "account" | "flat" — cara grouping output
      format    : "json" (default) | "text" — format output
      limit     : max nomor per akun (default: 9999 = ambil semua)
      search    : filter pencarian nomor / range

    Contoh:
      /numbers/all                               ← semua akun, group by range
      /numbers/all?account=email@x.com           ← 1 akun
      /numbers/all?range=RangeA                  ← filter range
      /numbers/all?group=flat                    ← flat list semua nomor
      /numbers/all?group=account                 ← group by akun
      /numbers/all?format=text                   ← output plain text
    """
    acc_email    = request.args.get("account", "").strip()
    range_filter = request.args.get("range", "").strip().lower()
    group_by     = request.args.get("group", "range").strip().lower()
    fmt          = request.args.get("format", "json").strip().lower()
    limit        = int(request.args.get("limit", 9999))
    search       = request.args.get("search", "").strip()

    sessions = login_all_accounts()
    if not sessions:
        err = {"status": "error", "code": "LOGIN_FAILED", "message": "Login gagal semua akun",
               "hint": "Cek kredensial di IVAS_ACCOUNTS atau /bot/accounts"}
        return jsonify(err), 500

    targets = [s for s in sessions if s["email"] == acc_email] if acc_email else sessions
    if not targets:
        return jsonify({
            "status":  "error",
            "code":    "ACCOUNT_NOT_FOUND",
            "message": f"Akun '{acc_email}' tidak ditemukan atau login gagal",
            "hint":    "Cek /bot/accounts untuk daftar akun aktif",
        }), 404

    all_rows = []
    errors   = []
    lock     = threading.Lock()

    def _fetch_one(session):
        email   = session["email"]
        account = _get_account(email)
        if not account:
            with lock:
                errors.append({"account": email, "code": "ACCOUNT_NOT_IN_CONFIG",
                                "message": "Akun ada di session tapi tidak di ACCOUNTS list"})
            return
        try:
            rows, total = _fetch_my_numbers(account, search=search, length=limit)
            fetched = []
            for row in rows:
                raw_num    = re.sub(r"<[^>]+>", "", str(row.get("Number", row.get("number", "")))).strip()
                range_name = re.sub(r"<[^>]+>", "", str(row.get("range", ""))).strip()
                if not raw_num:
                    continue
                if range_filter and range_name.lower() != range_filter:
                    continue
                nid = _get_number_id(row)
                fetched.append({
                    "account":       email,
                    "number_id":     nid,
                    "number":        raw_num,
                    "range_name":    range_name,
                    "rate_a2p":      re.sub(r"<[^>]+>", "", str(row.get("A2P", ""))).strip(),
                    "limit_range":   re.sub(r"<[^>]+>", "", str(row.get("LimitA2P", row.get("Limit_Range", "")))).strip(),
                    "limit_did_a2p": re.sub(r"<[^>]+>", "", str(row.get("limit_did_a2p", ""))).strip(),
                    "limit_cli_a2p": re.sub(r"<[^>]+>", "", str(row.get("limit_cli_a2p", ""))).strip(),
                    "created_at":    str(row.get("created_at", "")).strip(),
                })
            with lock:
                all_rows.extend(fetched)
                if len(fetched) == 0 and total > 0 and not range_filter:
                    errors.append({"account": email, "code": "NO_ROWS_RETURNED",
                                   "message": f"iVAS return total={total} tapi rows kosong",
                                   "hint": "Coba tambah ?search= atau cek endpoint /numbers/my-list"})
            logger.info(f"[ALL] {email}: {len(fetched)} nomor dari {total} total")
        except Exception as e:
            with lock:
                errors.append({"account": email, "code": "FETCH_ERROR", "message": str(e)})
            logger.error(f"[ALL] Error {email}: {e}")

    with ThreadPoolExecutor(max_workers=max(len(targets), 1)) as ex:
        list(ex.map(_fetch_one, targets))

    # Auto-detect semua range name yang ada
    all_ranges = sorted(set(r["range_name"] for r in all_rows if r["range_name"]))

    # ── Grouping ──────────────────────────────────────────
    if group_by == "flat":
        grouped = all_rows

    elif group_by == "account":
        grouped = {}
        for row in all_rows:
            acc = row["account"]
            if acc not in grouped:
                grouped[acc] = {"account": acc, "total": 0, "ranges": [], "numbers": []}
            grouped[acc]["numbers"].append(row)
            grouped[acc]["total"] += 1
        # Auto-detect ranges per akun
        for acc_data in grouped.values():
            acc_data["ranges"] = sorted(set(r["range_name"] for r in acc_data["numbers"] if r["range_name"]))
        grouped = list(grouped.values())

    else:  # group_by == "range" (default)
        grouped = {}
        for row in all_rows:
            rng = row["range_name"] or "(no range)"
            if rng not in grouped:
                grouped[rng] = {"range_name": rng, "total": 0, "accounts": [], "numbers": []}
            grouped[rng]["numbers"].append(row)
            grouped[rng]["total"] += 1
        # Auto-detect akun per range
        for rng_data in grouped.values():
            rng_data["accounts"] = sorted(set(r["account"] for r in rng_data["numbers"]))
        grouped = sorted(grouped.values(), key=lambda x: x["range_name"])

    # ── Format text ───────────────────────────────────────
    if fmt == "text":
        lines = []
        lines.append(f"SEMUA NOMOR iVAS — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total: {len(all_rows)} nomor | Akun: {len(targets)} | Range: {len(all_ranges)}")
        if errors:
            lines.append(f"⚠️  {len(errors)} error:")
            for e in errors:
                lines.append(f"  [{e['code']}] {e['account']}: {e['message']}")
        lines.append("")

        if group_by == "flat":
            for r in all_rows:
                lines.append(f"{r['number']}\t{r['range_name']}\t{r['account']}\t{r['number_id']}")
        elif group_by == "account":
            for grp in grouped:
                lines.append(f"{'='*60}")
                lines.append(f"AKUN: {grp['account']} | {grp['total']} nomor | Range: {', '.join(grp['ranges'])}")
                lines.append(f"{'='*60}")
                for r in grp["numbers"]:
                    lines.append(f"  {r['number']}\t{r['range_name']}\t{r['number_id']}")
                lines.append("")
        else:
            for grp in grouped:
                lines.append(f"{'='*60}")
                lines.append(f"RANGE: {grp['range_name']} | {grp['total']} nomor | Akun: {', '.join(grp['accounts'])}")
                lines.append(f"{'='*60}")
                for r in grp["numbers"]:
                    lines.append(f"  {r['number']}\t{r['account']}\t{r['number_id']}")
                lines.append("")

        return Response("\n".join(lines), mimetype="text/plain; charset=utf-8")

    # ── Format JSON ───────────────────────────────────────
    return jsonify({
        "status":         "ok" if not errors else "partial",
        "total":          len(all_rows),
        "accounts_ok":    len(targets) - len(errors),
        "accounts_fail":  len(errors),
        "ranges_detected": all_ranges,
        "group_by":       group_by,
        "data":           grouped,
        "errors":         errors if errors else None,
        "hint": {
            "group_by_range":   "/numbers/all?group=range",
            "group_by_account": "/numbers/all?group=account",
            "flat_list":        "/numbers/all?group=flat",
            "filter_range":     "/numbers/all?range=NAMA_RANGE",
            "filter_account":   "/numbers/all?account=email@x.com",
            "text_output":      "/numbers/all?format=text",
        },
    })


# ════════════════════════════════════════════════════════
# BOT ACCOUNT MANAGEMENT
# /bot/login    — tambah / verifikasi akun baru
# /bot/accounts — lihat semua akun aktif
# /bot/remove   — hapus akun dari pool
# ════════════════════════════════════════════════════════




@app.route("/debug/raw")
def debug_raw_v2():
    """
    Dump raw response dari URL iVAS apapun via session akun.
    Auto-detect payload berdasarkan endpoint.

    Usage:
      /debug/raw?url=URL&method=POST&account=EMAIL
      &from=2026-03-13&to=2026-03-13&range=TOGO+650&number=22872460914
    """
    target_url = request.args.get("url",     "").strip()
    method     = request.args.get("method",  "GET").strip().upper()
    acc_email  = request.args.get("account", "").strip()
    from_date  = request.args.get("from",    datetime.now().strftime("%Y-%m-%d")).strip()
    to_date    = request.args.get("to",      datetime.now().strftime("%Y-%m-%d")).strip()
    range_name = request.args.get("range",   "").strip()
    phone_num  = request.args.get("number",  "").strip()

    if not target_url or not target_url.startswith("http"):
        return Response(
            "ERROR: url= wajib diisi URL lengkap (https://...)\n\n"
            "Contoh:\n"
            "  GET  : /debug/raw?url=https://www.ivasms.com/portal/sms/received&account=EMAIL\n"
            "  getsms (ranges) : /debug/raw?url=.../getsms&method=POST&account=EMAIL&from=2026-03-13&to=2026-03-13\n"
            "  getsms/number   : /debug/raw?url=.../getsms/number&method=POST&account=EMAIL&from=2026-03-13&to=2026-03-13&range=TOGO+650\n"
            "  getsms/number/sms: /debug/raw?url=.../getsms/number/sms&method=POST&account=EMAIL&from=2026-03-13&to=2026-03-13&range=TOGO+650&number=22872460914",
            mimetype="text/plain")

    target_acc = next((a for a in ACCOUNTS if a["email"] == acc_email), ACCOUNTS[0])
    SEP = "=" * 70
    out = []
    out.append("[DEBUG RAW]")
    out.append(f"Account : {target_acc['email']}")
    out.append(f"URL     : {target_url}")
    out.append(f"Method  : {method}")
    out.append(SEP)

    try:
        sess = get_session(target_acc)
        if not sess.get("ok"):
            out.append("ERROR: Login gagal")
            return Response("\n".join(out), mimetype="text/plain; charset=utf-8")

        sc_r     = sess["scraper"]
        csrf     = _get_csrf_cached(sc_r, RECV_URL) or sess.get("recv_csrf","") or sess.get("csrf","")
        start_dt = f"{from_date} 00:00:00"
        end_dt   = f"{to_date} 23:59:59"
        out.append(f"CSRF     : {csrf[:60] if csrf else 'MISSING'}")
        out.append(f"start_dt : {start_dt}  end_dt: {end_dt}")
        out.append(SEP)

        if method == "GET":
            r = sc_r.get(target_url, timeout=20)
        else:
            if "getsms/number/sms" in target_url:
                post_data = {"_token": csrf, "start": start_dt, "end": end_dt,
                             "Number": phone_num, "Range": range_name}
            elif "getsms/number" in target_url:
                post_data = {"_token": csrf, "start": start_dt, "end": end_dt,
                             "range": range_name}
            else:
                post_data = {"_token": csrf, "from": from_date, "to": to_date}
            out.append(f"POST data : {post_data}")
            out.append("-" * 50)
            r = sc_r.post(target_url, data=post_data,
                          headers={"Accept":"text/html,*/*;q=0.01",
                                   "X-Requested-With":"XMLHttpRequest",
                                   "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
                                   "Referer":RECV_URL,"Origin":BASE_URL}, timeout=20)

        h = decode_response(r)
        out.append(f"Status       : {r.status_code}")
        out.append(f"Final URL    : {r.url}")
        out.append(f"Content-Type : {r.headers.get('Content-Type','')}")
        out.append(f"Size         : {len(h)} chars")
        out.append("")
        out.append("[ FULL RAW BODY ]")
        out.append(h)

    except Exception as e:
        import traceback
        out.append(f"ERROR: {e}")
        out.append(traceback.format_exc())

    out.append("\nUsage: /debug/raw?url=URL&method=GET|POST&account=EMAIL&from=YYYY-MM-DD&to=YYYY-MM-DD&range=NAME&number=PHONE")
    return Response("\n".join(out), mimetype="text/plain; charset=utf-8")


@app.route("/debug/getsms-raw")
def debug_getsms_raw_v2():
    """
    Full chain: ranges → numbers → messages, raw dump per step.
    Pakai datetime CONFIRMED: start/end = YYYY-MM-DD HH:MM:SS

    Usage: /debug/getsms-raw?account=EMAIL&from=2026-03-13&to=2026-03-13
    """
    acc_email = request.args.get("account", "").strip()
    from_date = request.args.get("from",    datetime.now().strftime("%Y-%m-%d")).strip()
    to_date   = request.args.get("to",      datetime.now().strftime("%Y-%m-%d")).strip()

    target_acc = next((a for a in ACCOUNTS if a["email"] == acc_email), ACCOUNTS[0])
    SEP  = "=" * 70
    SEP2 = "-" * 50
    out  = []
    out.append("[DEBUG GETSMS-RAW] Full chain test")
    out.append(f"Account  : {target_acc['email']}")
    out.append(f"Date     : {from_date} -> {to_date}")
    out.append(SEP)

    try:
        sess = get_session(target_acc)
        if not sess.get("ok"):
            out.append("ERROR: Login gagal")
            return Response("\n".join(out), mimetype="text/plain; charset=utf-8")

        sc_g     = sess["scraper"]
        start_dt = f"{from_date} 00:00:00"
        end_dt   = f"{to_date} 23:59:59"
        out.append(f"start_dt : {start_dt}  end_dt: {end_dt}")
        out.append(SEP)

        def _csrf():
            return _get_csrf_cached(sc_g, RECV_URL) or sess.get("recv_csrf","")
        def _hdrs():
            return {"Accept":"text/html,*/*;q=0.01","X-Requested-With":"XMLHttpRequest",
                    "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
                    "Referer":RECV_URL,"Origin":BASE_URL}

        # CHAIN 1
        out.append("CHAIN 1: POST /portal/sms/received/getsms")
        out.append(SEP2)
        r1 = sc_g.post(f"{BASE_URL}/portal/sms/received/getsms",
                       data={"_token": _csrf(), "from": from_date, "to": to_date},
                       headers=_hdrs(), timeout=20)
        h1 = decode_response(r1)
        out.append(f"Status: {r1.status_code}  Size: {len(h1)} chars")
        out.append("[ FULL RAW ]")
        out.append(h1)
        ranges = []
        for m in re.finditer(r"toggleRange\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", h1):
            ranges.append({"name": m.group(1), "id": m.group(2)})
        for m in re.finditer(r'toggleRange\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\)', h1):
            if not any(rr["name"] == m.group(1) for rr in ranges):
                ranges.append({"name": m.group(1), "id": m.group(2)})
        out.append(f"[ {len(ranges)} ranges ] {[r['name'] for r in ranges]}")
        out.append(SEP)

        if not ranges:
            out.append("0 ranges.")
            return Response("\n".join(out), mimetype="text/plain; charset=utf-8")

        # CHAIN 2
        out.append("CHAIN 2: POST getsms/number (DATETIME CONFIRMED)")
        out.append(SEP2)
        all_tasks = []
        for rng in ranges:
            out.append(f"Range: {rng['name']}")
            r2 = sc_g.post(f"{BASE_URL}/portal/sms/received/getsms/number",
                           data={"_token": _csrf(), "start": start_dt, "end": end_dt,
                                 "range": rng["name"]},
                           headers=_hdrs(), timeout=20)
            h2 = decode_response(r2)
            out.append(f"  Status: {r2.status_code}  Size: {len(h2)} chars")
            out.append(h2[:800])
            nums = []
            for m in re.finditer(r"toggleNum\w*\s*\(\s*'(\d{7,15})'\s*,\s*'([^']+)'\s*\)", h2):
                nums.append({"number": m.group(1), "id": m.group(2)})
            out.append(f"  Numbers: {[n['number'] for n in nums]}")
            for n in nums:
                all_tasks.append({"number": n["number"], "range": rng["name"]})
            out.append(SEP2)
        out.append(SEP)

        if not all_tasks:
            out.append("0 nomor.")
            return Response("\n".join(out), mimetype="text/plain; charset=utf-8")

        # CHAIN 3
        out.append("CHAIN 3: POST getsms/number/sms (max 5)")
        out.append(SEP2)
        for task in all_tasks[:5]:
            out.append(f"Number: {task['number']}  Range: {task['range']}")
            r3 = sc_g.post(f"{BASE_URL}/portal/sms/received/getsms/number/sms",
                           data={"_token": _csrf(), "start": start_dt, "end": end_dt,
                                 "Number": task["number"], "Range": task["range"]},
                           headers=_hdrs(), timeout=20)
            h3 = decode_response(r3)
            out.append(f"Status: {r3.status_code}  Size: {len(h3)} chars")
            out.append("[ FULL RAW ]")
            out.append(h3)
            soup3 = BeautifulSoup(h3, "html.parser")
            msgs3 = [el.get_text(strip=True) for el in soup3.select("div.msg-text,td.msg-text,p.msg-text")]
            out.append(f"div.msg-text: {msgs3}")
            out.append(SEP2)

    except Exception as e:
        import traceback
        out.append(f"ERROR: {e}")
        out.append(traceback.format_exc())

    out.append("\nUsage: /debug/getsms-raw?account=EMAIL&from=YYYY-MM-DD&to=YYYY-MM-DD")
    return Response("\n".join(out), mimetype="text/plain; charset=utf-8")




# ══════════════════════════════════════════════════════════════════════════════
# /debug/master  — Debug semua fitur sekaligus, raw dump per section
# Usage: /debug/master?account=EMAIL&date=YYYY-MM-DD&sections=all
#        sections: all | received | live | addnumber | deletenumber | websocket
#        dry_run=1 → add/delete tidak dieksekusi (preview saja)
# ══════════════════════════════════════════════════════════════════════════════
# ════════════════════════════════════════════════════════
# COOKIE INJECT — Bypass Turnstile dengan cookies dari browser
# ════════════════════════════════════════════════════════
#
# CARA PAKAI (manual dari browser):
#   1. Buka ivasms.com/login di Chrome/Firefox
#   2. Login manual (selesaikan Turnstile di browser)
#   3. Buka DevTools → Application → Cookies → ivasms.com
#   4. Copy semua cookie (terutama: laravel_session, XSRF-TOKEN, remember_web_*)
#   5. POST ke /set-cookies dengan body JSON:
#      {
#        "email": "kicenofficial@gmail.com",
#        "cookies": {
#          "laravel_session": "eyJpdiI6...",
#          "XSRF-TOKEN": "eyJpdiI6...",
#          "remember_web_xxx": "..."
#        }
#      }
#   6. API akan inject ke scraper dan verifikasi ke iVAS
#
# CARA PAKAI (via cURL):
#   curl -X POST https://yourapp.vercel.app/set-cookies \
#     -H "Content-Type: application/json" \
#     -d '{"email":"kicenofficial@gmail.com","cookies":{"laravel_session":"VALUE","XSRF-TOKEN":"VALUE"}}'
#
# COOKIE EXPIRY: cookies iVAS biasanya valid 2 jam (session) atau 7 hari (remember_me)
# ════════════════════════════════════════════════════════

@app.route("/update-cookies")
def update_cookies_quick():
    """
    Update cookies via GET — mudah dari browser.
    
    Usage:
      /update-cookies?email=ceptampan58@gmail.com&xsrf=NILAI&ivas=NILAI
      
    Params:
      email : email akun (wajib)
      xsrf  : nilai XSRF-TOKEN (wajib)
      ivas  : nilai ivas_sms_session (wajib)
    """
    email = request.args.get("email", "").strip()
    xsrf  = request.args.get("xsrf",  "").strip()
    ivas  = request.args.get("ivas",  "").strip()

    if not email or not xsrf or not ivas:
        return jsonify({
            "status": "error",
            "message": "Parameter email, xsrf, dan ivas wajib diisi",
            "contoh": "/update-cookies?email=ceptampan58@gmail.com&xsrf=NILAI_XSRF&ivas=NILAI_IVAS"
        }), 400

    # Inject ke scraper — set untuk kedua domain (BASE_URL & www.ivasms.com)
    scraper = build_scraper()
    scraper.cookies.set("XSRF-TOKEN",       xsrf, domain=_BASE_DOMAIN,    path="/")
    scraper.cookies.set("ivas_sms_session", ivas, domain=_BASE_DOMAIN,    path="/")
    scraper.cookies.set("XSRF-TOKEN",       xsrf, domain="www.ivasms.com", path="/")
    scraper.cookies.set("ivas_sms_session", ivas, domain="www.ivasms.com", path="/")

    # Verifikasi ke iVAS
    try:
        r = scraper.get(f"{BASE_URL}/portal/sms/received", timeout=25, allow_redirects=True)
        expired = "/login" in r.url
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    if expired:
        return jsonify({
            "status":  "error",
            "code":    "COOKIES_EXPIRED",
            "message": "Cookies tidak valid / sudah expired",
            "hint":    "Login ulang di browser dan copy cookies yang baru"
        }), 401

    # Simpan ke session cache
    session_entry = {
        "ok": True, "scraper": scraper,
        "csrf": xsrf, "recv_csrf": xsrf,
        "live_html": "", "email": email,
        "via": "update_cookies_quick",
        "injected_at": datetime.now().isoformat(),
    }
    with _session_lock:
        _session_cache[email] = session_entry

    # Simpan ke cookies.json
    _save_cookies_json(email, {"XSRF-TOKEN": xsrf, "ivas_sms_session": ivas})

    # Update _PRESET_COOKIES di memory
    for entry in _PRESET_COOKIES:
        if entry.get("email") == email:
            entry["cookies"]["XSRF-TOKEN"]       = xsrf
            entry["cookies"]["ivas_sms_session"] = ivas

    return jsonify({
        "status":  "ok",
        "email":   email,
        "message": "Cookies berhasil diupdate dan diverifikasi!",
        "saved_to": "cookies.json + session cache",
        "hint": {
            "test": f"/sms?mode=received&date={datetime.now().strftime('%d/%m/%Y')}",
            "status": "/health"
        }
    })


@app.route("/set-cookies", methods=["GET", "POST"])
def set_cookies():
    """
    Inject cookies browser ke session scraper — bypass Turnstile tanpa solver.

    POST JSON:
      { "email": "...", "cookies": { "laravel_session": "...", "XSRF-TOKEN": "...", ... } }

    GET (untuk test / lihat panduan):
      /set-cookies → tampil panduan cara pakai

    Optional params:
      verify=0  → skip verifikasi ke iVAS (langsung simpan cookies)
      verify=1  → (default) verifikasi cookies ke iVAS sebelum simpan
    """
    # GET → tampil panduan
    if request.method == "GET" and not request.args.get("email"):
        guide = {
            "endpoint": "/set-cookies",
            "method": "POST",
            "content_type": "application/json",
            "body": {
                "email": "email_akun@gmail.com",
                "cookies": {
                    "XSRF-TOKEN":        "PASTE_DARI_BROWSER (wajib)",
                    "ivas_sms_session":  "PASTE_DARI_BROWSER (wajib)",
                    "cf_clearance":      "PASTE_DARI_BROWSER (WAJIB untuk bypass Cloudflare)",
                    "remember_web_XXXX": "PASTE_JIKA_ADA (opsional)"
                },
                "verify": 1
            },
            "cara_ambil_cookies": [
                "1. Buka ivasms.com/login di browser Chrome/Firefox",
                "2. Login manual — selesaikan Cloudflare Turnstile challenge",
                "3. Setelah masuk portal, buka DevTools (F12)",
                "4. Pergi ke: Application → Storage → Cookies → https://www.ivasms.com",
                "5. Copy nilai cookie: XSRF-TOKEN, ivas_sms_session, cf_clearance (WAJIB!)",
                "6. POST ke endpoint ini dengan body JSON di atas",
                "PENTING: cf_clearance terikat ke User-Agent browser kamu — wajib sama!"
            ],
            "cookies_penting": {
                "cf_clearance":     "Cookie Cloudflare — wajib untuk bypass tantangan CF (diambil dari ivasms.com cookies)",
                "XSRF-TOKEN":       "Laravel CSRF token",
                "ivas_sms_session": "Laravel session cookie"
            },
            "status_endpoint": "/cookies-status",
            "debug_endpoint":  "/debug/raw?path=/portal/sms/received",
            "contoh_curl": (
                'curl -X POST /set-cookies '
                '-H "Content-Type: application/json" '
                '-d \'{"email":"akun@gmail.com","cookies":{"XSRF-TOKEN":"VALUE","ivas_sms_session":"VALUE","cf_clearance":"VALUE"}}\''
            )
        }
        return jsonify(guide), 200

    # Parse body
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
    else:
        data = dict(request.args)

    email   = (data.get("email") or "").strip()
    cookies = data.get("cookies") or {}
    verify  = str(data.get("verify", "1")).strip() != "0"

    if not email:
        return jsonify({"status": "error", "code": "MISSING_EMAIL",
                        "message": "Field 'email' wajib diisi"}), 400
    if not cookies or not isinstance(cookies, dict):
        return jsonify({"status": "error", "code": "MISSING_COOKIES",
                        "message": "Field 'cookies' wajib diisi (dict nama:nilai)"}), 400

    # Cari akun di pool
    account = _get_account(email)
    if not account:
        # Akun tidak ada di pool — buat entry dummy supaya bisa dipakai
        account = {"email": email, "password": ""}
        logger.info(f"[COOKIES] Akun {email} tidak ada di pool, buat entry dummy")

    # Build scraper baru dan inject cookies — set untuk kedua domain
    scraper = build_scraper()
    for name, value in cookies.items():
        scraper.cookies.set(name, str(value), domain=_BASE_DOMAIN,    path="/")
        scraper.cookies.set(name, str(value), domain="www.ivasms.com", path="/")

    logger.info(f"[COOKIES] Inject {len(cookies)} cookies untuk {email}")

    # Verifikasi ke iVAS
    csrf     = None
    verified = False
    err_msg  = None

    if verify:
        try:
            # Coba akses halaman portal
            r = scraper.get(LIVE_URL, timeout=20, allow_redirects=True)
            if "/login" in r.url:
                err_msg = "Cookies tidak valid atau sudah expired — redirect ke /login"
                logger.warning(f"[COOKIES] {email}: {err_msg}")
            else:
                verified = True
                # Ambil CSRF dari halaman
                html  = decode_response(r)
                soup  = BeautifulSoup(html, "html.parser")
                meta  = soup.find("meta", {"name": "csrf-token"})
                inp   = soup.find("input", {"name": "_token"})
                csrf  = (meta["content"] if meta else (inp["value"] if inp else None))
                logger.info(f"[COOKIES] {email} verified OK, csrf={'FOUND' if csrf else 'MISSING'}")
        except Exception as e:
            err_msg = str(e)
            logger.error(f"[COOKIES] Verify error {email}: {e}")
    else:
        verified = None  # skip verify

    if verify and not verified:
        return jsonify({
            "status":  "error",
            "code":    "COOKIES_INVALID",
            "email":   email,
            "message": err_msg or "Cookies tidak valid",
            "hint":    "Pastikan copy cookies SETELAH login berhasil di browser, terutama laravel_session"
        }), 401

    # Simpan ke session cache
    session_entry = {
        "ok":        True,
        "scraper":   scraper,
        "csrf":      csrf or "",
        "recv_csrf": csrf or "",
        "live_html": "",
        "email":     email,
        "via":       "cookie_inject",
        "verified":  verified,
        "cookies_injected": list(cookies.keys()),
        "injected_at": datetime.now().isoformat(),
    }
    with _session_lock:
        _session_cache[email] = session_entry

    # Simpan cookies ke cookies.json supaya akun persist & ikut di multi-account loop
    _save_cookies_to_file(
        email,
        cookies.get("XSRF-TOKEN", ""),
        cookies.get("ivas_sms_session", ""),
        extra={k: v for k, v in cookies.items() if k not in ("XSRF-TOKEN", "ivas_sms_session")}
    )

    return jsonify({
        "status":           "ok",
        "email":            email,
        "verified":         verified,
        "cookies_injected": list(cookies.keys()),
        "csrf_found":       bool(csrf),
        "message":          f"Cookies berhasil diinject {'dan diverifikasi' if verified else '(tanpa verifikasi)'}",
        "hint": {
            "lihat_status": "/cookies-status",
            "test_sms":     f"/sms?mode=received&date={datetime.now().strftime('%d/%m/%Y')}&account={email}",
            "refresh":      "Kirim ulang ke /set-cookies kalau cookies expired"
        }
    })


@app.route("/cookies-status", methods=["GET"])
def cookies_status():
    """
    Lihat status cookies semua akun — mana yang aktif via cookie inject vs login biasa.

    Response:
      { "accounts": [ { "email": ..., "via": ..., "ok": ..., "injected_at": ... }, ... ] }
    """
    result = []
    with _session_lock:
        cache_copy = dict(_session_cache)

    for email, sess in cache_copy.items():
        entry = {
            "email":    email,
            "ok":       sess.get("ok", False),
            "via":      sess.get("via", "normal_login"),
            "verified": sess.get("verified"),
        }
        if sess.get("injected_at"):
            entry["injected_at"]     = sess["injected_at"]
            entry["cookies_injected"] = sess.get("cookies_injected", [])
        result.append(entry)

    # Akun yang belum punya session sama sekali
    all_accounts = _get_all_accounts()
    cached_emails = {e["email"] for e in result}
    for acc in all_accounts:
        if acc["email"] not in cached_emails:
            result.append({
                "email":  acc["email"],
                "ok":     False,
                "via":    "no_session",
                "hint":   "Belum login — POST cookies ke /set-cookies"
            })

    ok_count = sum(1 for e in result if e.get("ok"))
    return jsonify({
        "total":          len(result),
        "active":         ok_count,
        "inactive":       len(result) - ok_count,
        "accounts":       result,
        "set_cookies_url": "/set-cookies",
        "panduan":        "GET /set-cookies untuk lihat cara inject cookies dari browser"
    })


# ════════════════════════════════════════════════════════
# FAST POLL — No delay, no sleep, langsung ambil SMS terbaru
# Cocok untuk polling dari bot / panel setiap beberapa detik
# ════════════════════════════════════════════════════════

# Cache SMS terakhir per akun untuk fast diff
_fast_cache: dict = {}  # email → set of (number, msg[:80])
_fast_lock = threading.Lock()

@app.route("/fast/sms", methods=["GET"])
def fast_sms():
    """
    Fast SMS endpoint — ambil SMS hari ini dari semua akun aktif.
    No retry delay, paralel fetch, return immediately.

    Params:
      date    : DD/MM/YYYY (default: hari ini)
      new_only: 1 → hanya SMS baru sejak request terakhir (diff cache)
      account : filter per akun (opsional)

    Response cepat karena:
    - Pakai session cache (no re-login)
    - No sleep/retry delay
    - Semua akun paralel
    """
    date_str     = request.args.get("date", datetime.now().strftime("%d/%m/%Y"))
    new_only     = request.args.get("new_only", "0") == "1"
    filter_email = request.args.get("account", "").strip().lower()

    sessions = login_all_accounts()
    if not sessions:
        return jsonify({"status": "error", "error": "Tidak ada akun aktif", "total": 0}), 503

    if filter_email:
        sessions = [s for s in sessions if s["email"].lower() == filter_email]
        if not sessions:
            return jsonify({"status": "error", "error": f"Akun {filter_email} tidak aktif", "total": 0}), 404

    all_sms = []
    seen    = set()

    def _fetch_fast(session):
        email   = session["email"]
        account = _get_account(email) or {"email": email, "password": ""}
        out = []
        try:
            ranges = get_ranges(account, date_str, date_str)
            if not ranges:
                return []
            tasks = []
            for rng in ranges:
                nums = get_numbers(account, rng["name"], date_str, date_str, range_id=rng["id"])
                for n in nums:
                    num_val = n["number"] if isinstance(n, dict) else str(n)
                    tasks.append((num_val, rng["name"]))
            if not tasks:
                return []
            with ThreadPoolExecutor(max_workers=min(len(tasks), 10)) as ex2:
                futs = {ex2.submit(get_sms, account, t[0], t[1], date_str, date_str): t for t in tasks}
                done2, _ = _cf.wait(futs, timeout=6.0)
                for fut in done2:
                    num_val, rng_name = futs[fut]
                    try:
                        msgs = fut.result() or []
                        for m in msgs:
                            if isinstance(m, dict):
                                msg_text = _ivas_clean_msg(str(m.get("message", m.get("otp_message", ""))))
                                sid_val  = str(m.get("sid", m.get("sender", "")))
                                rcv_val  = str(m.get("received_at", m.get("senttime", "")))
                            else:
                                msg_text = _ivas_clean_msg(str(m))
                                sid_val  = rcv_val = ""
                            out.append({
                                "range":       rng_name,
                                "number":      num_val,
                                "message":     msg_text,
                                "sid":         sid_val,
                                "received_at": rcv_val,
                                "account":     email,
                                "source":      "received",
                            })
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"[FAST] {email}: {e}")
        return out

    # Paralel semua akun, timeout 10s
    with ThreadPoolExecutor(max_workers=max(len(sessions), 1)) as ex:
        futs = [ex.submit(_fetch_fast, s) for s in sessions]
        done_outer, _ = _cf.wait(futs, timeout=10.0)
        for f in done_outer:
            try:
                for item in f.result():
                    key = f"{item['number']}|{item['message'][:80]}"
                    if key not in seen:
                        seen.add(key)
                        all_sms.append(item)
            except Exception:
                pass

    # new_only: filter SMS yang belum pernah di-return sebelumnya
    if new_only:
        new_sms = []
        with _fast_lock:
            for item in all_sms:
                email = item["account"]
                cache = _fast_cache.setdefault(email, set())
                key   = f"{item['number']}|{item['message'][:80]}"
                if key not in cache:
                    cache.add(key)
                    new_sms.append(item)
        all_sms = new_sms

    return jsonify({
        "status":       "ok",
        "date":         date_str,
        "total":        len(all_sms),
        "accounts_used": len(sessions),
        "new_only":     new_only,
        "sms":          all_sms,
    })


@app.route("/fast/clear-cache", methods=["GET", "POST"])
def fast_clear_cache():
    """Reset cache new_only — semua SMS akan dianggap baru lagi."""
    with _fast_lock:
        _fast_cache.clear()
    return jsonify({"status": "ok", "message": "Cache di-reset"})



# ════════════════════════════════════════════════════════
# BACKGROUND POLLING — Fallback kalau WebSocket tidak trigger
# Poll /sms setiap 10 detik, forward SMS baru ke Telegram
# ════════════════════════════════════════════════════════

_POLL_INTERVAL      = int(os.getenv("TG_POLL_INTERVAL", "30"))  # detik antar re-scan (default 30s)
_SCAN_INTERVAL      = int(os.getenv("TG_SCAN_INTERVAL", "60"))  # alias
_poll_sms_cache     : set = set()   # cache SMS yang sudah dikirim ke TG (in-memory, session)
_poll_initial_done  : bool = False  # flag: scan pertama sudah selesai


def _tg_push_instant(sms_item: dict):
    """Push SMS ke Telegram seketika dari WS event atau polling. Zero delay."""
    try:
        # Cover semua field name variant dari WS maupun polling
        phone = str(
            sms_item.get("phone_number") or
            sms_item.get("originator")   or
            sms_item.get("number")       or
            sms_item.get("cli")          or ""
        ).strip().lstrip("+")

        message = str(
            sms_item.get("otp_message") or
            sms_item.get("message")     or ""
        ).strip()

        if not phone or not message:
            logger.debug(f"[TG-INSTANT] Skip — phone={phone!r} msg={message[:30]!r}")
            return

        key = f"{phone}|{message[:80]}"
        with _tg_sent_lock:
            if key in _tg_sent_cache:
                return
            _tg_sent_cache.add(key)
            _poll_sms_cache.add(key)  # sync ke poll cache juga
            _save_sent_ids(_tg_sent_cache)

        range_name = str(
            sms_item.get("range")          or
            sms_item.get("range_name")     or
            sms_item.get("termination_id") or ""
        )
        sid = str(sms_item.get("sid") or "")
        account = str(sms_item.get("account") or "")

        text = _tg_format_message(phone, message, range_name, sid, account)
        _tg_enqueue(text, _tg_extract_otp(message))
        logger.info(f"[TG-INSTANT] ⚡ {_tg_sensor(phone)} | {message[:50]}")
    except Exception as e:
        logger.error(f"[TG-INSTANT] Error: {e}")


def _tg_polling_worker():
    """
    Polling worker — berjalan permanen setiap hari.

    Logika sederhana dan stabil:
    - _poll_sms_cache  : set key SMS yang sudah dikirim ke TG (in-memory, per hari)
    - tg_sent.json     : persist cache antar restart (dalam 1 hari yang sama)
    - Setiap hari baru : reset kedua cache → scan fresh → kirim semua SMS hari ini
    - Setiap 30 detik  : scan iVAS → kirim yang baru → skip yang sudah ada di cache
    - Tidak ada sleep panjang yang bisa block deteksi SMS baru
    """
    global _poll_sms_cache, _poll_initial_done

    def _do_scan(date_str, label="POLL"):
        """Scan iVAS, kirim SMS baru ke TG. Return jumlah SMS terkirim."""
        _ivas_cache_invalidate()  # selalu fresh dari iVAS
        msgs, err = fetch_all_accounts(date_str, date_str, mode="received")
        if msgs is None:
            logger.warning(f"[TG-{label}] Fetch gagal: {err}")
            return -1  # -1 = error

        sent = 0
        for item in msgs:
            phone   = str(item.get("phone_number") or item.get("number") or "").strip()
            message = str(item.get("otp_message")  or item.get("message") or "").strip()
            if not phone or not message:
                continue

            key = f"{phone}|{message[:80]}"
            if key in _poll_sms_cache:
                continue  # sudah pernah dikirim, skip

            # Catat dulu ke cache
            _poll_sms_cache.add(key)
            with _tg_sent_lock:
                _tg_sent_cache.add(key)

            # Kirim ke TG
            text = _tg_format_message(
                phone, message,
                str(item.get("range") or ""),
                str(item.get("sid")   or ""),
                str(item.get("account") or ""),
            )
            _tg_enqueue(text, _tg_extract_otp(message))
            sent += 1
            logger.info(f"[TG-{label}] 📨 {_tg_sensor(phone)} | {message[:60]}")

        # Simpan ke file setelah setiap scan
        with _tg_sent_lock:
            _save_sent_ids(_tg_sent_cache)

        return sent

    def _reset_daily(date_str):
        """Reset semua cache untuk hari baru."""
        _poll_sms_cache.clear()
        with _tg_sent_lock:
            _tg_sent_cache.clear()
            _save_sent_ids(_tg_sent_cache)
        _ivas_cache_invalidate()
        logger.info(f"[TG-POLL] 🗓 Hari baru {date_str} — cache di-reset")

    # ── SCAN AWAL ───────────────────────────────────────
    logger.info("[TG-POLL] 🔍 Scan awal ke iVAS...")
    _scan_awal_attempts = 0
    while not _poll_initial_done:
        today = datetime.now().strftime("%d/%m/%Y")
        try:
            result = _do_scan(today, "SCAN-AWAL")
            _scan_awal_attempts += 1
            if result == -1:
                logger.warning("[TG-POLL] Scan awal gagal — retry 10s")
                time.sleep(10)
                continue
            # Kalau result=0 pada attempt pertama, mungkin session baru di-inject
            # dan iVAS belum siap → retry sekali lagi
            if result == 0 and _scan_awal_attempts == 1:
                logger.info("[TG-POLL] Scan awal 0 SMS (attempt 1) — tunggu 5s lalu retry")
                time.sleep(5)
                continue
            _poll_initial_done = True
            logger.info(
                f"[TG-POLL] ✅ Scan awal selesai — {result} SMS dikirim ke TG"
                f" | cache: {len(_poll_sms_cache)} | interval: {_POLL_INTERVAL}s"
            )
        except Exception as e:
            logger.error(f"[TG-POLL] Scan awal error: {e} — retry 10s")
            time.sleep(10)

    # ── LOOP PERMANEN ───────────────────────────────────
    _current_date = datetime.now().strftime("%d/%m/%Y")

    while True:
        try:
            today = datetime.now().strftime("%d/%m/%Y")

            # Hari baru → reset cache & scan ulang dari awal
            if today != _current_date:
                _reset_daily(today)
                _current_date = today

                # Scan awal hari baru — kirim semua SMS hari ini
                result = _do_scan(today, "DAILY")
                if result >= 0:
                    logger.info(f"[TG-POLL] ✅ Daily scan: {result} SMS dikirim ke TG")
                time.sleep(_POLL_INTERVAL)
                continue

            # Scan rutin — cek SMS baru
            result = _do_scan(today, "POLL")
            if result > 0:
                logger.info(f"[TG-POLL] ✅ {result} SMS baru → Telegram")
            elif result == 0:
                logger.debug(f"[TG-POLL] Tidak ada SMS baru (cache: {len(_poll_sms_cache)})")
            # result == -1: error, sudah di-log di _do_scan

        except Exception as e:
            logger.error(f"[TG-POLL] Error: {e}")

        time.sleep(_POLL_INTERVAL)


def start_tg_polling():
    """Jalankan polling thread kalau TG aktif."""
    if not _TG_ENABLED:
        logger.warning("[TG-POLL] Skip — TG_BOT_TOKEN / TG_CHAT_IDS belum diset")
        return
    t = threading.Thread(target=_tg_polling_worker, daemon=True, name="tg_polling")
    t.start()
    logger.info("[TG-POLL] Thread started ✓")


# Jalankan polling otomatis saat modul di-import (gunicorn / pterodactyl)
start_tg_polling()


if __name__ == "__main__":
    # ── Support Pterodactyl / Railway / Render / panel lain ──
    # Port dari env var (Pterodactyl set SERVER_PORT / PORT otomatis)
    port = int(
        os.getenv("SERVER_PORT") or   # Pterodactyl
        os.getenv("PORT") or          # Railway / Render / Heroku
        os.getenv("APP_PORT") or      # panel lain
        5000                          # default lokal
    )
    host = os.getenv("HOST", "0.0.0.0")
    debug = os.getenv("DEBUG", "false").lower() == "true"

    logger.info(f"[STARTUP] KY-SHIRO API starting on {host}:{port}")
    logger.info(f"[STARTUP] Domain: https://api.kyshiro.serverkicen.biz.id")
    logger.info(f"[STARTUP] Endpoints: /fast/sms /sms /health /set-cookies /cookies-status")
    logger.info(f"[STARTUP] Accounts loaded: {len(ACCOUNTS)}")

    app.run(host=host, port=port, debug=debug, threaded=True)




