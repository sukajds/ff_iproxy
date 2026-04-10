import glob
import hashlib
import json
import os
import platform
import queue
import re
import socket
import struct
import subprocess
import threading
import time
import traceback
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

from flask import Response, abort, jsonify, redirect, render_template, request
from plugin import F, PluginModuleBase, Job
import requests
try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

try:
    from bs4 import BeautifulSoup, SoupStrainer
except Exception:
    BeautifulSoup = None
    SoupStrainer = None

try:
    import yaml
except Exception:
    yaml = None

from .setup import P

logger = P.logger
package_name = P.package_name
ModelSetting = P.ModelSetting
SystemModelSetting = F.SystemModelSetting
blueprint = P.blueprint
path_app_root = F.path_app_root

STATE_LOCK = threading.Lock()
processes_hls = {}
udp_clients = {}
hls_clients = {}
last_access_hls = {}
last_access_udp = {}
last_access_hls_proxy = {}
cleanup_started = False
_daum_service_cache = None
encoder_probe_cache = {}

DEFAULT_CHANNELS = [
    {'id': 'ch0', 'name': 'YTN', 'url': 'rtp://239.192.47.7:49220'},
    {'id': 'ch2', 'name': 'CJ ONSTYLE+', 'url': 'rtp://239.192.64.7:49220'},
    {'id': 'ch4', 'name': '롯데홈쇼핑', 'url': 'rtp://239.192.56.5:49220'},
    {'id': 'ch5', 'name': 'SBS', 'url': 'udp://239.192.67.98:49220'},
    {'id': 'ch6', 'name': 'CJ ONSTYLE', 'url': 'rtp://239.192.58.11:49220'},
    {'id': 'ch7', 'name': 'KBS2', 'url': 'udp://239.192.67.99:49220'},
    {'id': 'ch8', 'name': '현대홈쇼핑', 'url': 'rtp://239.192.48.4:49220'},
    {'id': 'ch9', 'name': 'KBS1', 'url': 'udp://239.192.67.100:49220'},
    {'id': 'ch10', 'name': 'GS SHOP', 'url': 'rtp://239.192.49.4:49220'},
    {'id': 'ch11', 'name': 'MBC', 'url': 'udp://239.192.67.101:49220'},
    {'id': 'ch12', 'name': '홈&쇼핑', 'url': 'rtp://239.192.81.137:49220'},
    {'id': 'ch14', 'name': 'NS홈쇼핑', 'url': 'rtp://239.192.52.10:49220'},
    {'id': 'ch17', 'name': 'SK stoa', 'url': 'rtp://239.192.64.12:49220'},
    {'id': 'ch19', 'name': '신세계쇼핑', 'url': 'rtp://239.192.64.6:49220'},
    {'id': 'ch21', 'name': '공영쇼핑', 'url': 'rtp://239.192.81.169:49220'},
    {'id': 'ch22', 'name': 'KT알파 쇼핑', 'url': 'rtp://239.192.66.17:49220'},
    {'id': 'ch29', 'name': '쇼핑엔티', 'url': 'rtp://239.192.69.109:49220'},
    {'id': 'ch35', 'name': 'NS Shop+', 'url': 'rtp://239.192.81.143:49220'},
    {'id': 'ch37', 'name': 'W쇼핑', 'url': 'rtp://239.192.81.101:49220'},
    {'id': 'ch72', 'name': 'OCN Movies', 'url': 'udp://239.192.67.227:49220'},
    {'id': 'ch73', 'name': 'OCN', 'url': 'udp://239.192.67.226:49220'},
    {'id': 'ch74', 'name': 'OCN Movies2', 'url': 'udp://239.192.67.228:49220'},
    {'id': 'ch75', 'name': 'Screen', 'url': 'udp://239.192.67.229:49220'},
    {'id': 'ch95', 'name': 'EBS2', 'url': 'rtp://239.192.42.2:49220'},
    {'id': 'ch120', 'name': 'UXN', 'url': 'rtp://239.192.81.38:49220'},
    {'id': 'ch121', 'name': 'UHD Dream TV', 'url': 'rtp://239.192.81.37:49220'},
    {'id': 'ch122', 'name': 'Asia UHD', 'url': 'rtp://239.192.81.36:49220'},
    {'id': 'ch312', 'name': '미드나잇', 'url': 'http://1.214.67.100/vod/62801.m3u8?VOD_RequestID=v2M2-0101-1010-7272-5050-000020180717021633', 'hidden': True},
    {'id': 'ch314', 'name': '허니TV', 'url': 'http://1.214.67.100/vod/74301.m3u8?VOD_RequestID=v2M2-0101-1010-7272-5050-000020180717021633', 'hidden': True},
    {'id': 'ch316', 'name': '플레이보이TV', 'url': 'http://1.214.67.100/vod/62901.m3u8?VOD_RequestID=v2M2-0101-1010-7272-5050-000020180717021633', 'hidden': True},
    {'id': 'ch974', 'name': 'SPOTV Golf&Health', 'url': 'udp://239.192.67.163:49220'},
    {'id': 'ch976', 'name': 'JTBC GOLF', 'url': 'udp://239.192.67.165:49220'},
    {'id': 'ch977', 'name': 'SBS GOLF', 'url': 'udp://239.192.67.164:49220'},
    {'id': 'ch982', 'name': 'SPOTV2', 'url': 'udp://239.192.67.162:49220'},
    {'id': 'ch983', 'name': 'MBC Sports+', 'url': 'udp://239.192.67.131:49220'},
    {'id': 'ch984', 'name': 'SBS Sports', 'url': 'udp://239.192.67.130:49220'},
    {'id': 'ch986', 'name': 'SPOTV', 'url': 'udp://239.192.67.133:49220'},
    {'id': 'ch999', 'name': 'GS MY SHOP', 'url': 'rtp://239.192.66.4:49220'},
]

DEFAULT_CHANNELS = []


def get_bundled_channels_path():
    return Path(__file__).resolve().parent / 'channels.json'


def get_bundled_channels_raw():
    path = get_bundled_channels_path()
    if not path.exists():
        return ''
    try:
        return path.read_text(encoding='utf-8')
    except Exception:
        logger.exception('Failed to load bundled channels.json')
        return ''


def get_default_manual_channels_source_path():
    return str(get_bundled_channels_path())


def get_manual_channels_source_path():
    value = (ModelSetting.get('manual_channels_source_path') or '').strip()
    return value or get_default_manual_channels_source_path()


if yaml is not None:
    class YamlIncludeLoader(yaml.SafeLoader):
        def __init__(self, stream):
            stream_name = getattr(stream, 'name', '')
            self._root = Path(stream_name).resolve().parent if stream_name else Path.cwd()
            super().__init__(stream)


    def _yaml_include(loader, node):
        filename = loader._root / loader.construct_scalar(node)
        with open(filename, 'r', encoding='utf-8') as handle:
            return yaml.load(handle, Loader=YamlIncludeLoader)


    YamlIncludeLoader.add_constructor('!include', _yaml_include)
else:
    YamlIncludeLoader = None

EPG_SOURCE_OPTIONS = ['AUTO']
EPG_AUTO_PRIORITY = ['SK', 'LG', 'DAUM', 'NAVER']
EPG_HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
}
EPG_NAVER_HEADERS = {
    'Referer': 'https://search.naver.com/search.naver?where=nexearch',
    'Sec-CH-UA': '"Chromium";v="123", "Not:A-Brand";v="8", "Google Chrome";v="123"',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}
EPG_DAUM_HEADERS = {}
DAUM_CHANNEL_CATEGORIES = ['지상파', '종합편성', '케이블', '스카이라이프', '해외위성', '라디오']
DAUM_SEARCH_URL = 'https://search.daum.net/search?DA=B3T&w=tot&rtmaxcoll=B3T&q={} 편성표'
DAUM_TITLE_REGEX = r'^(?P<title>.*?)\s?([<\(]?(?P<part>\d{1})부[>\)]?)?\s?(<(?P<subname1>.*)>)?\s?((?P<epnum>\d+)회)?\s?(<(?P<subname2>.*)>)?$'
NAVER_CHANNEL_ALIASES = {
    'CJ ONSTYLE': ['CJ 온스타일'],
    'CJ ONSTYLE+': ['CJ온스타일플러스', 'CJ 온스타일 플러스', 'CJ 온스타일+'],
    'SK stoa': ['SK스토아'],
    '신세계쇼핑': ['신세계 쇼핑', '신세계TV쇼핑', '신세계 TV쇼핑'],
    '쇼핑엔티': ['쇼핑NT', '쇼핑 N T', '쇼핑엔티TV', '쇼핑엔티 TV'],
    'W쇼핑': ['더블유쇼핑', 'W 쇼핑'],
    'OCN Movies': ['OCN Movies 편성표', 'OCN무비스', 'OCN 무비스'],
    'OCN': ['오씨엔'],
    'OCN Movies2': ['OCN Movies 2', 'OCN무비즈2', 'OCN 무비즈2', 'OCN Movies II'],
    'Screen': ['스크린'],
    'EBS2': ['EBS 2', 'EBS plus2', 'EBS Plus2'],
    'UHD Dream TV': ['UHD DreamTV', 'UHD드림TV', '유에이치디 드림티비', '드림티비'],
    'SPOTV Golf&Health': ['SPOTV Golf Health', 'SPOTV Golf & Health', '스포티비 골프앤헬스'],
    'JTBC GOLF': ['JTBC Golf', 'JTBC골프'],
    'SBS GOLF': ['SBS Golf', 'SBS골프'],
    'MBC Sports+': ['MBC SPORTS+', 'MBC Sports Plus', 'MBC 스포츠플러스', 'MBC스포츠플러스'],
    'SBS Sports': ['SBS SPORTS', 'SBS 스포츠'],
    'SPOTV': ['스포티비'],
    'GS MY SHOP': ['GS MYSHOP', 'GS 마이샵', 'GS마이샵'],
}
DAUM_CHANNEL_ALIASES = {
    'CJ ONSTYLE': ['CJ온스타일'],
    'CJ ONSTYLE+': ['CJ온스타일플러스', 'CJ온스타일 플러스'],
    '신세계쇼핑': ['(i)신세계 쇼핑', '신세계 TV쇼핑', '신세계TV쇼핑'],
    '쇼핑엔티': ['쇼핑엔티', '쇼핑NT'],
    'W쇼핑': ['W쇼핑'],
    'Screen': ['스크린'],
    'EBS2': ['EBS2', '지상파 EBS2'],
    'UHD Dream TV': ['UHD Dream TV', 'SKYLIFE UHD Dream TV'],
    'SPOTV Golf&Health': ['SPOTV Golf & Health'],
    'JTBC GOLF': ['JTBC GOLF'],
    'SBS GOLF': ['SBS Golf2', 'SBS GOLF'],
    'MBC Sports+': ['MBC Sports+', 'MBC SPORTS+', 'MBC 스포츠플러스'],
    'SBS Sports': ['SBS Sports', 'SBS 스포츠'],
    'SPOTV': ['SPOTV', 'SKYLIFE SPOTV'],
    'GS MY SHOP': ['GS MY SHOP'],
}


def ensure_cleanup_thread():
    global cleanup_started
    with STATE_LOCK:
        if cleanup_started:
            return
        cleanup_started = True
    thread = threading.Thread(target=cleanup_loop, daemon=True)
    thread.start()


def get_plugin_data_dir():
    path = Path(__file__).resolve().parent / 'data'
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_hls_dir():
    path = get_plugin_data_dir() / 'hls'
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_ffmpeg_bin():
    if platform.system() == 'Windows':
        candidate = os.path.join(path_app_root, 'bin', 'Windows', 'ffmpeg.exe')
        if os.path.exists(candidate):
            return candidate
    return ModelSetting.get('ffmpeg_path') or 'ffmpeg'


def get_idle_timeout():
    try:
        return max(5, int(ModelSetting.get('idle_timeout')))
    except Exception:
        return 30


def get_public_api_key():
    use_apikey = SystemModelSetting.get_bool('use_apikey')
    if not use_apikey:
        return None
    apikey = SystemModelSetting.get('apikey')
    return apikey or None


def require_api_key(req):
    expected = get_public_api_key()
    if expected is None:
        return
    actual = req.args.get('apikey')
    if actual != expected:
        abort(403)


def get_base_url(req=None):
    ddns = SystemModelSetting.get('ddns')
    if ddns:
        return ddns.rstrip('/') + f'/{package_name}'
    if req is None:
        req = request
    return req.url_root.rstrip('/') + f'/{package_name}'


def with_apikey(url):
    apikey = get_public_api_key()
    if not apikey:
        return url
    separator = '&' if '?' in url else '?'
    return f'{url}{separator}apikey={apikey}'


def with_request_apikey(url, req=None):
    if req is None:
        req = request
    apikey = ''
    try:
        apikey = (req.args.get('apikey') or '').strip()
    except Exception:
        apikey = ''
    if apikey:
        separator = '&' if '?' in url else '?'
        return f'{url}{separator}apikey={apikey}'
    return with_apikey(url)


def normalize_scheme_type(url, explicit_type=None):
    if explicit_type:
        explicit = explicit_type.lower()
        if explicit in ('rtp', 'udp', 'http', 'https', 'http_hls', 'http_stream'):
            if explicit == 'rtp':
                return 'rtp'
            if explicit in ('http', 'https'):
                return 'http_hls'
            if explicit in ('http_hls', 'http_stream'):
                return explicit
            return 'udp'
        return explicit
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == 'rtp':
        return 'rtp'
    if parsed.scheme == 'udp':
        return 'udp'
    if parsed.scheme in ('http', 'https'):
        return 'http_hls'
    return 'udp'


def detect_iproxy_api_type(url):
    parsed = urllib.parse.urlparse(url or '')
    if parsed.scheme not in ('http', 'https'):
        return ''
    path = (parsed.path or '').lower()
    query = urllib.parse.parse_qs(parsed.query or '', keep_blank_values=True)
    if '/api/hls/' in path:
        return 'http_hls'
    if path.endswith('/api/playlist.m3u') or '/api/playlist.m3u/' in path:
        mode = ((query.get('mode') or [''])[0] or '').strip().lower()
        if mode == 'hls':
            return 'http_hls'
    return ''


def is_forward_channel_url(url):
    text = str(url or '').strip()
    normalized = normalize_url_to_localhost_if_ddns(text)
    if normalized != text:
        return True
    parsed = urllib.parse.urlparse(text)
    host = (parsed.hostname or '').strip().lower()
    if host not in ('localhost', '127.0.0.1'):
        return False
    return bool(detect_iproxy_api_type(url))


def apply_known_channel_types(items):
    result = []
    for item in items or []:
        normalized = dict(item)
        detected_type = detect_iproxy_api_type(normalized.get('url'))
        if detected_type:
            normalized['type'] = detected_type
            normalized['type_diagnosed'] = True
            normalized['rtp'] = False
        result.append(normalized)
    return result


def normalize_epg_source(value):
    return 'AUTO'


def make_channel_id(index):
    return f'ch{index:03d}'


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'y', 'yes', 'on')


def parse_line_channels(raw):
    channels = []
    for line in raw.splitlines():
        text = line.strip()
        if not text or text.startswith('#'):
            continue
        if '|' in text:
            parts = [x.strip() for x in text.split('|')]
        elif '\t' in text:
            parts = [x.strip() for x in text.split('\t')]
        else:
            parts = [x.strip() for x in text.split(',', 1)]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            continue
        name, url = parts[0], parts[1]
        channels.append({
            'name': name,
            'url': url,
            'type': '',
            'epg_enabled': True,
            'hidden': False,
            'rtp': False,
            'source': 'line',
        })
    return channels


def parse_manual_channels(raw):
    text = (raw or '').strip()
    if not text:
        return []
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError('수동 채널 형식은 배열이어야 합니다.')

    items = []
    for index, value in enumerate(data, start=1):
        if not isinstance(value, dict):
            continue
        name = (value.get('name') or '').strip()
        url = (value.get('url') or '').strip()
        if not name or not url:
            continue
        explicit_type = (value.get('type') or '').strip().lower()
        type_diagnosed = parse_bool(value.get('type_diagnosed'))
        detected_type = normalize_scheme_type(url, explicit_type) if (explicit_type and type_diagnosed) else ''
        items.append({
            'id_hint': value.get('id') or make_channel_id(index),
            'name': name,
            'url': url,
            'type': detected_type,
            'type_diagnosed': type_diagnosed,
            'epg_source': normalize_epg_source(value.get('epg_source')),
            'epg_enabled': parse_bool(value.get('epg_enabled')) if value.get('epg_enabled') is not None else True,
            'hidden': parse_bool(value.get('hidden')),
            'rtp': detected_type == 'rtp',
            'source': 'manual',
        })
    return items


def get_manual_channels_raw():
    raw = (ModelSetting.get('manual_channels') or '').strip()
    if raw in ('', '[]'):
        return '[]'
    return raw


def update_manual_channel_type(channel_id, detected_type, url=None):
    raw = get_manual_channels_raw()
    try:
        data = json.loads(raw)
    except Exception:
        return False
    if not isinstance(data, list):
        return False

    updated = False
    for index, value in enumerate(data, start=1):
        if not isinstance(value, dict):
            continue
        current_id = str(value.get('id') or make_channel_id(index))
        current_url = (value.get('url') or '').strip()
        if current_id != str(channel_id):
            continue
        if url and current_url and current_url != url:
            continue
        value['id'] = current_id
        value['type'] = detected_type or ''
        value['type_diagnosed'] = True if detected_type else False
        value['rtp'] = True if detected_type == 'rtp' else False
        updated = True
        break

    if updated:
        ModelSetting.set('manual_channels', json.dumps(data, ensure_ascii=False))
    return updated


def parse_json_channels(raw):
    text = raw.strip()
    if not text:
        return []
    data = json.loads(text)
    return parse_structured_channels(data, source='json')


def _parse_structured_channels_legacy(data, source='json'):
    items = []
    if isinstance(data, dict):
        iterable = list(data.items())
    elif isinstance(data, list):
        iterable = list(enumerate(data, start=1))
    else:
        raise ValueError('JSON 채널 형식은 객체 또는 배열이어야 합니다.')

    for key, value in iterable:
        if not isinstance(value, dict):
            continue
        url = (value.get('url') or '').strip()
        name = (value.get('name') or str(key)).strip()
        if not url or not name:
            continue
        items.append({
            'id_hint': str(key),
            'name': name,
            'url': url,
            'type': '',
            'type_diagnosed': False,
            'epg_enabled': parse_bool(value.get('epg_enabled')) if value.get('epg_enabled') is not None else True,
            'hidden': parse_bool(value.get('hidden')),
            'rtp': False,
            'source': 'json',
        })
    return items


def parse_structured_channels(data, source='json'):
    items = []
    if isinstance(data, dict):
        if len(data) == 1 and isinstance(next(iter(data.values())), (list, dict)):
            data = next(iter(data.values()))
        if isinstance(data, dict):
            iterable = list(data.items())
        else:
            iterable = list(enumerate(data, start=1))
    elif isinstance(data, list):
        iterable = list(enumerate(data, start=1))
    else:
        raise ValueError(f'{str(source).upper()} 채널 형식은 객체 또는 배열이어야 합니다.')

    for key, value in iterable:
        if isinstance(value, str):
            value = {'name': str(key), 'url': value}
        if not isinstance(value, dict):
            continue
        url = (value.get('url') or value.get('source') or value.get('link') or '').strip()
        name = (value.get('name') or value.get('title') or value.get('channel') or str(key)).strip()
        if not url or not name:
            continue
        explicit_type = (value.get('type') or value.get('scheme') or '').strip().lower()
        type_diagnosed = parse_bool(value.get('type_diagnosed')) and bool(explicit_type)
        detected_type = normalize_scheme_type(url, explicit_type) if type_diagnosed else ''
        items.append({
            'id_hint': str(value.get('id') or value.get('id_hint') or key),
            'name': name,
            'url': url,
            'type': detected_type,
            'type_diagnosed': type_diagnosed,
            'epg_enabled': parse_bool(value.get('epg_enabled')) if value.get('epg_enabled') is not None else True,
            'hidden': parse_bool(value.get('hidden')),
            'rtp': detected_type == 'rtp',
            'source': source,
        })
    return items


def parse_simple_yaml_scalar(value):
    text = str(value or '').strip()
    if text == '':
        return ''
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    lowered = text.lower()
    if lowered in ('true', 'yes', 'on'):
        return True
    if lowered in ('false', 'no', 'off'):
        return False
    if lowered in ('null', 'none', '~'):
        return None
    if re.fullmatch(r'-?\d+', text):
        try:
            return int(text)
        except Exception:
            return text
    return text


def parse_simple_yaml(raw):
    lines = raw.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        cleaned.append(line.rstrip())
    if not cleaned:
        return []

    first = cleaned[0].lstrip()
    if first.startswith('- '):
        items = []
        current = None
        for line in cleaned:
            stripped = line.lstrip()
            if stripped.startswith('- '):
                if current is not None:
                    items.append(current)
                current = {}
                remainder = stripped[2:].strip()
                if remainder and ':' in remainder:
                    key, value = remainder.split(':', 1)
                    current[key.strip()] = parse_simple_yaml_scalar(value)
                elif remainder:
                    current['url'] = parse_simple_yaml_scalar(remainder)
                continue
            if current is None or ':' not in stripped:
                continue
            key, value = stripped.split(':', 1)
            current[key.strip()] = parse_simple_yaml_scalar(value)
        if current is not None:
            items.append(current)
        return items

    root = {}
    current_key = None
    current_value = None
    for line in cleaned:
        indent = len(line) - len(line.lstrip(' '))
        stripped = line.strip()
        if indent == 0:
            if current_key is not None:
                root[current_key] = current_value
            if ':' not in stripped:
                continue
            key, value = stripped.split(':', 1)
            current_key = key.strip()
            value = value.strip()
            if value == '':
                current_value = {}
            else:
                current_value = parse_simple_yaml_scalar(value)
        elif isinstance(current_value, dict) and ':' in stripped:
            key, value = stripped.split(':', 1)
            current_value[key.strip()] = parse_simple_yaml_scalar(value)
    if current_key is not None:
        root[current_key] = current_value
    return root


def parse_yaml_channels(raw):
    text = (raw or '').strip()
    if not text:
        return []
    data = yaml.safe_load(text) if yaml is not None else parse_simple_yaml(text)
    return parse_structured_channels(data, source='yaml')


def parse_m3u_channels(raw):
    text = (raw or '').strip()
    if not text:
        return []
    items = []
    current = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('#EXTM3U'):
            continue
        if stripped.startswith('#EXTINF'):
            current = {}
            match = re.match(r'#EXTINF:(?P<duration>[^,]*),(?P<name>.*)$', stripped)
            if match:
                current['name'] = match.group('name').strip()
            tvg_id = re.search(r'tvg-id="([^"]+)"', stripped)
            tvg_name = re.search(r'tvg-name="([^"]+)"', stripped)
            if tvg_id:
                current['id_hint'] = tvg_id.group(1).strip()
            if tvg_name and not current.get('name'):
                current['name'] = tvg_name.group(1).strip()
            continue
        if stripped.startswith('#'):
            continue
        name = (current.get('name') or '').strip() or f'Channel {len(items) + 1}'
        items.append({
            'id_hint': current.get('id_hint') or make_channel_id(len(items) + 1),
            'name': name,
            'url': stripped,
            'type': '',
            'type_diagnosed': False,
            'epg_enabled': True,
            'hidden': False,
            'rtp': False,
            'source': 'm3u',
        })
        current = {}
    return items


def parse_import_channels(raw):
    text = (raw or '').strip()
    if not text:
        raise ValueError('가져올 채널 목록을 입력하세요.')

    if text.startswith('#EXTM3U') or '#EXTINF:' in text:
        items = parse_m3u_channels(text)
        detected = 'm3u8'
    else:
        for fmt, parser in (('json', parse_json_channels), ('yaml', parse_yaml_channels)):
            try:
                items = parser(text)
                detected = fmt
                break
            except Exception:
                continue
        else:
            raise ValueError('지원하지 않는 형식입니다. json, yaml, m3u8 형식을 사용하세요.')

    if not items:
        raise ValueError(f'입력한 {detected} 형식에서 채널을 찾지 못했습니다.')
    return {
        'format': detected,
        'channels': apply_known_channel_types(items),
    }


def load_import_source(raw):
    text = (raw or '').strip()
    if not text:
        raise ValueError('가져올 채널 목록을 입력하세요.')

    if any(token in text for token in ('\n', '\r', '#EXTINF', '{', '[')):
        return {
            'content': text,
            'source': 'inline',
        }

    if re.match(r'^https?://', text, re.I):
        target_url = normalize_url_to_localhost_if_ddns(text)
        req = urllib.request.Request(target_url, headers={'User-Agent': ModelSetting.get('user_agent') or 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode('utf-8', errors='replace')
        return {
            'content': content,
            'source': text,
        }

    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (Path(__file__).resolve().parent / path).resolve()
    if not path.exists() or not path.is_file():
        raise ValueError(f'파일을 찾을 수 없습니다: {path}')
    try:
        content = path.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = path.read_text(encoding='utf-8', errors='replace')
    return {
        'content': content,
        'source': str(path),
    }


def import_channels_from_source(raw):
    loaded = load_import_source(raw)
    parsed = parse_import_channels(loaded['content'])
    parsed['source'] = loaded['source']
    parsed['channels'] = restore_localhost_urls_to_source_host(parsed.get('channels', []), loaded['source'])
    parsed['channels'] = mark_forward_channels(parsed.get('channels', []))
    return parsed


def normalize_url_to_localhost_if_ddns(url):
    ddns = (SystemModelSetting.get('ddns') or '').strip()
    if not ddns:
        return url

    ddns_parsed = urllib.parse.urlparse(ddns)
    ddns_host = (ddns_parsed.hostname or '').strip().lower()
    if not ddns_host:
        return url
    ddns_port = ddns_parsed.port

    parsed = urllib.parse.urlparse(str(url or '').strip())
    host = (parsed.hostname or '').strip().lower()
    port = parsed.port
    if parsed.scheme not in ('http', 'https'):
        return url
    if host != ddns_host:
        return url
    if not (ddns_port in (None, port) or port in (None, ddns_port)):
        return url

    netloc = 'localhost'
    if port is not None:
        netloc = f'{netloc}:{port}'
    elif ddns_port is not None:
        netloc = f'{netloc}:{ddns_port}'
    return urllib.parse.urlunparse((
        parsed.scheme,
        netloc,
        parsed.path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))


def restore_localhost_urls_to_source_host(channels, source_url):
    source_text = str(source_url or '').strip()
    if not re.match(r'^https?://', source_text, re.I):
        return channels

    source_parsed = urllib.parse.urlparse(source_text)
    source_host = (source_parsed.hostname or '').strip()
    if not source_host:
        return channels

    normalized = []
    for channel in channels or []:
        item = dict(channel)
        url = str(item.get('url') or '').strip()
        parsed = urllib.parse.urlparse(url)
        host = (parsed.hostname or '').strip().lower()
        if parsed.scheme in ('http', 'https') and host in ('localhost', '127.0.0.1'):
            netloc = source_host
            if parsed.port is not None:
                netloc = f'{netloc}:{parsed.port}'
            elif source_parsed.port is not None:
                netloc = f'{netloc}:{source_parsed.port}'
            item['url'] = urllib.parse.urlunparse((
                source_parsed.scheme or parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            ))
        normalized.append(item)
    return normalized


def mark_forward_channels(channels):
    normalized = []
    for channel in channels or []:
        item = dict(channel)
        if is_forward_channel_url(item.get('url') or ''):
            item['type'] = 'forward'
            item['type_diagnosed'] = True
            item['rtp'] = False
        normalized.append(item)
    return normalized


def get_alive_yaml_path():
    return Path(F.path_data) / 'db' / 'alive.yaml'


def _escape_alive_yaml_scalar(value):
    text = str(value or '')
    return text.replace('\\', '\\\\').replace('"', '\\"')


def build_alive_fix_url_block(channels):
    lines = [
        '  fix_url:',
    ]
    for channel in channels:
        channel_name = str(channel.get('name') or '').strip()
        channel_url = str(channel.get('url') or '').strip()
        if not channel_name or not channel_url:
            continue
        escaped_name = _escape_alive_yaml_scalar(channel_name)
        escaped_url = _escape_alive_yaml_scalar(channel_url)
        lines.extend([
            f'    "{escaped_name}":',
            f'      name: "{escaped_name}"',
            f'      url: "{escaped_url}"',
        ])
    return '\n'.join(lines) + '\n'


def parse_alive_fix_url_block(lines, fix_url_indent):
    if not lines:
        return []
    normalized_lines = []
    for line in lines:
        if line.startswith(' ' * fix_url_indent):
            normalized_lines.append(line[fix_url_indent:])
        else:
            normalized_lines.append(line.lstrip(' '))
    block_text = '\n'.join(normalized_lines) + '\n'

    if yaml is None:
        return []

    try:
        parsed = yaml.safe_load(block_text) or {}
    except Exception:
        logger.exception('Failed to parse existing fix_url block from alive.yaml')
        return []

    fix_url = parsed.get('fix_url')
    if not isinstance(fix_url, dict):
        return []

    channels = []
    for key, item in fix_url.items():
        if not isinstance(item, dict):
            continue
        channel_name = str(item.get('name') or key or '').strip()
        channel_url = str(item.get('url') or '').strip()
        if not channel_name or not channel_url:
            continue
        channels.append({
            'name': channel_name,
            'url': channel_url,
        })
    return channels


def merge_alive_fix_url_channels(existing_channels, new_channels):
    merged = [
        {
            'name': str(channel.get('name') or '').strip(),
            'url': str(channel.get('url') or '').strip(),
        }
        for channel in existing_channels
        if str(channel.get('name') or '').strip() and str(channel.get('url') or '').strip()
    ]

    for channel in new_channels:
        channel_name = str(channel.get('name') or '').strip()
        channel_url = str(channel.get('url') or '').strip()
        if not channel_name or not channel_url:
            continue

        replace_index = None
        for index, existing in enumerate(merged):
            existing_name = str(existing.get('name') or '').strip()
            existing_url = str(existing.get('url') or '').strip()
            if existing_name == channel_name or existing_url == channel_url:
                replace_index = index
                break

        if replace_index is None:
            merged.append({
                'name': channel_name,
                'url': channel_url,
            })
        else:
            merged[replace_index] = {
                'name': channel_name,
                'url': channel_url,
            }

    return merged


def get_alive_yaml_mode():
    mode = (ModelSetting.get('alive_yaml_mode') or 'stream').strip().lower()
    return 'hls' if mode == 'hls' else 'stream'


def _find_block_end(lines, start_index, parent_indent):
    for index in range(start_index + 1, len(lines)):
        stripped = lines[index].strip()
        if stripped == '':
            continue
        current_indent = len(lines[index]) - len(lines[index].lstrip(' '))
        if current_indent <= parent_indent:
            return index
    return len(lines)


def update_alive_fix_url(req):
    path = get_alive_yaml_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        original_text = path.read_text(encoding='utf-8')
    else:
        original_text = ''
    lines = original_text.splitlines()

    alive_channels = []
    alive_mode = get_alive_yaml_mode()
    include_hidden = ModelSetting.get_bool('include_hidden_channels')
    for channel in get_channels():
        if channel.get('hidden') and not include_hidden:
            continue
        payload = make_channel_payload(channel, req)
        channel_name = str(payload.get('name') or '').strip()
        if not channel_name:
            continue
        alive_channels.append({
            'name': channel_name,
            'url': payload['hls_url'] if alive_mode == 'hls' else payload['stream_url'],
        })

    channel_source_index = None
    channel_source_indent = 0
    for index, line in enumerate(lines):
        if line.strip() == 'channel_source:':
            channel_source_index = index
            channel_source_indent = len(line) - len(line.lstrip(' '))
            break

    fix_url_index = None
    fix_url_end = None
    if channel_source_index is not None:
        channel_source_end = _find_block_end(lines, channel_source_index, channel_source_indent)
        fix_url_indent = channel_source_indent + 2
        for index in range(channel_source_index + 1, channel_source_end):
            if lines[index].strip() == 'fix_url:':
                current_indent = len(lines[index]) - len(lines[index].lstrip(' '))
                if current_indent == fix_url_indent:
                    fix_url_index = index
                    break
        if fix_url_index is not None:
            fix_url_end = _find_block_end(lines, fix_url_index, fix_url_indent)

    block_text = build_alive_fix_url_block(alive_channels).rstrip('\n')

    if channel_source_index is None:
        if original_text and not original_text.endswith('\n'):
            original_text += '\n'
        prefix = original_text
        if prefix and not prefix.endswith('\n\n'):
            prefix += '\n'
        new_text = prefix + 'channel_source:\n' + block_text + '\n'
    else:
        if fix_url_index is None:
            new_lines = lines[:channel_source_index + 1] + block_text.splitlines() + lines[channel_source_index + 1:]
        else:
            new_lines = lines[:fix_url_index] + block_text.splitlines() + lines[fix_url_end:]
        new_text = '\n'.join(new_lines)
        if original_text.endswith('\n') or not new_text.endswith('\n'):
            new_text += '\n'

    path.write_text(new_text, encoding='utf-8')
    return {
        'path': str(path),
        'count': len(alive_channels),
    }


def get_channels():
    manual_channels = parse_manual_channels(get_manual_channels_raw())
    legacy_line_channels = []
    if not manual_channels:
        legacy_line_channels = parse_line_channels(ModelSetting.get('line_channels') or '')
    merged = []
    index = 1
    for item in manual_channels + legacy_line_channels:
        channel_id = item.get('id_hint') or make_channel_id(index)
        normalized = {
            'id': re.sub(r'[^a-zA-Z0-9_-]', '_', str(channel_id)) or make_channel_id(index),
            'name': item['name'],
            'url': item['url'],
            'type': item['type'],
            'type_diagnosed': item.get('type_diagnosed', False),
            'epg_source': normalize_epg_source(item.get('epg_source')),
            'epg_enabled': item.get('epg_enabled', True),
            'hidden': item.get('hidden', False),
            'rtp': item.get('rtp', False),
            'source': item.get('source', 'line'),
        }
        merged.append(normalized)
        index += 1
    return apply_known_channel_types(merged)


def get_channel_map():
    return {item['id']: item for item in get_channels()}


def fetch_url(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': ModelSetting.get('user_agent')})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        logger.error('FETCH ERROR %s : %s', url, e)
        return None


def get_hls_default_options():
    return {
        'codec_mode': normalize_hls_codec_mode(ModelSetting.get('hls_codec_mode')),
        'resolution': normalize_hls_resolution_value(ModelSetting.get('hls_target_resolution')),
        'preset': normalize_hls_preset_value(ModelSetting.get('hls_preset')),
    }


def normalize_hls_codec_mode(value):
    text = (value or 'original').strip().lower()
    return text if text in ('original', 'h264') else 'original'


def normalize_hls_resolution_value(value):
    text = (value or 'original').strip().lower()
    return text if text in ('original', '1080p', '720p', '480p') else 'original'


def normalize_hls_preset_value(value):
    text = (value or 'normal').strip().lower()
    return text if text in ('fast', 'normal', 'quality') else 'normal'


def get_hls_request_options(req=None):
    options = get_hls_default_options()
    if req is None:
        return options
    options['codec_mode'] = normalize_hls_codec_mode(req.args.get('codec_mode') or options['codec_mode'])
    options['resolution'] = normalize_hls_resolution_value(req.args.get('resolution') or options['resolution'])
    options['preset'] = normalize_hls_preset_value(req.args.get('preset') or options['preset'])
    return options


def get_hls_session_key(channel_id, options=None):
    if options is None:
        effective = get_hls_default_options()
    else:
        effective = {
            'codec_mode': normalize_hls_codec_mode(options.get('codec_mode')),
            'resolution': normalize_hls_resolution_value(options.get('resolution')),
            'preset': normalize_hls_preset_value(options.get('preset')),
        }
    digest = hashlib.md5(json.dumps(effective, sort_keys=True).encode('utf-8')).hexdigest()[:12]
    return f'{channel_id}__{digest}'


def get_hls_playlist_path(hls_key):
    return get_hls_dir() / f'{hls_key}.m3u8'


def get_hls_segment_pattern(hls_key):
    return str(get_hls_dir() / f'{hls_key}_%03d.ts')


def get_hls_key_from_segment_filename(filename):
    stem = os.path.splitext(os.path.basename(filename))[0]
    if '_' not in stem:
        return stem
    return stem.rsplit('_', 1)[0]


def stop_hls(hls_key):
    with STATE_LOCK:
        proc = processes_hls.pop(hls_key, None)
        last_access_hls.pop(hls_key, None)
    if proc is not None:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass
    hls_dir = get_hls_dir()
    for file_path in glob.glob(str(hls_dir / f'{hls_key}_*.ts')):
        try:
            os.remove(file_path)
        except Exception:
            pass
    playlist = hls_dir / f'{hls_key}.m3u8'
    if playlist.exists():
        try:
            playlist.unlink()
        except Exception:
            pass


def _log_process_output(proc, prefix):
    try:
        if proc.stderr is None:
            return
        for raw_line in proc.stderr:
            line = raw_line.rstrip()
            if line:
                logger.info('%s %s', prefix, line)
    except Exception as e:
        logger.warning('%s output logger error: %s', prefix, e)


def hls_request_uses_proxy(channel, options=None):
    if channel.get('type') != 'http_hls':
        return False
    if options is None:
        options = get_hls_default_options()
    codec_mode = normalize_hls_codec_mode(options.get('codec_mode'))
    resolution = normalize_hls_resolution_value(options.get('resolution'))
    return codec_mode == 'original' and resolution == 'original'


def start_hls(channel_id, options=None):
    channels = get_channel_map()
    channel = channels.get(channel_id)
    if channel is None or channel['type'] not in ('udp', 'rtp', 'http_stream', 'http_hls'):
        return ''
    hls_options = get_hls_default_options() if options is None else dict(options)
    hls_key = get_hls_session_key(channel_id, hls_options)

    with STATE_LOCK:
        proc = processes_hls.get(hls_key)
        if proc is not None and proc.poll() is None:
            last_access_hls[hls_key] = time.time()
            return hls_key

    ffmpeg_bin = get_ffmpeg_bin()
    source = channel['url']
    input_url = source
    if source.startswith('udp://'):
        input_url = f'{source}?fifo_size=5000000&overrun_nonfatal=1'
    codec_mode = normalize_hls_codec_mode(hls_options.get('codec_mode'))
    video_codec = ''
    source_height = 0
    should_transcode_h264 = False
    target_height = get_hls_target_height(hls_options)
    target_width, _ = get_hls_target_dimensions(hls_options)
    probe = ffmpeg_probe_input(input_url, timeout=8)
    video_codec = normalize_video_codec_name(probe.get('video_codec'))
    source_height = int(probe.get('height') or 0)
    if codec_mode == 'h264':
        should_transcode_h264 = video_codec in ('hevc', 'h265')
    should_scale = bool(target_height and source_height and target_height < source_height)
    should_transcode = should_transcode_h264 or should_scale
    logger.info('[HLS] channel=%s codec_mode=%s detected_codec=%s source_height=%s target_height=%s transcode=%s', channel.get('name'), codec_mode, video_codec or '', source_height, target_height, should_transcode)
    selected_encoder = (ModelSetting.get('hls_h264_encoder') or 'libx264').strip()

    def build_hls_command(encoder_name):
        cmd = [
            ffmpeg_bin,
            '-loglevel', 'info',
            '-fflags', '+genpts+discardcorrupt',
        ]
        if input_url.startswith('http://') or input_url.startswith('https://'):
            user_agent = (ModelSetting.get('user_agent') or '').strip()
            if user_agent:
                cmd.extend(['-user_agent', user_agent])
        cmd.extend(['-i', input_url])
        if should_transcode:
            encoder = encoder_name or 'libx264'
            vaapi_device = (ModelSetting.get('hls_vaapi_device') or '').strip()
            preset = map_encoder_preset(encoder, get_hls_preset_value(hls_options))
            cmd.extend([
                '-map', '0:v:0',
                '-map', '0:a?',
            ])
            if encoder in ('h264_vaapi',):
                cmd[1:1] = ['-vaapi_device', vaapi_device or '/dev/dri/renderD128']
                vf = 'format=nv12,hwupload'
                if should_scale and target_width and target_height:
                    vf += f',scale_vaapi={target_width}:{target_height}'
                cmd.extend([
                    '-vf', vf,
                    '-c:v', encoder,
                    '-b:v', '1500k',
                    '-maxrate', '1500k',
                    '-bufsize', '3000k',
                    '-g', '60',
                ])
            else:
                if should_scale and target_width and target_height:
                    cmd.extend(['-vf', f'scale={target_width}:{target_height}'])
                cmd.extend(['-c:v', encoder])
                if encoder == 'libx264':
                    if preset:
                        cmd.extend(['-preset', preset])
                    cmd.extend(['-crf', '23'])
                elif encoder == 'h264_nvenc':
                    if preset:
                        cmd.extend(['-preset', preset])
                    cmd.extend(['-pix_fmt', 'nv12'])
                elif encoder == 'h264_qsv' and preset:
                    cmd.extend(['-preset', preset])
                cmd.extend([
                    '-b:v', '1500k',
                    '-maxrate', '1500k',
                    '-bufsize', '3000k',
                    '-g', '60',
                ])
            cmd.extend([
                '-c:a', 'aac',
                '-b:a', '192k',
                '-ar', '48000',
            ])
        else:
            cmd.extend([
                '-map', '0',
                '-c', 'copy',
            ])
        cmd.extend([
            '-f', 'hls',
            '-hls_time', str(ModelSetting.get('hls_time')),
            '-hls_list_size', str(ModelSetting.get('hls_list_size')),
            '-hls_flags', 'delete_segments+append_list+independent_segments+temp_file',
            '-hls_segment_filename', get_hls_segment_pattern(hls_key),
            str(get_hls_playlist_path(hls_key)),
        ])
        return cmd

    cmd = build_hls_command(selected_encoder)
    if should_transcode:
        vaapi_device = (ModelSetting.get('hls_vaapi_device') or '').strip()
    stop_hls(hls_key)
    logger.info('[HLS] channel=%s encoder=%s vaapi_device=%s', channel.get('name'), selected_encoder or 'copy', vaapi_device if should_transcode else '')
    logger.info('[HLS] command=%s', subprocess.list2cmdline(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    threading.Thread(target=_log_process_output, args=(proc, f'[FFMPEG:{channel_id}]'), daemon=True).start()
    if should_transcode and selected_encoder != 'libx264':
        time.sleep(1)
        if proc.poll() is not None:
            logger.warning('[HLS] channel=%s encoder=%s exited early. retrying with libx264', channel.get('name'), selected_encoder)
            cmd = build_hls_command('libx264')
            logger.info('[HLS] retry command=%s', subprocess.list2cmdline(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
            )
            threading.Thread(target=_log_process_output, args=(proc, f'[FFMPEG:{channel_id}:retry]'), daemon=True).start()
    with STATE_LOCK:
        processes_hls[hls_key] = proc
        last_access_hls[hls_key] = time.time()
    return hls_key


TS_PACKET_SIZE = 188


def strip_rtp_header(data):
    if len(data) < 12:
        return data
    version = (data[0] >> 6) & 0x3
    if version != 2:
        return data
    cc = data[0] & 0x0F
    has_ext = (data[0] >> 4) & 0x1
    header_len = 12 + cc * 4
    if has_ext:
        if len(data) < header_len + 4:
            return data
        ext_len = ((data[header_len + 2] << 8) | data[header_len + 3]) * 4
        header_len += 4 + ext_len
    if header_len > len(data):
        return data
    return data[header_len:]


def align_ts_packets(data):
    start = -1
    for i in range(min(len(data), TS_PACKET_SIZE)):
        if data[i] == 0x47:
            next_pos = i + TS_PACKET_SIZE
            if next_pos >= len(data) or data[next_pos] == 0x47:
                start = i
                break
    if start < 0:
        return b''
    payload = data[start:]
    aligned_len = (len(payload) // TS_PACKET_SIZE) * TS_PACKET_SIZE
    return payload[:aligned_len]


def ffmpeg_probe_input(input_url, timeout=8):
    ffmpeg_bin = get_ffmpeg_bin()
    cmd = [
        ffmpeg_bin,
        '-hide_banner',
        '-nostdin',
        '-loglevel', 'info',
    ]
    if input_url.startswith('http://') or input_url.startswith('https://'):
        user_agent = (ModelSetting.get('user_agent') or '').strip()
        if user_agent:
            cmd.extend(['-user_agent', user_agent])
    cmd.extend([
        '-analyzeduration', '1000000',
        '-probesize', '1000000',
        '-i', input_url,
        '-map', '0:0',
        '-t', '1',
        '-c', 'copy',
        '-f', 'null',
        '-',
    ])
    started_at = time.time()
    logger.info('[DIAG] ffmpeg_probe start url=%s timeout=%s cmd=%s', input_url, timeout, ' '.join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
    timed_out = False
    try:
        _, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        _, stderr = proc.communicate()
    success = ('Input #0' in stderr and 'Stream #0' in stderr) or ('Output #0' in stderr)
    width, height = extract_ffmpeg_video_resolution(stderr)
    result = {
        'success': success,
        'timed_out': timed_out,
        'stderr': stderr,
        'returncode': proc.returncode,
        'command': cmd,
        'video_codec': extract_ffmpeg_video_codec(stderr),
        'width': width,
        'height': height,
    }
    elapsed = time.time() - started_at
    logger.info(
        '[DIAG] ffmpeg_probe done url=%s elapsed=%.2fs success=%s timeout=%s returncode=%s codec=%s size=%sx%s',
        input_url,
        elapsed,
        result['success'],
        result['timed_out'],
        result['returncode'],
        result['video_codec'] or '',
        result['width'],
        result['height'],
    )
    stderr_preview = ' | '.join([line.strip() for line in (stderr or '').splitlines() if line.strip()][:8])
    if stderr_preview:
        logger.info('[DIAG] ffmpeg_probe stderr url=%s preview=%s', input_url, stderr_preview)
    return result


def extract_ffmpeg_video_codec(stderr):
    match = re.search(r'Video:\s*([a-zA-Z0-9_]+)', stderr or '')
    if not match:
        return ''
    return match.group(1).strip().lower()


def extract_ffmpeg_video_resolution(stderr):
    match = re.search(r'Video:\s*[a-zA-Z0-9_]+.*?,\s*(\d{2,5})x(\d{2,5})', stderr or '')
    if not match:
        return 0, 0
    try:
        return int(match.group(1)), int(match.group(2))
    except Exception:
        return 0, 0


def normalize_video_codec_name(codec_name):
    text = (codec_name or '').strip().lower()
    if text in ('hevc', 'h265', 'libx265'):
        return 'hevc'
    if text in ('h264', 'avc1', 'libx264'):
        return 'h264'
    return text


def get_hls_target_height(options=None):
    if options is None:
        options = get_hls_default_options()
    value = normalize_hls_resolution_value(options.get('resolution'))
    return {
        '1080p': 1080,
        '720p': 720,
        '480p': 480,
    }.get(value, 0)


def get_hls_target_dimensions(options=None):
    if options is None:
        options = get_hls_default_options()
    value = normalize_hls_resolution_value(options.get('resolution'))
    return {
        '1080p': (1920, 1080),
        '720p': (1280, 720),
        '480p': (854, 480),
    }.get(value, (0, 0))


def get_hls_preset_value(options=None):
    if options is None:
        options = get_hls_default_options()
    return normalize_hls_preset_value(options.get('preset'))


def map_encoder_preset(encoder, preset):
    if encoder == 'libx264':
        return {'fast': 'veryfast', 'normal': 'medium', 'quality': 'slow'}.get(preset, 'medium')
    if encoder == 'h264_nvenc':
        return {'fast': 'fast', 'normal': 'medium', 'quality': 'slow'}.get(preset, 'medium')
    if encoder == 'h264_qsv':
        return {'fast': 'faster', 'normal': 'medium', 'quality': 'slow'}.get(preset, 'medium')
    return ''


def candidate_h264_encoders():
    return ['h264_nvenc', 'h264_qsv', 'h264_vaapi', 'libx264']


def list_vaapi_devices():
    dri_root = '/dev/dri'
    if not os.path.isdir(dri_root):
        return []
    devices = []
    for name in sorted(os.listdir(dri_root)):
        if name.startswith('renderD'):
            devices.append(os.path.join(dri_root, name))
    return devices


def build_encoder_probe_command(ffmpeg_path, encoder, vaapi_device=None):
    base = [ffmpeg_path, '-hide_banner', '-loglevel', 'error']
    if encoder == 'h264_vaapi':
        return base + [
            '-vaapi_device', vaapi_device,
            '-f', 'lavfi',
            '-i', 'testsrc2=size=1280x720:rate=30',
            '-vf', 'format=nv12,hwupload',
            '-frames:v', '30',
            '-an',
            '-c:v', encoder,
            '-f', 'null', '-',
        ]
    cmd = base + [
        '-f', 'lavfi',
        '-i', 'testsrc2=size=1280x720:rate=30',
        '-frames:v', '30',
        '-an',
        '-c:v', encoder,
    ]
    if encoder == 'h264_nvenc':
        cmd.extend(['-preset', 'medium', '-pix_fmt', 'nv12'])
    cmd.extend(['-f', 'null', '-'])
    return cmd


def encoder_is_usable(ffmpeg_path, encoder, force=False, with_device=False):
    cache_key = encoder
    if force:
        encoder_probe_cache.pop(cache_key, None)
    cached = encoder_probe_cache.get(cache_key)
    if cached is not None:
        ok, device = cached
        return (ok, device) if with_device else ok
    if encoder == 'h264_vaapi':
        for device in list_vaapi_devices():
            cmd = build_encoder_probe_command(ffmpeg_path, encoder, vaapi_device=device)
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
            if proc.returncode == 0:
                encoder_probe_cache[cache_key] = (True, device)
                return (True, device) if with_device else True
            log_text = (proc.stderr or proc.stdout or '').strip()
            logger.warning('Encoder probe failed [%s][%s]: %s', encoder, device, log_text)
        encoder_probe_cache[cache_key] = (False, '')
        return (False, '') if with_device else False
    cmd = build_encoder_probe_command(ffmpeg_path, encoder)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
    ok = proc.returncode == 0
    device = ''
    encoder_probe_cache[cache_key] = (ok, device)
    if not ok:
        log_text = (proc.stderr or proc.stdout or '').strip()
        logger.warning('Encoder probe failed [%s]: %s', encoder, log_text)
    return (ok, device) if with_device else ok


def probe_channel_type(url, timeout=3):
    parsed = urllib.parse.urlparse(url)
    logger.info('[DIAG] probe start url=%s timeout=%s scheme=%s', url, timeout, parsed.scheme or '')
    if is_forward_channel_url(url):
        normalized_url = normalize_url_to_localhost_if_ddns(url)
        return {
            'ret': 'success',
            'msg': '포워드 채널로 처리합니다.',
            'data': {
                'detected_type': 'forward',
                'suggested_url': normalized_url,
            },
        }
    if parsed.scheme in ('http', 'https'):
        iproxy_type = detect_iproxy_api_type(url)
        if iproxy_type:
            logger.info('[DIAG] probe url_pattern_match url=%s detected_type=%s', url, iproxy_type)
            return {
                'ret': 'success',
                'msg': 'I-Proxy API URL 형식을 자동 인식했습니다.',
                'data': {
                    'detected_type': iproxy_type,
                    'diagnosed_by': 'url_pattern',
                    'ffmpeg_returncode': None,
                    'ffmpeg_timeout': False,
                },
            }
        result = ffmpeg_probe_input(url, timeout=max(5, timeout + 2))
        stderr = (result.get('stderr') or '').lower()
        detected_type = ''
        if result['success']:
            if ('format: hls' in stderr) or ('input #0, hls' in stderr) or ('input #0, applehttp' in stderr):
                detected_type = 'http_hls'
            else:
                detected_type = 'http_stream'
        logger.info(
            '[DIAG] probe http_result url=%s success=%s timeout=%s returncode=%s detected_type=%s',
            url,
            result.get('success'),
            result.get('timed_out'),
            result.get('returncode'),
            detected_type,
        )
        return {
            'ret': 'success' if detected_type else 'warning',
            'msg': '채널 진단 완료' if detected_type else 'ffmpeg로 HTTP 채널 타입을 확인하지 못했습니다.',
            'data': {
                'detected_type': detected_type,
                'diagnosed_by': 'ffmpeg',
                'ffmpeg_returncode': result['returncode'],
                'ffmpeg_timeout': result['timed_out'],
            },
        }
    if parsed.scheme not in ('udp', 'rtp'):
        logger.info('[DIAG] probe unsupported_scheme url=%s scheme=%s', url, parsed.scheme or '')
        return {
            'ret': 'warning',
            'msg': 'UDP/RTP 채널만 진단할 수 있습니다.',
            'data': {'detected_type': ''},
        }

    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        logger.info('[DIAG] probe invalid_host_port url=%s host=%s port=%s', url, host or '', port or '')
        return {
            'ret': 'danger',
            'msg': '주소에서 호스트 또는 포트를 확인할 수 없습니다.',
        }

    rtp_url = f'rtp://{host}:{port}'
    udp_url = f'udp://{host}:{port}?fifo_size=5000000&overrun_nonfatal=1'
    tests = [
        ('rtp', rtp_url),
        ('udp', udp_url),
    ]
    attempts = []
    for detected_type, candidate in tests:
        logger.info('[DIAG] probe transport_try url=%s candidate_type=%s candidate=%s', url, detected_type, candidate)
        result = ffmpeg_probe_input(candidate, timeout=max(5, timeout + 2))
        attempts.append({
            'type': detected_type,
            'returncode': result['returncode'],
            'timed_out': result['timed_out'],
            'success': result['success'],
        })
        logger.info(
            '[DIAG] probe transport_result url=%s candidate_type=%s success=%s timeout=%s returncode=%s',
            url,
            detected_type,
            result.get('success'),
            result.get('timed_out'),
            result.get('returncode'),
        )
        if result['success']:
            suggested_url = urllib.parse.urlunparse((detected_type, f'{host}:{port}', '', '', '', ''))
            logger.info('[DIAG] probe success url=%s detected_type=%s suggested_url=%s', url, detected_type, suggested_url)
            return {
                'ret': 'success',
                'msg': '채널 진단 완료',
                'data': {
                    'detected_type': detected_type,
                    'suggested_url': suggested_url,
                    'ffmpeg_attempts': attempts,
                },
            }
    logger.info('[DIAG] probe failed url=%s attempts=%s', url, json.dumps(attempts, ensure_ascii=False))
    return {
        'ret': 'warning',
        'msg': 'ffmpeg로 채널 타입을 확인하지 못했습니다.',
        'data': {
            'detected_type': '',
            'ffmpeg_attempts': attempts,
        },
    }


def stream_http_source(url, timeout=15, chunk_size=188 * 32):
    req = urllib.request.Request(url, headers={'User-Agent': ModelSetting.get('user_agent')})
    resp = urllib.request.urlopen(req, timeout=timeout)

    def generate():
        try:
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                resp.close()
            except Exception:
                pass

    return generate()


def stream_via_ffmpeg_copy(channel, chunk_size=188 * 32):
    ffmpeg_bin = get_ffmpeg_bin()
    source = channel['url']
    input_url = source
    if source.startswith('udp://'):
        input_url = f'{source}?fifo_size=5000000&overrun_nonfatal=1'

    cmd = [
        ffmpeg_bin,
        '-loglevel', 'info',
        '-fflags', '+genpts+discardcorrupt',
    ]
    if input_url.startswith('http://') or input_url.startswith('https://'):
        user_agent = (ModelSetting.get('user_agent') or '').strip()
        if user_agent:
            cmd.extend(['-user_agent', user_agent])
    cmd.extend([
        '-analyzeduration', '3000000',
        '-probesize', '3000000',
        '-i', input_url,
        '-ignore_unknown',
        '-map', '0:v?',
        '-map', '0:a?',
        '-c', 'copy',
        '-f', 'mpegts',
        '-',
    ])

    logger.info('[STREAM] ffmpeg_copy channel=%s command=%s', channel.get('name'), subprocess.list2cmdline(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    threading.Thread(target=_log_process_output, args=(proc, f'[FFMPEG-STREAM:{channel.get("id")}]'), daemon=True).start()

    def generate():
        try:
            while True:
                chunk = proc.stdout.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass

    return generate()


def udp_reader_loop(channel_id):
    channels = get_channel_map()
    channel = channels[channel_id]
    parsed = urllib.parse.urlparse(channel['url'])
    mcast_group = parsed.hostname
    mcast_port = parsed.port

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((mcast_group, mcast_port))
    mreq = struct.pack('4sL', socket.inet_aton(mcast_group), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.settimeout(10)
    is_rtp = None

    try:
        while True:
            try:
                data, _ = sock.recvfrom(65536)
            except socket.timeout:
                with STATE_LOCK:
                    if channel_id not in udp_clients:
                        break
                    if len(udp_clients[channel_id]) == 0 and time.time() - last_access_udp.get(channel_id, 0) > get_idle_timeout():
                        break
                continue

            with STATE_LOCK:
                if channel_id not in udp_clients:
                    break

            if is_rtp is None:
                is_rtp = channel.get('rtp', False) or (len(data) >= 12 and (data[0] >> 6) == 2 and data[0] != 0x47)

            if is_rtp:
                data = align_ts_packets(strip_rtp_header(data))
            else:
                data = align_ts_packets(data)
            if not data:
                continue

            with STATE_LOCK:
                queues = list(udp_clients.get(channel_id, []))
            for item in queues:
                try:
                    item.put_nowait(data)
                except queue.Full:
                    try:
                        item.get_nowait()
                        item.put_nowait(data)
                    except Exception:
                        pass
    finally:
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
            sock.close()
        except Exception:
            pass
        with STATE_LOCK:
            udp_clients.pop(channel_id, None)


def start_udp(channel_id):
    with STATE_LOCK:
        if channel_id in udp_clients:
            last_access_udp[channel_id] = time.time()
            return True
        udp_clients[channel_id] = []
        last_access_udp[channel_id] = time.time()
    thread = threading.Thread(target=udp_reader_loop, args=(channel_id,), daemon=True)
    thread.start()
    return True


def hls_proxy_loop(channel_id):
    channels = get_channel_map()
    channel = channels[channel_id]
    source_url = channel['url']
    base_url = source_url.rsplit('/', 1)[0] + '/'
    seen_segments = []

    while True:
        with STATE_LOCK:
            if channel_id not in hls_clients:
                break
            if len(hls_clients[channel_id]) == 0 and time.time() - last_access_hls_proxy.get(channel_id, 0) > get_idle_timeout():
                break

        m3u8_data = fetch_url(source_url)
        if not m3u8_data:
            time.sleep(2)
            continue

        segment_urls = []
        for line in m3u8_data.decode('utf-8', errors='replace').splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            if stripped.startswith('http'):
                segment_urls.append(stripped)
            else:
                segment_urls.append(base_url + stripped)

        for segment_url in [x for x in segment_urls if x not in seen_segments]:
            segment_data = fetch_url(segment_url)
            if not segment_data:
                continue
            seen_segments.append(segment_url)
            if len(seen_segments) > 20:
                seen_segments.pop(0)
            with STATE_LOCK:
                queues = list(hls_clients.get(channel_id, []))
            for item in queues:
                try:
                    item.put_nowait(segment_data)
                except queue.Full:
                    pass
        time.sleep(2)

    with STATE_LOCK:
        hls_clients.pop(channel_id, None)


def start_hls_proxy(channel_id):
    with STATE_LOCK:
        if channel_id in hls_clients:
            last_access_hls_proxy[channel_id] = time.time()
            return True
        hls_clients[channel_id] = []
        last_access_hls_proxy[channel_id] = time.time()
    thread = threading.Thread(target=hls_proxy_loop, args=(channel_id,), daemon=True)
    thread.start()
    return True


def get_proxied_m3u8(channel_id, req):
    channels = get_channel_map()
    channel = channels[channel_id]
    source_url = channel['url']
    base_url = source_url.rsplit('/', 1)[0] + '/'
    m3u8_data = fetch_url(source_url)
    if not m3u8_data:
        return None

    base = get_base_url(req)
    lines = []
    for line in m3u8_data.decode('utf-8', errors='replace').splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith('#'):
            segment_url = stripped if stripped.startswith('http') else base_url + stripped
            encoded = urllib.parse.quote(segment_url, safe='')
            lines.append(with_apikey(f'{base}/api/seg/{encoded}'))
        else:
            lines.append(line)
    return '\n'.join(lines) + '\n'


def cleanup_loop():
    while True:
        now = time.time()
        timeout = get_idle_timeout()
        with STATE_LOCK:
            stale = [key for key in list(processes_hls.keys()) if now - last_access_hls.get(key, 0) > timeout]
        for key in stale:
            stop_hls(key)
        time.sleep(10)


def make_channel_payload(channel, req):
    base = get_base_url(req)
    hls_url = with_request_apikey(f'{base}/api/hls/{channel["id"]}', req)
    stream_url = with_request_apikey(f'{base}/api/stream/{channel["id"]}', req)
    player_url = with_request_apikey(f'{base}/player/{channel["id"]}', req)
    if channel.get('type') == 'forward':
        hls_url = channel['url']
        stream_url = channel['url']
        player_url = channel['url']
    return {
        'id': channel['id'],
        'name': channel['name'],
        'url': channel['url'],
        'type': channel['type'],
        'epg_source': channel.get('epg_source', 'AUTO'),
        'epg_enabled': channel.get('epg_enabled', True),
        'hidden': channel['hidden'],
        'rtp': channel['rtp'],
        'source': channel['source'],
        'stream_url': stream_url,
        'hls_url': hls_url,
        'player_url': player_url,
    }


def build_playlist(req, mode='stream', include_all=True):
    lines = ['#EXTM3U']
    for channel in get_channels():
        if channel.get('hidden') and not include_all:
            continue
        payload = make_channel_payload(channel, req)
        url = payload['hls_url'] if mode == 'hls' else payload['stream_url']
        lines.append(f'#EXTINF:-1 tvg-name="{channel["name"]}",{channel["name"]}')
        lines.append(url)
    return '\n'.join(lines) + '\n'


def _escape_tvh_attr(value):
    return str(value or '').replace('&', '&amp;').replace('"', '&quot;')


def _escape_tvh_pipe_value(value):
    return str(value or '').replace('\\', '\\\\').replace('"', '\\"')


def build_playlist_tvh(req, mode='stream', include_all=True):
    lines = ['#EXTM3U']
    user_agent = ModelSetting.get('user_agent') or 'Mozilla/5.0'
    for idx, channel in enumerate(get_channels(), start=1):
        if channel.get('hidden') and not include_all:
            continue
        payload = make_channel_payload(channel, req)
        display_name = str(channel.get('name') or '').strip()
        if not display_name:
            continue
        url = payload['hls_url'] if mode == 'hls' else payload['stream_url']
        lines.append(
            '#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{tvg_name}" tvg-logo="" '
            'group-title="I-Proxy" tvg-chno="{chno}" tvh-chnum="{chno}",{display}'.format(
                tvg_id=_escape_tvh_attr(display_name),
                tvg_name=_escape_tvh_attr(display_name),
                chno=idx,
                display=display_name,
            )
        )
        lines.append(
            'pipe://ffmpeg -user_agent "{ua}" -loglevel quiet -i "{url}" '
            '-c copy -metadata service_provider=ff_iproxy -metadata service_name="{name}" '
            '-c:v copy -c:a aac -b:a 128k -f mpegts -tune zerolatency pipe:1'.format(
                ua=_escape_tvh_pipe_value(user_agent),
                url=_escape_tvh_pipe_value(url),
                name=_escape_tvh_pipe_value(display_name),
            )
        )
    return '\n'.join(lines) + '\n'


def get_epg_dir():
    path = get_plugin_data_dir() / 'epg'
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_epg_output_path():
    return get_epg_dir() / 'epg.xml'


def get_epg_debug_dir():
    path = get_epg_dir() / 'debug'
    path.mkdir(parents=True, exist_ok=True)
    return path


def append_epg_log(message):
    logger.info('[EPG] %s', message)


def append_epg_channel_log(channel_name, message):
    append_epg_log(f'[{channel_name}] {message}')


def save_epg_debug_html(source, channel_name, query, text):
    safe_source = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(source or 'unknown')).strip('_') or 'unknown'
    safe_channel = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(channel_name or 'unknown')).strip('_') or 'unknown'
    safe_query = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(query or 'query')).strip('_') or 'query'
    filename = f'{safe_source}_{safe_channel}_{safe_query}.html'
    path = get_epg_debug_dir() / filename
    try:
        path.write_text(text or '', encoding='utf-8', errors='replace')
        return str(path)
    except Exception:
        logger.exception('Failed to write EPG debug html')
        return ''


def reset_epg_log(message=None):
    if message:
        append_epg_log(message)


def read_epg_log_tail(max_lines=120):
    _ = max_lines
    return 'EPG/진단 로그는 기본 로그 파일에 기록됩니다. 로그 메뉴에서 확인하세요.'


def get_current_program_map():
    path = get_epg_output_path()
    if not path.exists():
        return {}
    try:
        root = ET.fromstring(path.read_text(encoding='utf-8', errors='replace'))
    except Exception:
        logger.exception('Failed to parse epg.xml')
        return {}

    now = datetime.now()
    current = {}
    for program in root.findall('programme'):
        channel_id = program.attrib.get('channel')
        start_text = program.attrib.get('start', '')
        stop_text = program.attrib.get('stop', '')
        if not channel_id or len(start_text) < 14 or len(stop_text) < 14:
            continue
        try:
            start = datetime.strptime(start_text[:14], '%Y%m%d%H%M%S')
            stop = datetime.strptime(stop_text[:14], '%Y%m%d%H%M%S')
        except Exception:
            continue
        if not (start <= now < stop):
            continue
        title_node = program.find('title')
        sub_title_node = program.find('sub-title')
        title = title_node.text.strip() if title_node is not None and title_node.text else ''
        sub_title = sub_title_node.text.strip() if sub_title_node is not None and sub_title_node.text else ''
        if title == '':
            continue
        current[channel_id] = f'{title} {sub_title}'.strip()
    return current


def load_epg_channel_map():
    path = Path(__file__).resolve().parent / 'epg_channel_map.json'
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        logger.exception('Failed to load epg_channel_map.json')
        return []


def load_epg_channel_map_full():
    path = Path(__file__).resolve().parent / 'epg_channel_map_full.json'
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        logger.exception('Failed to load epg_channel_map_full.json')
        return {}


def load_epg_service_overrides():
    path = Path(__file__).resolve().parent / 'epg_service_overrides.json'
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.exception('Failed to load epg_service_overrides.json')
        return {}


def load_epg_provider_channels(provider):
    data = load_epg_channel_map_full()
    section = data.get(str(provider or '').upper(), {})
    channels = section.get('CHANNELS', []) if isinstance(section, dict) else []
    return channels if isinstance(channels, list) else []


def normalize_channel_name_for_epg(name):
    text = str(name or '').strip().lower()
    text = text.replace('+', ' plus ')
    text = text.replace('&', 'and')
    return re.sub(r'[^0-9a-zA-Z가-힣]+', '', text)


def get_channel_aliases_for_epg(channel_name, source=None):
    aliases = [str(channel_name or '').strip()]
    alias_map = {}
    if source == 'NAVER':
        alias_map = NAVER_CHANNEL_ALIASES
    elif source == 'DAUM':
        alias_map = DAUM_CHANNEL_ALIASES
    for alias in alias_map.get(channel_name, []):
        alias = str(alias or '').strip()
        if alias and alias not in aliases:
            aliases.append(alias)
    return aliases


def resolve_epg_service(channel, source):
    channel_name = channel.get('name')
    aliases = get_channel_aliases_for_epg(channel.get('name'), source)
    candidates = [normalize_channel_name_for_epg(x) for x in aliases]
    candidates = [x for x in candidates if x]
    if not candidates:
        append_epg_channel_log(channel_name, f'{source} normalized candidate list is empty')
        return None
    if source in ('LG', 'SK', 'NAVER', 'DAUM'):
        overrides = load_epg_service_overrides()
        provider_overrides = overrides.get(source, {}) if isinstance(overrides, dict) else {}
        if isinstance(provider_overrides, dict):
            fixed = provider_overrides.get(channel.get('name'))
            if fixed:
                append_epg_channel_log(channel_name, f'{source} service id resolved from override: {fixed}')
                return fixed
        matches = []
        for row in load_epg_provider_channels(source):
            if not isinstance(row, dict):
                continue
            row_name = normalize_channel_name_for_epg(row.get('Name'))
            row_service = normalize_channel_name_for_epg(row.get('ServiceId'))
            if any(candidate and candidate in candidates for candidate in (row_name, row_service)):
                matches.append(row)
        if matches:
            selected = matches[0]
            if source == 'LG' and len(matches) > 1:
                exact = [row for row in matches if normalize_channel_name_for_epg(row.get('Name')) == candidates[0]]
                pool = exact or matches
                def lg_sort_key(row):
                    service = str(row.get('ServiceId') or '').strip()
                    try:
                        return int(service)
                    except Exception:
                        return -1
                selected = sorted(pool, key=lg_sort_key, reverse=True)[0]
                append_epg_channel_log(
                    channel_name,
                    f'{source} embedded map multiple matches={len(matches)} selected={selected.get("ServiceId")}',
                )
            service = selected.get('ServiceId')
            append_epg_channel_log(channel_name, f'{source} service id resolved from embedded map: {service}')
            return '' if service in (None, '') else str(service).strip()
        if source == 'DAUM':
            append_epg_channel_log(channel_name, f'{source} service id not found in embedded map; aliases={aliases}')
        elif source in ('LG', 'SK'):
            append_epg_channel_log(channel_name, f'{source} embedded map miss; fallback to legacy map')
    alias_keys = ['Name']
    service_key = 'ServiceId'
    if source == 'KT':
        alias_keys = ['KT Name', 'Name']
        service_key = 'KTCh'
    elif source == 'LG':
        alias_keys = ['LG Name', 'Name']
        service_key = 'LGCh'
    elif source == 'SK':
        alias_keys = ['SK Name', 'Name']
        service_key = 'SKCh'
    elif source == 'NAVER':
        alias_keys = ['Name', 'KT Name', 'LG Name', 'SK Name']
    for row in load_epg_channel_map():
        if not isinstance(row, dict):
            continue
        for key in alias_keys:
            candidate = normalize_channel_name_for_epg(row.get(key))
            if candidate and candidate in candidates:
                service = row.get(service_key)
                if service in (None, '') and service_key != 'ServiceId':
                    service = row.get('ServiceId')
                append_epg_channel_log(channel_name, f'{source} service id resolved from legacy map key={key} field={service_key}: {service}')
                return '' if service in (None, '') else str(service).strip()
    append_epg_channel_log(channel_name, f'{source} service id unresolved; aliases={get_channel_aliases_for_epg(channel.get("name"), source)}')
    return None


def make_epg_session():
    user_agent = (ModelSetting.get('epg_user_agent') or '').strip()
    referer = (ModelSetting.get('epg_referer') or '').strip()
    if curl_requests is not None:
        session = curl_requests.Session(impersonate='chrome')
    else:
        session = requests.Session()
    session.headers.update(EPG_HTTP_HEADERS)
    if user_agent:
        session.headers['User-Agent'] = user_agent
    if referer:
        session.headers['Referer'] = referer
    return session


def epg_request(session, method, url, params=None, data=None, headers=None, timeout=15):
    parsed = urllib.parse.urlparse(url)
    host = (parsed.netloc or '').lower()
    merged_headers = {}
    if 'naver.com' in host:
        merged_headers.update(EPG_NAVER_HEADERS)
    elif 'daum.net' in host:
        merged_headers.update(EPG_DAUM_HEADERS)
    if headers:
        merged_headers.update(headers)
    response = session.request(
        method,
        url,
        params=params,
        data=data,
        timeout=timeout,
        headers=merged_headers or None,
    )
    response.raise_for_status()
    return response


def parse_kt_programs(channel, session, days):
    if BeautifulSoup is None:
        raise RuntimeError('bs4 is required for KT EPG parsing')
    schedule_url = 'https://tv.kt.com/tv/channel/pSchedule.asp'
    service_id = str(channel.get('epg_service_id') or '').strip()
    if service_id == '':
        raise RuntimeError(f'KT service id not set for {channel["name"]}')
    programmes = []
    for offset in range(days):
        target = date.today() + timedelta(days=offset)
        resp = epg_request(
            session,
            'POST',
            schedule_url,
            data={
                'ch_type': '1',
                'view_type': '1',
                'service_ch_no': service_id,
                'seldate': target.strftime('%Y%m%d'),
            },
            headers={'Referer': 'https://tv.kt.com/'},
        )
        body = BeautifulSoup(urllib.parse.unquote(resp.text), 'html.parser', parse_only=SoupStrainer('tbody'))
        if not body:
            continue
        for row in body.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
            hours = cells[0].get_text(strip=True)
            for minute, program, category in zip(*[c.find_all('p') for c in cells[1:]]):
                start_time = datetime.strptime(
                    f'{target.isoformat()} {hours}:{minute.get_text(strip=True)}',
                    '%Y-%m-%d %H:%M',
                )
                title = program.get_text(' ', strip=True).replace('방송중 ', '').strip()
                cat = category.get_text(' ', strip=True)
                rating = ''
                for image in program.find_all('img', alt=True):
                    found = re.match(r'([\d,]+)', image['alt'])
                    if found:
                        rating = found.group(1)
                        break
                programmes.append({
                    'channel_id': channel['id'],
                    'title': title,
                    'sub_title': '',
                    'category': cat,
                    'episode': '',
                    'desc': '',
                    'rebroadcast': False,
                    'rating': rating,
                    'start': start_time,
                })
    return programmes


def parse_lg_programs(channel, session, days):
    service_id = str(channel.get('epg_service_id') or '').strip()
    if service_id == '':
        raise RuntimeError(f'LG service id not set for {channel["name"]}')
    fetch_days = min(max(1, int(days)), 5)
    title_regex = re.compile(r'\s?(?:\[.*?\])?(.*?)(?:\[(.*)\])?\s?(?:\(([\d,]+)회\))?\s?(<재>)?$')
    rating_map = {'0': 0, '1': 7, '2': 12, '3': 15, '4': 19}
    category_map = {
        '00': '영화',
        '02': '만화',
        '03': '드라마',
        '05': '스포츠',
        '06': '교육',
        '08': '연예/오락',
        '09': '공연/음악',
        '11': '다큐',
        '12': '뉴스/정보',
        '13': '라이프',
        '31': '기타',
    }
    programmes = []
    for offset in range(fetch_days):
        target = date.today() + timedelta(days=offset)
        resp = epg_request(
            session,
            'GET',
            'https://www.lguplus.com/uhdc/fo/prdv/chnlgid/v1/tv-schedule-list',
            params={
                'urcBrdCntrTvChnlId': service_id,
                'brdCntrTvChnlBrdDt': target.strftime('%Y%m%d'),
            },
        )
        payload = resp.json() or {}
        entries = payload.get('brdCntTvSchIDtoList', []) or []
        if not entries:
            continue
        for item in entries:
            title = str(item.get('brdPgmTitNm') or '').strip()
            if title == '':
                continue
            match = title_regex.match(title)
            parsed_title = match.group(1).strip() if match and match.group(1) else title
            sub_title = match.group(2).strip() if match and match.group(2) else ''
            episode = match.group(3).strip() if match and match.group(3) else ''
            rebroadcast = bool(match and match.group(4))
            start_raw = f'{item.get("brdCntrTvChnlBrdDt", "")}{item.get("epgStrtTme", "")}'
            if len(start_raw) != 16:
                continue
            category = category_map.get(str(item.get('urcBrdCntrTvSchdGnreCd') or '').strip(), '')
            rating = rating_map.get(str(item.get('brdWtchAgeGrdCd') or '').strip(), 0)
            programmes.append({
                'channel_id': channel['id'],
                'title': parsed_title,
                'sub_title': sub_title,
                'category': category,
                'episode': episode,
                'desc': str(item.get('brdPgmDscr') or '').strip(),
                'rebroadcast': rebroadcast,
                'rating': rating,
                'start': datetime.strptime(start_raw, '%Y%m%d%H:%M:%S'),
            })
    return programmes


def parse_skb_programs(channel, session, days):
    service_id = str(channel.get('epg_service_id') or '').strip()
    if service_id == '':
        raise RuntimeError(f'SK service id not set for {channel["name"]}')
    fetch_days = min(max(1, int(days)), 3)
    append_epg_channel_log(channel['name'], f'SK service id in use: {service_id}')
    resp = epg_request(
        session,
        'GET',
        'https://www.bworld.co.kr/myb/core-prod/product/btv-channel/week-frmt-list',
        params={'idSvc': service_id, 'stdDt': date.today().strftime('%Y%m%d'), 'gubun': 'week'},
        headers={'Referer': 'https://www.bworld.co.kr/'},
    )
    info_list = (((resp.json() or {}).get('result') or {}).get('chnlFrmtInfoList') or [])
    if not isinstance(info_list, list):
        raise RuntimeError(f'SK unexpected response for {channel["name"]}')
    genre_code = {
        '1': '드라마',
        '2': '영화',
        '4': '만화',
        '8': '스포츠',
        '9': '교육',
        '11': '홈쇼핑',
        '13': '예능',
        '14': '시사/다큐',
        '15': '음악',
        '16': '라이프',
        '17': '교양',
        '18': '뉴스',
    }
    title_regex = re.compile(r'^(.*?)(\(([\d,]+)회\))?(<(.*)>)?(\((재)\))?$')
    programmes = []
    for offset in range(fetch_days):
        target = date.today() + timedelta(days=offset)
        target_ymd = target.strftime('%Y%m%d')
        day_count = 0
        for info in info_list:
            if str(info.get('eventDt') or '') != target_ymd:
                continue
            title = str(info.get('nmTitle') or '').strip()
            sub_title = ''
            episode = ''
            rebroadcast = False
            match = title_regex.match(title)
            if match:
                title = (match.group(1) or '').strip()
                sub_title = (match.group(5) or '').strip()
                episode = (match.group(3) or '').strip()
                rebroadcast = bool(match.group(7))
            start_raw = str(info.get('dtEventStart') or '').strip()
            end_raw = str(info.get('dtEventEnd') or '').strip()
            if len(start_raw) != 14:
                continue
            programmes.append({
                'channel_id': channel['id'],
                'title': title,
                'sub_title': sub_title,
                'category': genre_code.get(str(info.get('cdGenre') or '').strip(), ''),
                'episode': episode,
                'desc': str(info.get('nmSynop') or '').strip(),
                'rebroadcast': rebroadcast,
                'rating': str(info.get('cdRating') or '').strip(),
                'start': datetime.strptime(start_raw, '%Y%m%d%H%M%S'),
                'end': datetime.strptime(end_raw, '%Y%m%d%H%M%S') if len(end_raw) == 14 else None,
            })
            day_count += 1
        append_epg_channel_log(channel['name'], f'SK programmes for {target.isoformat()}: {day_count}')
    return programmes


def parse_naver_programs(channel, session, days):
    if BeautifulSoup is None:
        raise RuntimeError('bs4 is required for NAVER EPG parsing')
    channel_name = channel.get('name')

    def parse_search_date(text, fallback_year):
        match = re.search(r'(\d{2})\.(\d{2})\.', str(text or ''))
        if match:
            return date(fallback_year, int(match.group(1)), int(match.group(2)))
        match = re.search(r'(\d{2})월\s*(\d{2})일', str(text or ''))
        if match:
            return date(fallback_year, int(match.group(1)), int(match.group(2)))
        return None

    def parse_search_programmes_from_html(html, target):
        section = BeautifulSoup(html, 'html.parser', parse_only=SoupStrainer('section', {'class': re.compile(r'cs_tvtime')}))
        root = section.find('section') if hasattr(section, 'find') else None
        if root is None and getattr(section, 'name', None) == 'section':
            root = section
        if root is None:
            return []

        date_links = root.select('ul._date_list a')
        target_col = None
        for idx, link in enumerate(date_links, start=1):
            label_date = parse_search_date(link.get_text(' ', strip=True), target.year)
            if label_date == target:
                target_col = idx
                break
            href = link.get('href', '')
            query = urllib.parse.parse_qs(urllib.parse.urlparse(href).query).get('query', [''])[0]
            query_date = parse_search_date(urllib.parse.unquote_plus(query), target.year)
            if query_date == target:
                target_col = idx
                break

        if target_col is None:
            current = root.select_one('ul._date_list em.today')
            if current is not None:
                parent = current.find_parent('a')
                if parent is not None:
                    for idx, link in enumerate(date_links, start=1):
                        if link == parent:
                            target_col = idx
                            break

        if target_col is None:
            return []

        programmes = []
        for row in root.select('ul.program_list > li.item'):
            hour_node = row.select_one('.time_box span')
            if hour_node is None:
                continue
            hour_match = re.search(r'(\d{1,2})', hour_node.get_text(' ', strip=True))
            if hour_match is None:
                continue
            hour_value = int(hour_match.group(1))
            column = row.select_one(f'.ind_program.col{target_col}')
            if column is None:
                continue
            for inner in column.select('.inner'):
                minute_node = inner.select_one('.time_min')
                title_node = inner.select_one('.pr_title')
                if minute_node is None or title_node is None:
                    continue
                minute_match = re.search(r'(\d{1,2})', minute_node.get_text(' ', strip=True))
                if minute_match is None:
                    continue
                minute_value = int(minute_match.group(1))
                start_time = datetime.combine(target, datetime.min.time()).replace(hour=hour_value, minute=minute_value)
                sub_title_node = inner.select_one('.pr_sub_title')
                desc_text = ''
                if sub_title_node is not None:
                    desc_text = sub_title_node.get_text(' ', strip=True)
                programmes.append({
                    'channel_id': channel['id'],
                    'title': title_node.get_text(' ', strip=True),
                    'sub_title': '',
                    'category': '',
                    'episode': '',
                    'desc': desc_text,
                    'rebroadcast': inner.select_one('.s_label.re') is not None or '재' in inner.get_text(' ', strip=True),
                    'rating': '',
                    'start': start_time,
                })
        return programmes

    def fetch_search_programmes_for_date(target):
        base_names = [channel['name']]
        for alias in NAVER_CHANNEL_ALIASES.get(channel['name'], []):
            if alias not in base_names:
                base_names.append(alias)

        queries = []
        for base_name in base_names:
            queries.extend([
                f'{base_name} 편성표',
                f'{base_name} 오늘 편성표',
                f'{base_name} {target.strftime("%m월 %d일")} 편성표',
                f'{base_name}{target.strftime("%m월 %d일")} 편성표',
            ])

        seen = set()
        for query in queries:
            if query in seen:
                continue
            seen.add(query)
            try:
                append_epg_channel_log(channel_name, f'NAVER search query: {query}')
                resp = epg_request(
                    session,
                    'GET',
                    'https://search.naver.com/search.naver',
                    params={
                        'where': 'nexearch',
                        'sm': 'tab_etc',
                        'query': query,
                    },
                )
            except Exception as exception:
                append_epg_channel_log(channel_name, f'NAVER search query failed: {query} / {str(exception)}')
                continue
            parsed = parse_search_programmes_from_html(resp.text, target)
            if parsed:
                append_epg_channel_log(channel_name, f'NAVER search query matched {len(parsed)} programmes for {target.isoformat()}: {query}')
                return parsed
            append_epg_channel_log(channel_name, f'NAVER search query returned 0 programmes for {target.isoformat()}: {query}')
        return []

    service_id = str(channel.get('epg_service_id') or '').strip()
    append_epg_channel_log(channel_name, f'NAVER service id in use: {service_id or "(none)"}')
    programmes = []
    for offset in range(days):
        target = date.today() + timedelta(days=offset)
        day_programmes = []
        if service_id != '':
            try:
                append_epg_channel_log(channel_name, f'NAVER API request date={target.isoformat()} service_id={service_id}')
                resp = epg_request(
                    session,
                    'GET',
                    'https://m.search.naver.com/p/csearch/content/nqapirender.nhn',
                    params={
                        'key': 'SingleChannelDailySchedule',
                        'where': 'm',
                        'pkid': '66',
                        'u1': service_id,
                        'u2': target.strftime('%Y%m%d'),
                    },
                )
                data = resp.json()
                append_epg_channel_log(channel_name, f'NAVER API status for {target.isoformat()}: {data.get("statusCode")}')
                if str(data.get('statusCode', '')).lower() == 'success':
                    soup = BeautifulSoup(''.join(data.get('dataHtml', [])), 'html.parser')
                    for row in soup.find_all('li', {'class': 'list'}):
                        cells = row.find_all('div')
                        if len(cells) < 5:
                            continue
                        start_text = cells[1].get_text(strip=True)
                        if ':' not in start_text:
                            continue
                        start_time = datetime.strptime(f'{target.isoformat()} {start_text}', '%Y-%m-%d %H:%M')
                        sub_title = cells[5].get_text(strip=True) if len(cells) > 5 else ''
                        day_programmes.append({
                            'channel_id': channel['id'],
                            'title': cells[4].get_text(' ', strip=True),
                            'sub_title': sub_title,
                            'category': '',
                            'episode': '',
                            'desc': '',
                            'rebroadcast': row.find('span', {'class': 're'}) is not None,
                            'rating': '',
                            'start': start_time,
                        })
                append_epg_channel_log(channel_name, f'NAVER API programmes for {target.isoformat()}: {len(day_programmes)}')
            except Exception:
                append_epg_channel_log(channel_name, f'NAVER API fallback to search page for {target.isoformat()}')
        if not day_programmes:
            day_programmes = fetch_search_programmes_for_date(target)
        append_epg_channel_log(channel_name, f'NAVER final programmes for {target.isoformat()}: {len(day_programmes)}')
        programmes.extend(day_programmes)
    return programmes


def parse_daum_programs(channel, session, days):
    if BeautifulSoup is None:
        raise RuntimeError('bs4 is required for DAUM EPG parsing')
    channel_name = channel.get('name')
    service_id = str(channel.get('epg_service_id') or '').strip()
    if service_id == '':
        raise RuntimeError(f'DAUM service id not set for {channel["name"]}')
    append_epg_channel_log(channel_name, f'DAUM service id in use: {service_id}')

    query_candidates = [service_id]
    for name in get_channel_aliases_for_epg(channel.get('name'), 'DAUM'):
        if name and name not in query_candidates:
            query_candidates.append(name)
    append_epg_channel_log(channel_name, f'DAUM query candidates: {query_candidates}')

    soup = None
    matched_query = None
    title_regex = re.compile(DAUM_TITLE_REGEX)
    for query in query_candidates:
        try:
            append_epg_channel_log(channel_name, f'DAUM request query: {query}')
            resp = epg_request(session, 'GET', DAUM_SEARCH_URL.format(query), timeout=20)
            append_epg_channel_log(
                channel_name,
                'DAUM response '
                f'status={resp.status_code} bytes={len(resp.text)} '
                f'content_type={resp.headers.get("Content-Type", "")} '
                f'content_encoding={resp.headers.get("Content-Encoding", "")} '
                f'final_url={resp.url}',
            )
        except Exception as exception:
            append_epg_channel_log(channel_name, f'DAUM request failed: {query} / {str(exception)}')
            continue
        candidate_soup = BeautifulSoup(resp.text, 'html.parser')
        marker_count = len(candidate_soup.find_all(attrs={'disp-attr': 'B3T'}))
        append_epg_channel_log(channel_name, f'DAUM B3T marker count for {query}: {marker_count}')
        if marker_count > 0:
            soup = candidate_soup
            matched_query = query
            append_epg_channel_log(channel_name, f'DAUM request matched query: {query}')
            break
        title_node = candidate_soup.select_one('title')
        title_text = title_node.get_text(' ', strip=True) if title_node is not None else ''
        preview = re.sub(r'\s+', ' ', (candidate_soup.get_text(' ', strip=True) or ''))[:200]
        debug_path = save_epg_debug_html('DAUM', channel_name, query, resp.text)
        append_epg_channel_log(
            channel_name,
            f'DAUM request had no B3T marker: {query} / title={title_text} / preview={preview} / debug={debug_path}',
        )
    if soup is None:
        raise RuntimeError(f'DAUM EPG data not found for {channel["name"]}')

    day_nodes = soup.select('div[class="tbl_head head_type2"] > span > span[class="date"]')
    if not day_nodes:
        raise RuntimeError(f'DAUM date headers not found for {channel["name"]}')
    append_epg_channel_log(channel_name, f'DAUM date headers found: {len(day_nodes)} using query={matched_query}')

    current = datetime.now()
    base_date = datetime.strptime(day_nodes[0].get_text(strip=True), '%m.%d').replace(year=current.year)
    if (base_date - current).days > 0:
        base_date = base_date.replace(year=base_date.year - 1)

    programmes = []
    max_days = min(days, len(day_nodes), 7)
    for day_index in range(max_days):
        target_date = (base_date + timedelta(days=day_index)).date().isoformat()
        hours = soup.select(f'[id="tvProgramListWrap"] > table > tbody > tr > td:nth-of-type({day_index + 1})')
        append_epg_channel_log(channel_name, f'DAUM hour cell count for {target_date}: {len(hours)}')
        if len(hours) != 24:
            append_epg_channel_log(channel_name, f'DAUM day {target_date} expected 24 hour cells but got {len(hours)}')
            continue
        day_count = 0
        for hour_index, hour in enumerate(hours):
            for dl in hour.select('dl'):
                minute_nodes = dl.select('dt')
                if not minute_nodes:
                    continue
                minute_text = minute_nodes[0].get_text(strip=True)
                if minute_text == '' or minute_text.isdigit() is False:
                    continue
                minute_value = int(minute_text)
                start_time = base_date + timedelta(days=day_index, hours=hour_index, minutes=minute_value)
                title = ''
                rebroadcast = False
                rating = ''
                extras = []
                for atag in dl.select('dd > a'):
                    title = atag.get_text(' ', strip=True)
                for span in dl.select('dd > span'):
                    class_val = ' '.join(span.get('class', []))
                    if class_val == '':
                        title = span.get_text(' ', strip=True)
                    elif 'ico_re' in class_val:
                        rebroadcast = True
                    elif 'ico_rate' in class_val:
                        rating_match = re.search(r'(\d+)', class_val)
                        if rating_match:
                            rating = rating_match.group(1)
                    else:
                        extras.append(span.get_text(' ', strip=True))
                if title == '':
                    continue
                sub_title = ''
                episode = ''
                matched = title_regex.search(title)
                if matched:
                    title = (matched.group('title') or '').strip()
                    if matched.group('part'):
                        title = f'{title} {matched.group("part")}부'.strip()
                    episode = matched.group('epnum') or ''
                    sub_title = (matched.group('subname2') or matched.group('subname1') or '').strip()
                programmes.append({
                    'channel_id': channel['id'],
                    'title': title,
                    'sub_title': sub_title,
                    'category': '',
                    'episode': episode,
                    'desc': ' '.join([x for x in extras if x]),
                    'rebroadcast': rebroadcast,
                    'rating': rating,
                    'start': start_time,
                })
                day_count += 1
        append_epg_channel_log(channel_name, f'DAUM programmes for {target_date}: {day_count}')
    if matched_query and matched_query != service_id:
        append_epg_channel_log(channel_name, f'DAUM matched alternate query: {matched_query}')
    return programmes


def sort_and_fill_programmes(programmes, days):
    grouped = {}
    for item in programmes:
        grouped.setdefault(item['channel_id'], []).append(item)
    end_cap = datetime.combine(date.today() + timedelta(days=days + 1), datetime.min.time())
    for channel_id, items in grouped.items():
        items.sort(key=lambda x: x['start'])
        for index, item in enumerate(items):
            next_start = items[index + 1]['start'] if index + 1 < len(items) else end_cap
            item['stop'] = next_start
    return grouped


def build_xmltv(channels, programmes_by_channel):
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<!DOCTYPE tv SYSTEM "xmltv.dtd">', '<tv generator-info-name="ff_iproxy">']
    for channel in channels:
        lines.append(f'  <channel id="{xml_escape(channel["id"])}">')
        lines.append(f'    <display-name>{xml_escape(channel["name"])}</display-name>')
        lines.append('  </channel>')
    for channel in channels:
        for program in programmes_by_channel.get(channel['id'], []):
            start = program['start'].strftime('%Y%m%d%H%M%S +0900')
            stop = program['stop'].strftime('%Y%m%d%H%M%S +0900')
            lines.append(f'  <programme start="{start}" stop="{stop}" channel="{xml_escape(channel["id"])}">')
            lines.append(f'    <title>{xml_escape(program["title"] or "")}</title>')
            if program.get('sub_title'):
                lines.append(f'    <sub-title>{xml_escape(program["sub_title"])}</sub-title>')
            if program.get('desc'):
                lines.append(f'    <desc>{xml_escape(program["desc"])}</desc>')
            if program.get('category'):
                lines.append(f'    <category>{xml_escape(program["category"])}</category>')
            if program.get('episode'):
                lines.append(f'    <episode-num system="onscreen">{xml_escape(program["episode"])}</episode-num>')
            if program.get('rebroadcast'):
                lines.append('    <previously-shown />')
            if program.get('rating'):
                lines.append('    <rating system="KMRB">')
                lines.append(f'      <value>{xml_escape(str(program["rating"]))}</value>')
                lines.append('    </rating>')
            lines.append('  </programme>')
    lines.append('</tv>')
    return '\n'.join(lines) + '\n'


def generate_epg_xml():
    channels = [channel for channel in get_channels() if not channel.get('hidden') and channel.get('epg_enabled', True)]
    days = max(1, min(7, int(ModelSetting.get('epg_days') or '2')))
    session = make_epg_session()
    reset_epg_log(f'EPG generation started for {len(channels)} channels')
    programmes = []
    failed_channels = []
    fetchers = {
        'KT': parse_kt_programs,
        'LG': parse_lg_programs,
        'SK': parse_skb_programs,
        'NAVER': parse_naver_programs,
        'DAUM': parse_daum_programs,
    }
    for channel in channels:
        selected = normalize_epg_source(channel.get('epg_source'))
        tried = EPG_AUTO_PRIORITY if selected == 'AUTO' else [selected]
        success = False
        last_reason = ''
        append_epg_channel_log(channel['name'], f'EPG start selected={selected} tried={tried}')
        for source in tried:
            fetcher = fetchers.get(source)
            if fetcher is None:
                continue
            try:
                append_epg_log(f'[{channel["name"]}] trying {source}')
                channel['epg_service_id'] = resolve_epg_service(channel, source)
                result = fetcher(channel, session, days)
                if result:
                    programmes.extend(result)
                    append_epg_log(f'[{channel["name"]}] {source} success ({len(result)} programmes)')
                    success = True
                    break
                append_epg_log(f'[{channel["name"]}] {source} returned no programmes')
                last_reason = f'{source}: returned no programmes'
            except Exception as exception:
                append_epg_log(f'[{channel["name"]}] {source} failed: {str(exception)}')
                last_reason = f'{source}: {str(exception)}'
        if not success:
            append_epg_log(f'[{channel["name"]}] no EPG data')
            failed_channels.append({'name': channel['name'], 'reason': last_reason or 'no EPG data'})
    xml_text = build_xmltv(channels, sort_and_fill_programmes(programmes, days))
    output = get_epg_output_path()
    output.write_text(xml_text, encoding='utf-8')
    ModelSetting.set('epg_last_generated', time.strftime('%Y-%m-%d %H:%M:%S'))
    if failed_channels:
        append_epg_log(f'Failed channels ({len(failed_channels)})')
        for item in failed_channels:
            if item.get('reason'):
                append_epg_log(f'- {item["name"]}: {item["reason"]}')
            else:
                append_epg_log(f'- {item["name"]}')
    else:
        append_epg_log('Failed channels (0)')
    append_epg_log(f'EPG generation finished: {output}')
    return output


def sync_epg_scheduler_job(enabled_override=None, schedule_override=None):
    if enabled_override is not None:
        ModelSetting.set('epg_auto_start', 'True' if parse_bool(enabled_override) else 'False')
    if schedule_override is not None:
        ModelSetting.set('epg_schedule', str(schedule_override or '').strip())
    job_id = f'{package_name}_epg_generate'
    enabled = ModelSetting.get_bool('epg_auto_start')
    schedule = (ModelSetting.get('epg_schedule') or '').strip()
    if F.scheduler.is_include(job_id):
        F.scheduler.remove_job(job_id)
    if not enabled or schedule == '':
        return {'ret': 'success', 'msg': 'EPG scheduler disabled'}
    job = Job(package_name, job_id, schedule, generate_epg_xml, 'IPTV proxy EPG generate')
    F.scheduler.add_job_instance(job)
    return {'ret': 'success', 'msg': 'EPG scheduler updated'}


class Logic(PluginModuleBase):
    db_default = {
        'ffmpeg_path': 'ffmpeg',
        'idle_timeout': '30',
        'hls_time': '2',
        'hls_list_size': '6',
        'hls_codec_mode': 'original',
        'hls_target_resolution': 'original',
        'hls_preset': 'normal',
        'hls_h264_encoder': 'libx264',
        'hls_vaapi_device': '',
        'alive_yaml_mode': 'stream',
        'include_hidden_channels': 'True',
        'manual_channels': '[]',
        'manual_channels_source_path': get_default_manual_channels_source_path(),
        'line_channels': '',
        'user_agent': 'Mozilla/5.0',
        'epg_auto_start': 'False',
        'epg_schedule': '360',
        'epg_days': '2',
        'epg_user_agent': EPG_HTTP_HEADERS['User-Agent'],
        'epg_referer': '',
        'epg_last_generated': '',
    }

    def __init__(self, PM):
        super().__init__(PM, None)
        self.name = 'setting'
        ensure_cleanup_thread()
        self.sync_epg_scheduler()

    def epg_scheduler_job_id(self):
        return f'{package_name}_epg_generate'

    def sync_epg_scheduler(self, enabled_override=None, schedule_override=None):
        return sync_epg_scheduler_job(enabled_override, schedule_override)

    def scheduler_function(self):
        try:
            generate_epg_xml()
        except Exception as exception:
            append_epg_log(f'Scheduler failed: {str(exception)}')
            logger.error(traceback.format_exc())

    def process_menu(self, sub, req):
        arg = ModelSetting.to_dict()
        base = get_base_url(req)
        arg['package_name'] = package_name
        arg['stream_playlist_url'] = with_apikey(f'{base}/api/playlist.m3u?mode=stream')
        arg['hls_playlist_url'] = with_apikey(f'{base}/api/playlist.m3u?mode=hls')
        arg['stream_tvh_playlist_url'] = with_apikey(f'{base}/api/playlist_tvh.m3u?mode=stream')
        arg['hls_tvh_playlist_url'] = with_apikey(f'{base}/api/playlist_tvh.m3u?mode=hls')
        arg['epg_xml_url'] = with_apikey(f'{base}/api/epg.xml')
        arg['channel_count'] = len(get_channels())
        arg['epg_source_options'] = EPG_SOURCE_OPTIONS
        arg['epg_schedule_active'] = str(F.scheduler.is_include(self.epg_scheduler_job_id()))
        arg['epg_log_tail'] = read_epg_log_tail()
        arg['epg_last_generated'] = ModelSetting.get('epg_last_generated')
        arg['default_channels_json'] = '[]'
        arg['bundled_channels_raw'] = get_bundled_channels_raw()
        arg['manual_channels_source_path_default'] = get_default_manual_channels_source_path()
        arg['manual_channels_source_path'] = get_manual_channels_source_path()
        arg['system_ddns'] = (SystemModelSetting.get('ddns') or '').strip()
        try:
            arg['manual_channels'] = get_manual_channels_raw()
            arg['manual_channels_json'] = json.dumps(parse_manual_channels(arg['manual_channels']), ensure_ascii=False)
        except Exception:
            arg['manual_channels'] = '[]'
            arg['manual_channels_json'] = '[]'
        if sub == 'list':
            current_programs = get_current_program_map()
            arg['channels'] = []
            for item in get_channels():
                payload = make_channel_payload(item, req)
                payload['current_program'] = current_programs.get(item['id'], '')
                arg['channels'].append(payload)
        return render_template(f'{package_name}_{sub}.html', arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        _ = (arg3, req)
        try:
            if command == 'ffmpeg_version':
                return jsonify(self._ffmpeg_version())
            if command == 'transcode_capabilities':
                return jsonify(self._transcode_capabilities())
            if command == 'test_transcode_encoder':
                return jsonify(self._test_transcode_encoder())
            if command == 'sync_alive_yaml':
                result = update_alive_fix_url(req)
                return jsonify({'ret': 'success', 'msg': f'alive.yaml에 {result["count"]}개 채널을 반영했습니다.'})
            if command == 'generate_epg':
                output = generate_epg_xml()
                return jsonify({'ret': 'success', 'msg': 'EPG generated', 'title': 'EPG 생성 완료', 'modal': str(output)})
            if command == 'sync_epg_schedule':
                return jsonify(self.sync_epg_scheduler(arg1, arg2))
            return jsonify({'ret': 'warning', 'msg': f'Unknown command: {command}'})
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def process_ajax(self, sub, req):
        try:
            if sub == 'ffmpeg_version':
                return jsonify(self._ffmpeg_version())
            if sub == 'transcode_capabilities':
                return jsonify(self._transcode_capabilities())
            if sub == 'test_transcode_encoder':
                return jsonify(self._test_transcode_encoder())
            if sub == 'save_settings':
                return jsonify(self._save_settings(req))
            if sub == 'import_manual_channels':
                source_value = req.form.get('source_path', req.form.get('raw', ''))
                imported = import_channels_from_source(source_value)
                return jsonify({
                    'ret': 'success',
                    'msg': f'{len(imported["channels"])}개 채널을 가져왔습니다.',
                    'format': imported['format'],
                    'source': imported.get('source', ''),
                    'channels': imported['channels'],
                })
            if sub == 'channel_list':
                return jsonify({'list': [make_channel_payload(item, req) for item in get_channels()]})
            if sub == 'player_event':
                mode = (req.form.get('mode') or '').strip()
                channel_name = (req.form.get('name') or '').strip()
                channel_url = (req.form.get('url') or '').strip()
                action = (req.form.get('action') or 'click').strip()
                logger.info('[PLAYER] action=%s mode=%s channel=%s url=%s', action, mode, channel_name, channel_url)
                return jsonify({'ret': 'success'})
            if sub == 'diagnose_manual_channel':
                channel_id = req.form.get('id', '').strip()
                channel_url = req.form.get('url', '').strip()
                if not channel_url:
                    return jsonify({'ret': 'warning', 'msg': '채널을 찾을 수 없습니다.'})
                result = probe_channel_type(channel_url)
                detected_type = ((result.get('data') or {}).get('detected_type') or '').strip()
                if channel_id and detected_type:
                    update_manual_channel_type(channel_id, detected_type, channel_url)
                return jsonify(result)
            if sub == 'epg_status':
                return jsonify({
                    'ret': 'success',
                    'data': {
                        'last_generated': ModelSetting.get('epg_last_generated'),
                        'log_tail': read_epg_log_tail(),
                        'schedule_active': F.scheduler.is_include(self.epg_scheduler_job_id()),
                    },
                })
            return jsonify({'ret': 'warning', 'msg': f'Unknown ajax: {sub}'})
        except Exception as e:
            logger.error('Exception:%s', e)
            logger.error(traceback.format_exc())
            return jsonify({'ret': 'danger', 'msg': str(e)})

    def _ffmpeg_version(self):
        ffmpeg_path = ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}
        proc = subprocess.run([ffmpeg_path, '-version'], capture_output=True, text=True, timeout=10, check=False)
        if proc.returncode != 0:
            return {'ret': 'danger', 'msg': proc.stderr.strip() or 'ffmpeg check failed'}
        first_lines = '\n'.join(proc.stdout.strip().splitlines()[:12])
        return {'ret': 'success', 'title': 'ffmpeg version', 'modal': first_lines}

    def _transcode_capabilities(self):
        ffmpeg_path = ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}
        encoders = subprocess.run([ffmpeg_path, '-hide_banner', '-encoders'], capture_output=True, text=True, timeout=20, check=False)
        hwaccels = subprocess.run([ffmpeg_path, '-hide_banner', '-hwaccels'], capture_output=True, text=True, timeout=20, check=False)
        if encoders.returncode != 0:
            return {'ret': 'danger', 'msg': encoders.stderr.strip() or 'encoder check failed'}

        encoder_text = encoders.stdout
        hwaccel_text = hwaccels.stdout if hwaccels.returncode == 0 else (hwaccels.stderr or '')
        known = [
            ('libx264', 'H.264 CPU'),
            ('libx265', 'H.265 CPU'),
            ('h264_nvenc', 'H.264 NVIDIA NVENC'),
            ('hevc_nvenc', 'H.265 NVIDIA NVENC'),
            ('h264_qsv', 'H.264 Intel QSV'),
            ('hevc_qsv', 'H.265 Intel QSV'),
            ('h264_vaapi', 'H.264 VAAPI'),
            ('hevc_vaapi', 'H.265 VAAPI'),
        ]
        lines = ['[Detected encoders]']
        found = False
        for token, label in known:
            if token in encoder_text:
                lines.append(f'- {label}: available')
                found = True
        if not found:
            lines.append('- No known video encoder detected')
        lines.extend(['', '[hwaccels]', hwaccel_text.strip() or 'No hwaccels output', '', '[matching encoder lines]'])
        excerpt = [line for line in encoder_text.splitlines() if any(token in line for token, _ in known)]
        lines.append('\n'.join(excerpt) if excerpt else 'No matching encoder lines')
        return {'ret': 'success', 'title': 'Available encoders', 'modal': '\n'.join(lines)}

    def _test_transcode_encoder(self):
        ffmpeg_path = ModelSetting.get('ffmpeg_path')
        if not ffmpeg_path:
            return {'ret': 'danger', 'msg': 'ffmpeg_path is empty'}

        tested = []
        selected = None
        selected_device = ''
        for encoder in candidate_h264_encoders():
            ok, device = encoder_is_usable(ffmpeg_path, encoder, force=True, with_device=True)
            suffix = f' ({device})' if device else ''
            tested.append(f'- {encoder}: {"OK" if ok else "FAIL"}{suffix}')
            if ok and selected is None:
                selected = encoder
                selected_device = device or ''

        if selected is None:
            return {
                'ret': 'danger',
                'title': 'Encoder test',
                'modal': '\n'.join(['[codec]', 'h264', '', '[results]', *tested]),
                'msg': '사용 가능한 h264 인코더를 찾지 못했습니다',
            }

        ModelSetting.set('hls_h264_encoder', selected)
        ModelSetting.set('hls_vaapi_device', selected_device)
        return {
            'ret': 'success',
            'title': 'Encoder test',
            'modal': '\n'.join(['[codec]', 'h264', '', '[saved]', selected, selected_device or '', '', '[results]', *tested]),
            'data': {'selected_encoder': selected, 'selected_device': selected_device},
            'msg': '테스트 결과가 저장되었습니다',
        }

    def _save_settings(self, req):
        keys = [
            'ffmpeg_path',
            'idle_timeout',
            'hls_time',
            'hls_list_size',
            'hls_codec_mode',
            'hls_target_resolution',
            'hls_preset',
            'user_agent',
            'alive_yaml_mode',
            'include_hidden_channels',
            'manual_channels',
            'manual_channels_source_path',
        ]
        for key in keys:
            if key not in req.form:
                continue
            value = req.form.get(key, '')
            if key == 'include_hidden_channels':
                value = 'True' if parse_bool(value) else 'False'
            ModelSetting.set(key, value)
        try:
            alive_result = update_alive_fix_url(req)
            return {
                'ret': 'success',
                'msg': f'설정을 저장했습니다. alive.yaml에 {alive_result["count"]}개 채널을 반영했습니다.',
            }
        except Exception as exception:
            logger.exception('Failed to update alive.yaml')
            return {
                'ret': 'warning',
                'msg': f'설정을 저장했지만 alive.yaml 갱신은 실패했습니다: {str(exception)}',
            }


@blueprint.route('/api/playlist.m3u', methods=['GET'])
def api_playlist():
    require_api_key(request)
    mode = request.args.get('mode', 'stream')
    include_param = request.args.get('include')
    if include_param is None:
        include_all = ModelSetting.get_bool('include_hidden_channels')
    else:
        include_all = include_param == 'all'
    return Response(build_playlist(request, mode=mode, include_all=include_all), content_type='audio/mpegurl')


@blueprint.route('/api/playlist_tvh.m3u', methods=['GET'])
def api_playlist_tvh():
    require_api_key(request)
    mode = request.args.get('mode', 'stream')
    include_param = request.args.get('include')
    if include_param is None:
        include_all = ModelSetting.get_bool('include_hidden_channels')
    else:
        include_all = include_param == 'all'
    return Response(build_playlist_tvh(request, mode=mode, include_all=include_all), content_type='audio/mpegurl')


@blueprint.route('/api/channels.json', methods=['GET'])
def api_channels():
    require_api_key(request)
    include_param = request.args.get('include')
    if include_param is None:
        include_all = ModelSetting.get_bool('include_hidden_channels')
    else:
        include_all = include_param == 'all'

    program_map = get_current_program_map()
    channels = []
    for channel in get_channels():
        if channel.get('hidden') and not include_all:
            continue
        payload = make_channel_payload(channel, request)
        payload['current_program'] = program_map.get(channel['id'], '')
        channels.append(payload)

    return jsonify({'updated_at': int(time.time()), 'channels': channels})


@blueprint.route('/api/epg.xml', methods=['GET'])
def api_epg():
    require_api_key(request)
    path = get_epg_output_path()
    if not path.exists():
        try:
            generate_epg_xml()
        except Exception:
            logger.error(traceback.format_exc())
            abort(502)
    if not path.exists():
        abort(404)
    return Response(path.read_text(encoding='utf-8', errors='replace'), content_type='application/xml; charset=utf-8')


@blueprint.route('/player/<channel_id>', methods=['GET'])
def page_player(channel_id):
    require_api_key(request)
    channels = get_channel_map()
    channel = channels.get(channel_id)
    if channel is None:
        abort(404)
    payload = make_channel_payload(channel, request)
    payload['current_program'] = get_current_program_map().get(channel_id, '')
    logger.info('[PLAYER] popup_open channel=%s type=%s remote=%s ua=%s', channel.get('name'), channel.get('type'), request.remote_addr, request.headers.get('User-Agent', ''))
    return render_template(f'{package_name}_player.html', arg={
        'package_name': package_name,
        'channel': payload,
        'play_url': payload['hls_url'],
    })


@blueprint.route('/ajax/epg_generate', methods=['POST'])
def ajax_epg_generate():
    try:
        output = generate_epg_xml()
        return jsonify({'ret': 'success', 'msg': 'EPG generated', 'path': str(output)})
    except Exception as exception:
        append_epg_log(f'EPG generate failed: {str(exception)}')
        logger.error(traceback.format_exc())
        return jsonify({'ret': 'danger', 'msg': str(exception)})


@blueprint.route('/ajax/epg_schedule', methods=['POST'])
def ajax_epg_schedule():
    try:
        enabled = request.form.get('enabled', '')
        schedule = request.form.get('schedule', '')
        return jsonify(sync_epg_scheduler_job(enabled, schedule))
    except Exception as exception:
        logger.error(traceback.format_exc())
        return jsonify({'ret': 'danger', 'msg': str(exception)})


@blueprint.route('/ajax/ffmpeg_version', methods=['POST'])
def ajax_ffmpeg_version():
    try:
        return jsonify(P.logic.get_module('setting')._ffmpeg_version())
    except Exception as exception:
        logger.error(traceback.format_exc())
        return jsonify({'ret': 'danger', 'msg': str(exception)})


@blueprint.route('/ajax/transcode_capabilities', methods=['POST'])
def ajax_transcode_capabilities():
    try:
        return jsonify(P.logic.get_module('setting')._transcode_capabilities())
    except Exception as exception:
        logger.error(traceback.format_exc())
        return jsonify({'ret': 'danger', 'msg': str(exception)})


@blueprint.route('/api/stream/<channel_id>', methods=['GET'])
def api_stream(channel_id):
    require_api_key(request)
    channels = get_channel_map()
    channel = channels.get(channel_id)
    if channel is None:
        abort(404)
    logger.info('[PLAYER] stream_request channel=%s type=%s remote=%s ua=%s', channel.get('name'), channel.get('type'), request.remote_addr, request.headers.get('User-Agent', ''))

    if channel['type'] == 'forward':
        return redirect(channel['url'], code=302)

    if channel['type'] in ('udp', 'rtp'):
        return Response(stream_via_ffmpeg_copy(channel), mimetype='video/MP2T')

    if channel['type'] == 'http_stream':
        return Response(stream_via_ffmpeg_copy(channel), mimetype='video/MP2T')

    if channel['type'] == 'http_hls':
        content = get_proxied_m3u8(channel_id, request)
        if content is None:
            abort(502)
        return Response(content, content_type='application/vnd.apple.mpegurl')

    start_hls_proxy(channel_id)
    item_queue = queue.Queue(maxsize=10)
    with STATE_LOCK:
        hls_clients[channel_id].append(item_queue)
        last_access_hls_proxy[channel_id] = time.time()

    def generate():
        try:
            while True:
                try:
                    data = item_queue.get(timeout=15)
                except queue.Empty:
                    break
                yield data
        finally:
            with STATE_LOCK:
                if channel_id in hls_clients and item_queue in hls_clients[channel_id]:
                    hls_clients[channel_id].remove(item_queue)
                last_access_hls_proxy[channel_id] = time.time()

    return Response(generate(), mimetype='video/MP2T')


@blueprint.route('/api/hls/<channel_id>', methods=['GET'])
def api_hls(channel_id):
    require_api_key(request)
    channels = get_channel_map()
    channel = channels.get(channel_id)
    if channel is None:
        abort(404)
    logger.info('[PLAYER] hls_request channel=%s type=%s remote=%s ua=%s', channel.get('name'), channel.get('type'), request.remote_addr, request.headers.get('User-Agent', ''))
    request_options = get_hls_request_options(request)

    if channel['type'] == 'forward':
        return redirect(channel['url'], code=302)

    if hls_request_uses_proxy(channel, request_options):
        content = get_proxied_m3u8(channel_id, request)
        if content is None:
            abort(502)
        return Response(content, content_type='application/vnd.apple.mpegurl')

    hls_key = start_hls(channel_id, request_options)
    if not hls_key:
        abort(400)
    playlist = get_hls_playlist_path(hls_key)

    for _ in range(60):
        if playlist.exists():
            break
        time.sleep(0.5)
    if not playlist.exists():
        abort(404)

    for _ in range(60):
        ts_files = list(get_hls_dir().glob(f'{hls_key}_*.ts'))
        if len(ts_files) >= 2:
            break
        time.sleep(0.5)

    content = playlist.read_text(encoding='utf-8')
    base = get_base_url(request)
    lines = []
    for line in content.splitlines():
        if line.endswith('.ts'):
            lines.append(with_apikey(f'{base}/api/ts/{line}'))
        else:
            lines.append(line)

    with STATE_LOCK:
        last_access_hls[hls_key] = time.time()
    return Response('\n'.join(lines) + '\n', content_type='application/vnd.apple.mpegurl')


@blueprint.route('/api/seg/<path:encoded_url>', methods=['GET'])
def api_seg(encoded_url):
    require_api_key(request)
    segment_url = urllib.parse.unquote(encoded_url)
    data = fetch_url(segment_url)
    if not data:
        abort(502)
    return Response(data, mimetype='video/MP2T')


@blueprint.route('/api/ts/<path:filename>', methods=['GET'])
def api_ts(filename):
    require_api_key(request)
    filename = os.path.basename(filename)
    hls_key = get_hls_key_from_segment_filename(filename)
    with STATE_LOCK:
        last_access_hls[hls_key] = time.time()
    path = get_hls_dir() / filename
    if not path.exists():
        abort(404)
    return Response(path.read_bytes(), mimetype='video/MP2T')
