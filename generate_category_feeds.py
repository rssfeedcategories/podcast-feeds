#!/usr/bin/env python3
"""
generate_category_feeds.py
==========================
Smart RSS feed generator with three modes:

  AUTO (default):
    - If checkpoint.json exists -> INCREMENTAL (checks page 1 only)
    - If no checkpoint.json -> FULL (first time only)

  INCREMENTAL (--incremental):
    - Checks only page 1 of each category for new episodes
    - Only updates XML and checkpoint if new episodes found
    - No changes = no commits to GitHub

  FULL (--full):
    - Fetches every page of every category
    - Full rebuild of all XML feeds

Requirements:
    pip install requests openpyxl
"""

import os
import re
import sys
import time
import json
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from datetime import datetime

import openpyxl
import requests

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

MASTER_RSS_URL = "https://rss.libsyn.com/shows/93012/destinations/468609.xml"
EXCEL_FILE     = "Havineni_RSS_Feed.xlsx"
OUTPUT_DIR     = Path("output_feeds")
CHECKPOINT     = Path("checkpoint.json")
REQUEST_DELAY  = 0.5

# ──────────────────────────────────────────────────────────────────────────────


def normalize(text: str) -> str:
    text = (text or "")
    text = text.replace('\u201c', '"').replace('\u201d', '"')
    text = text.replace('\u2018', "'").replace('\u2019', "'")
    text = text.replace('\u05f4', '"').replace('\u05f3', "'")
    return re.sub(r"\s+", " ", text).strip().lower()


def safe_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', '_', name.strip())
    return name[:80] or "category"


def load_categories(excel_path: str) -> list:
    wb = openpyxl.load_workbook(excel_path)
    ws = wb.active
    cats = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        name, url = row[0], row[1]
        if name and url:
            cats.append((str(name).strip(), str(url).strip()))
    return cats


def category_url_to_json_api(page_url: str, page_num: int) -> str:
    parsed = urlparse(page_url)
    path = parsed.path.rstrip("/")
    new_path = path.replace("/podcast/category/", f"/podcast/page/{page_num}/category/", 1)
    new_path = new_path + "/render-type/json"
    return urlunparse(parsed._replace(path=new_path, query=""))


def fetch_page(url: str, referer: str, page_num: int) -> list:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/javascript, */*",
        "Referer": referer,
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or text in ("", "[]", "null"):
            return []
        data = resp.json()
        if not data or not isinstance(data, list):
            return []
        return data
    except Exception as e:
        print(f"   WARNING: Page {page_num} error: {e}")
        return []


def fetch_all_titles_full(category_url: str) -> set:
    titles = set()
    page = 1
    while True:
        api_url = category_url_to_json_api(category_url, page)
        data = fetch_page(api_url, category_url, page)
        if not data:
            break
        for ep in data:
            t = ep.get("item_title", "")
            if t:
                titles.add(normalize(t))
        print(f"   Page {page}: {len(data)} episodes (total: {len(titles)})")
        page += 1
        time.sleep(REQUEST_DELAY)
    return titles


def fetch_new_titles_incremental(category_url: str, known_titles: set) -> set:
    """Check page 1 only. Returns ONLY titles not already known."""
    api_url = category_url_to_json_api(category_url, 1)
    data = fetch_page(api_url, category_url, 1)
    new_titles = set()
    for ep in data:
        t = ep.get("item_title", "")
        if t:
            norm = normalize(t)
            if norm not in known_titles:
                new_titles.add(norm)
    return new_titles


def load_checkpoint() -> dict:
    if CHECKPOINT.exists():
        try:
            return json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_checkpoint(data: dict):
    CHECKPOINT.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def build_checkpoint_from_existing_feeds(categories: list, item_title_map: list) -> dict:
    print("\nBuilding checkpoint from existing XML files...")
    checkpoint = {}
    for cat_name, _ in categories:
        fname = OUTPUT_DIR / f"{safe_filename(cat_name)}.xml"
        if not fname.exists():
            checkpoint[cat_name] = []
            continue
        try:
            tree = ET.parse(str(fname))
            items = tree.findall(".//item")
            titles = [normalize(item.findtext("title") or "") for item in items
                      if item.findtext("title")]
            checkpoint[cat_name] = titles
            print(f"   {cat_name}: {len(titles)} episodes")
        except Exception as e:
            print(f"   WARNING: Could not read {fname}: {e}")
            checkpoint[cat_name] = []
    save_checkpoint(checkpoint)
    print(f"Checkpoint saved.\n")
    return checkpoint


def fetch_master_rss(url: str) -> ET.Element:
    print(f"Fetching master RSS: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; RSSCategoryBuilder/1.0)"}
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"Cannot fetch master RSS feed: {e}")

    for prefix, uri in {
        "itunes":  "http://www.itunes.com/dtds/podcast-1.0.dtd",
        "atom":    "http://www.w3.org/2005/Atom",
        "content": "http://purl.org/rss/1.0/modules/content/",
        "media":   "http://search.yahoo.com/mrss/",
        "podcast": "https://podcastindex.org/namespace/1.0",
    }.items():
        ET.register_namespace(prefix, uri)

    return ET.fromstring(resp.content)


def build_category_feed(root, matched_items, category_name):
    new_root = ET.Element(root.tag, root.attrib)
    channel_src = root.find("channel")
    new_channel = ET.SubElement(new_root, "channel")
    for child in channel_src:
        if child.tag == "item":
            continue
        new_channel.append(deepcopy(child))
    title_el = new_channel.find("title")
    if title_el is not None:
        title_el.text = f"{title_el.text} - {category_name}"
    for item in matched_items:
        new_channel.append(deepcopy(item))
    return new_root


def write_feed(root, path):
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(str(path), encoding="utf-8", xml_declaration=True)


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # ── Determine mode ─────────────────────────────────────────────────────────
    if "--full" in sys.argv:
        mode = "FULL"
    elif "--incremental" in sys.argv:
        mode = "INCREMENTAL"
    else:
        mode = "INCREMENTAL" if CHECKPOINT.exists() else "FULL"

    print(f"{'='*60}")
    print(f"Mode: {mode}")
    print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    if not os.path.exists(EXCEL_FILE):
        raise FileNotFoundError(f"Excel file '{EXCEL_FILE}' not found.")

    categories = load_categories(EXCEL_FILE)
    print(f"Loaded {len(categories)} categories.\n")

    master_root = fetch_master_rss(MASTER_RSS_URL)
    channel     = master_root.find("channel")
    all_items   = channel.findall("item")
    print(f"Master RSS has {len(all_items)} episodes.\n")

    item_title_map = [
        (normalize(item.findtext("title") or ""), item)
        for item in all_items
    ]

    # ── Load or build checkpoint ───────────────────────────────────────────────
    checkpoint = load_checkpoint()
    if not checkpoint and any(OUTPUT_DIR.glob("*.xml")):
        checkpoint = build_checkpoint_from_existing_feeds(categories, item_title_map)
        mode = "INCREMENTAL"
        print("Existing XML files found — switching to INCREMENTAL mode.\n")

    summary = []
    new_checkpoint = dict(checkpoint)
    total_new = 0  # track globally if anything changed

    for i, (cat_name, cat_url) in enumerate(categories, 1):
        print(f"\n[{i}/{len(categories)}] {cat_name}")
        known_titles = set(checkpoint.get(cat_name, []))

        if mode == "FULL":
            all_titles = fetch_all_titles_full(cat_url)
            new_titles = all_titles - known_titles
        else:
            # INCREMENTAL: only fetch page 1, only get genuinely new titles
            new_titles = fetch_new_titles_incremental(cat_url, known_titles)
            all_titles = known_titles | new_titles

        if new_titles:
            print(f"   +{len(new_titles)} NEW episode(s) found: {list(new_titles)[:3]}")
            total_new += len(new_titles)
        else:
            print(f"   No new episodes.")

        # Only update checkpoint and XML if something changed
        if new_titles or mode == "FULL":
            new_checkpoint[cat_name] = list(all_titles)

            matched = [item for (norm_title, item) in item_title_map
                       if norm_title in all_titles]

            if not matched:
                matched = [
                    item for (norm_title, item) in item_title_map
                    if any(pt in norm_title or norm_title in pt
                           for pt in all_titles if len(pt) > 8)
                ]

            print(f"   {len(matched)} episode(s) in feed")

            if matched:
                feed_root = build_category_feed(master_root, matched, cat_name)
                out_path  = OUTPUT_DIR / f"{safe_filename(cat_name)}.xml"
                write_feed(feed_root, out_path)
                if new_titles:
                    print(f"   Updated -> {out_path}")

        summary.append((cat_name, len(all_titles & set(t for t, _ in item_title_map)), len(new_titles)))

    # ── Only save checkpoint if something actually changed ─────────────────────
    if total_new > 0 or mode == "FULL":
        save_checkpoint(new_checkpoint)
        print(f"\nCheckpoint updated ({total_new} new episodes total).")
    else:
        print(f"\nNo new episodes found — checkpoint unchanged, no commit needed.")

    print("\n" + "=" * 60)
    print(f"SUMMARY  ({mode} mode)")
    print("=" * 60)
    for cat_name, count, new in summary:
        new_str = f"  (+{new} NEW)" if new > 0 else ""
        print(f"  {'**' if new else '  '}  {cat_name}: {count} episode(s){new_str}")
    print(f"\nTotal new episodes detected: {total_new}")
    print(f"Done! Output: {OUTPUT_DIR.resolve()}/")


if __name__ == "__main__":
    main()
