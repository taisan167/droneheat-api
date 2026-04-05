import os
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

SERPAPI_KEY = os.getenv("SERPAPI_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

SEARCH_QUERIES = [
    "名古屋駅 ドローンスクール 駅近",
    "名古屋駅 ドローンスクール 徒歩",
    "名古屋 ドローンスクール 国家資格",
    "名古屋 ドローンスクール 国家資格 更新講習",
    "名古屋駅 国家資格 更新講習 ドローン",
    "なごのキャンパス ドローンスクール",
    "DSA ドローンスクール 名古屋",
]

TARGET_URLS = [
    "https://coeteco.jp/articles/15007",
    "https://coeteco.jp/articles/15009",
]

DSA_KEYWORDS = ["DSA", "なごのキャンパス", "DSAなごのキャンパス"]

SERPAPI_ENDPOINT = "https://serpapi.com/search"
NOTION_API_VERSION = "2022-06-28"
NOTION_PAGES_ENDPOINT = "https://api.notion.com/v1/pages"


def fetch_search_results(query: str, api_key: str) -> dict:
    params = {
        "q": query,
        "api_key": api_key,
        "engine": "google",
        "hl": "ja",
        "gl": "jp",
    }
    response = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_aio_detail(serpapi_link: str, api_key: str) -> dict:
    response = requests.get(serpapi_link, params={"api_key": api_key}, timeout=30)
    response.raise_for_status()
    return response.json()


def render_text_blocks(text_blocks: list) -> str:
    lines = []
    for block in text_blocks:
        block_type = block.get("type")
        if block_type == "paragraph":
            snippet = block.get("snippet", "").strip()
            if snippet:
                lines.append(snippet)
        elif block_type == "list":
            for item in block.get("list", []):
                snippet = item.get("snippet", "").strip()
                if snippet:
                    lines.append(f"  • {snippet}")
    return "\n".join(lines)


def collect_sources(text_blocks: list, references: list) -> list:
    ref_by_index = {r["index"]: r for r in references}
    seen_links = set()
    sources = []

    def add(title: str, link: str) -> None:
        if link and link not in seen_links:
            seen_links.add(link)
            sources.append({"title": title, "link": link})

    for block in text_blocks:
        for idx in block.get("reference_indexes", []):
            ref = ref_by_index.get(idx)
            if ref:
                add(ref.get("title", "（タイトルなし）"), ref.get("link", ""))
        for sl in block.get("snippet_links", []):
            add(sl.get("text", "（タイトルなし）"), sl.get("link", ""))
        for item in block.get("list", []):
            for sl in item.get("snippet_links", []):
                add(sl.get("text", "（タイトルなし）"), sl.get("link", ""))

    return sources


def check_target_urls(sources: list) -> list:
    """引用ソースの中から TARGET_URLS に一致するものを返す"""
    source_links = {s["link"] for s in sources}
    return [url for url in TARGET_URLS if url in source_links]


def extract_dsa_mentions(full_text: str) -> str:
    """AIOテキストからDSAキーワードを含む行を抽出する"""
    mentions = [
        line.strip()
        for line in full_text.split("\n")
        if any(kw in line for kw in DSA_KEYWORDS)
    ]
    return "\n".join(mentions)


def scrape_coeteco_page(url: str) -> str:
    """coeteco.jp のページをスクレイピングしてDSA関連テキストを抽出する"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return f"スクレイピングエラー: {e}"

    soup = BeautifulSoup(response.text, "html.parser")
    body = soup.find("article") or soup.find("main") or soup.body
    if not body:
        return "本文が取得できませんでした"

    dsa_paragraphs = [
        tag.get_text(strip=True)
        for tag in body.find_all(["p", "h1", "h2", "h3", "h4", "li"])
        if any(kw in tag.get_text() for kw in DSA_KEYWORDS)
    ]

    if dsa_paragraphs:
        return "\n".join(dsa_paragraphs)

    # DSA言及なし → 冒頭500文字を参考として返す
    all_text = body.get_text(separator="\n", strip=True)
    return f"（DSA言及なし）\n{all_text[:500]}"


def save_to_notion(
    query: str,
    full_text: str,
    sources: list,
    has_aio: bool,
    cited_urls: list,
    dsa_mentions: str,
    scraping_content: str,
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    sources_text = "\n".join(
        f"{i}. {s['title']}\n   {s['link']}"
        for i, s in enumerate(sources, start=1)
    )[:2000]

    cited_text = "\n".join(cited_urls) if cited_urls else "引用なし"

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_API_VERSION,
    }
    body = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "検索キーワード": {
                "title": [{"text": {"content": query}}]
            },
            "AIOテキスト": {
                "rich_text": [{"text": {"content": full_text[:2000]}}]
            },
            "引用ソース": {
                "rich_text": [{"text": {"content": sources_text or "なし"}}]
            },
            "取得日時": {
                "date": {"start": now}
            },
            "AIO有無": {
                "checkbox": has_aio
            },
            "対象URL引用有無": {
                "rich_text": [{"text": {"content": cited_text}}]
            },
            "DSA言及箇所": {
                "rich_text": [{"text": {"content": dsa_mentions[:2000] or "なし"}}]
            },
            "スクレイピング内容": {
                "rich_text": [{"text": {"content": scraping_content[:2000] or "なし"}}]
            },
        },
    }
    response = requests.post(NOTION_PAGES_ENDPOINT, headers=headers, json=body, timeout=30)
    if not response.ok:
        print(f"  Notionエラー詳細: {response.text}")
    response.raise_for_status()
    print("  → Notionへの保存完了")


def process_query(query: str, api_key: str) -> None:
    print(f"\n{'='*50}")
    print(f"検索: {query}")

    try:
        data = fetch_search_results(query, api_key)
    except requests.exceptions.RequestException as e:
        print(f"  検索エラー: {e}")
        return

    aio = data.get("ai_overview")

    if not aio:
        print("  AIO: なし")
        save_to_notion(query, "", [], False, [], "", "")
        return

    # AIO詳細が別エンドポイントにある場合は追加取得
    serpapi_link = aio.get("serpapi_link")
    if serpapi_link and not aio.get("text_blocks") and not aio.get("references"):
        print("  AIOの詳細データを取得中...")
        try:
            detail = fetch_aio_detail(serpapi_link, api_key)
            aio = detail.get("ai_overview", aio)
        except requests.exceptions.RequestException as e:
            print(f"  AIO詳細取得エラー: {e}")

    text_blocks = aio.get("text_blocks") or []
    full_text = render_text_blocks(text_blocks) or aio.get("snippet", "（テキストなし）")
    references = aio.get("references") or []
    sources = collect_sources(text_blocks, references)

    print(f"  AIO: あり（引用ソース {len(sources)} 件）")

    # 対象URL引用チェック
    cited_urls = check_target_urls(sources)
    print(f"  対象URL引用: {'あり → ' + ', '.join(cited_urls) if cited_urls else 'なし'}")

    # AIOテキスト内のDSA言及抽出
    dsa_mentions = extract_dsa_mentions(full_text)
    print(f"  AIO内DSA言及: {'あり' if dsa_mentions else 'なし'}")

    # スクレイピング（対象URLが引用されている場合のみ実行）
    scraping_parts = []
    for url in cited_urls:
        print(f"  スクレイピング: {url}")
        content = scrape_coeteco_page(url)
        scraping_parts.append(f"[{url}]\n{content}")
    scraping_content = "\n\n".join(scraping_parts)

    save_to_notion(query, full_text, sources, True, cited_urls, dsa_mentions, scraping_content)


def main() -> None:
    if not SERPAPI_KEY:
        print("エラー: SERPAPI_KEY が .env に設定されていません")
        return
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        print("エラー: NOTION_TOKEN または NOTION_DATABASE_ID が .env に設定されていません")
        return

    print(f"クエリ数: {len(SEARCH_QUERIES)} 件")
    print(f"監視対象URL: {TARGET_URLS}")

    for query in SEARCH_QUERIES:
        process_query(query, SERPAPI_KEY)

    print("\n全クエリの処理が完了しました。")


if __name__ == "__main__":
    main()
