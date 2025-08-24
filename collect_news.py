import feedparser
import json
import os
from datetime import datetime
import pytz

# 분석할 RSS 피드 목록
RSS_FEEDS = {
    "all_news": "https://www.investing.com/rss/news.rss",
    "economic_indicators": "https://www.investing.com/rss/news_25.rss",
    "forex_news": "https://www.investing.com/rss/news_1.rss"
}

# 데이터가 저장될 디렉토리 및 파일 경로
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "news_data.jsonl")

def parse_published_date(entry):
    """'published_parsed' 필드를 사용하여 날짜를 파싱하고 UTC로 변환"""
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6])
        return dt.replace(tzinfo=pytz.utc).isoformat()
    return datetime.now(pytz.utc).isoformat()

def fetch_and_parse_feeds():
    """RSS 피드를 가져와 파싱하고 새로운 뉴스 아이템을 반환"""
    new_articles = []
    for feed_name, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                article = {
                    "guid": entry.get("id", entry.get("link")), # GUID가 없으면 링크를 사용
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published_utc": parse_published_date(entry),
                    "summary": entry.get("summary", ""),
                    "source_feed": feed_name
                }
                new_articles.append(article)
        except Exception as e:
            print(f"Error fetching or parsing feed {url}: {e}")
    return new_articles

def main():
    """메인 실행 함수: 기존 데이터를 로드하고 새로운 데이터를 추가"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    existing_guids = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    existing_guids.add(json.loads(line)['guid'])
                except (json.JSONDecodeError, KeyError):
                    continue # 손상된 라인은 무시

    all_articles = fetch_and_parse_feeds()
    
    new_count = 0
    with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        for article in all_articles:
            if article['guid'] not in existing_guids:
                f.write(json.dumps(article, ensure_ascii=False) + '\n')
                existing_guids.add(article['guid'])
                new_count += 1

    print(f"Process completed. Found {len(all_articles)} total articles, added {new_count} new articles.")

if __name__ == "__main__":
    main()