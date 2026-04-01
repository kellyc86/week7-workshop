"""
Zoo 55 Competitor Intelligence: YouTube Data Collector
"""

import os
import csv
import time
from datetime import datetime, timezone
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY)

CHANNELS = {
    "UC2ccm1GajfSujz7T18d7cKA": "UC2ccm1GajfSujz7T18d7cKA",
    "UCEbOEsyAuh1rPKlpbtLvnjQ": "UCEbOEsyAuh1rPKlpbtLvnjQ",
    "UCFrO-dKhooOuTtix5dia2_g": "UCFrO-dKhooOuTtix5dia2_g",
    "UCb7xZQi7F3RW7BNtR57cNnA": "UCb7xZQi7F3RW7BNtR57cNnA",
    "UC_0r3EheCnp-wVvndYDGviQ": "UC_0r3EheCnp-wVvndYDGviQ",
    "UCZSE95RmyMUgJWmfra9Yx1A": "UCZSE95RmyMUgJWmfra9Yx1A",
    "UCiZ47iCEOmqsyZU_6XP9ekg": "UCiZ47iCEOmqsyZU_6XP9ekg",
    "UCS1MgGMoPaagPOzV1EHknhw": "UCS1MgGMoPaagPOzV1EHknhw",
    "UCeDnHAtv092zrrVIai87N4Q": "UCeDnHAtv092zrrVIai87N4Q",
}

VIDEOS_PER_CHANNEL = 50


def get_channel_stats(channel_id):
    response = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    ).execute()
    item = response["items"][0]
    stats = item["statistics"]
    snippet = item["snippet"]
    return {
        "channel_id": channel_id,
        "channel_name": snippet.get("title", channel_id),
        "subscriber_count": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
        "country": snippet.get("country", "unknown"),
        "published_at": snippet.get("publishedAt", ""),
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }


def get_channel_videos(channel_id):
    channel_response = youtube.channels().list(
        part="contentDetails,snippet",
        id=channel_id
    ).execute()
    channel_name = channel_response["items"][0]["snippet"]["title"]
    uploads_playlist = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    playlist_response = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=uploads_playlist,
        maxResults=VIDEOS_PER_CHANNEL
    ).execute()
    video_ids = [item["contentDetails"]["videoId"] for item in playlist_response.get("items", [])]
    if not video_ids:
        return []
    video_response = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids)
    ).execute()
    videos = []
    for item in video_response.get("items", []):
        snippet = item["snippet"]
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        publish_date = datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00"))
        days_since_publish = max((datetime.now(timezone.utc) - publish_date).days, 1)
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0))
        comments = int(stats.get("commentCount", 0))
        engagement_rate = round((likes + comments) / views * 100, 4) if views > 0 else 0
        views_per_day = round(views / days_since_publish, 1)
        videos.append({
            "video_id": item["id"],
            "channel_id": channel_id,
            "channel_name": channel_name,
            "title": snippet.get("title", ""),
            "publish_date": snippet["publishedAt"],
            "days_since_publish": days_since_publish,
            "duration": content.get("duration", ""),
            "views": views,
            "likes": likes,
            "comments": comments,
            "engagement_rate": engagement_rate,
            "views_per_day": views_per_day,
            "category_id": snippet.get("categoryId", ""),
            "tags": "|".join(snippet.get("tags", [])),
            "description_length": len(snippet.get("description", "")),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        })
    return videos


def save_to_csv(data, filename):
    if not data:
        print(f"  No data to save for {filename}")
        return
    with open(f"data/{filename}", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  Saved {len(data)} rows to data/{filename}")


def main():
    print("Zoo 55 Competitor Intelligence — Data Collection")
    print("=" * 50)
    all_channel_stats = []
    all_videos = []
    for channel_id in CHANNELS:
        print(f"\nCollecting: {channel_id}")
        try:
            stats = get_channel_stats(channel_id)
            all_channel_stats.append(stats)
            print(f"  Name: {stats['channel_name']}")
            print(f"  Subscribers: {stats['subscriber_count']:,} | Videos: {stats['video_count']:,}")
            videos = get_channel_videos(channel_id)
            all_videos.extend(videos)
            print(f"  Collected {len(videos)} videos")
            time.sleep(0.5)
        except Exception as e:
            print(f"  ERROR: {e}")
    print("\nSaving data...")
    save_to_csv(all_channel_stats, "channel_stats.csv")
    save_to_csv(all_videos, "video_data.csv")
    print("\nDone!")


if __name__ == "__main__":
    main()