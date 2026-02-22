# Extracts tweets using single-page Strategy C with correct lexical hierarchy
import tweepy
import sqlite3
import time
import re
from datetime import datetime, timedelta, timezone

# ===============================
# USER CONFIGURATION
# ===============================
BEARER_TOKEN = "" 
DB_PATH = ""

MAX_TWEETS_PER_SLICE = 100      # max tweets saved per term per slice
DAYS_BACK = [7]                 # list of days back to sample (0=today, 1=yesterday, etc.)
SLICE_HOUR_UTC = 0             # hour of day (UTC) to sample
SLICE_DURATION_MINUTES = 1438     # duration of each slice in minutes

EXCLUDE_TERMS = ["bot", "spam", "giveaway", "crypto", "airdrop", "NFT", "retweet", "follow"]

SEMANTIC_SETS = {
    "political": [
        #["import", "tariff", "tax", "possession"],
        #["public school", "education", "institution", "organization"],
        #["instruction", "rule", "regulation", "restriction"],
        #["", "rule", "", ""],
        #["census", "count", "investigation", "work"],
        #["", "count", "", ""],
        #["constitution", "legal document", "writing", "communication"],
        #["", "", "writing", ""],
        #["honesty", "righteousness", "morality", "attribute"],
        #["government", "organization", "social group", "group"],
        #["", "", "presentment", "judgment"],
        #["due process", "legal proceeding", "presentment", "judgment"],
        #["democrat", "politician", "leader", "person"],
        #["park", "tract", "geographical area", "region"],
        #["park", "tract", "", ""],
    ],
    "non_political": [
        #["horse", "equine", "mammal", "animal"],
        #["", "", "", "animal"],
        #["", "equine", "mammal", ""],
        #["volleyball", "sport", "game", "activity"],
        #["canoe", "boat", "vessel", "transport"],
        #["rain", "precipitation", "weather", "phenomenon"],
        #["canyon" , "ravine", "natural depression", "geological formation"] ,
        ["" , "", "", "geological formation"] ,
        #["" , "", "natural depression", ""] ,
        #["beach", "shore", "coast", "land"],
        #["", "shore", "", ""],
        #["octopus", "cephalopod", "mollusk", "invertebrate"],
        ["", "cephalopod", "", ""],
        #["", "", "mollusk", "invertebrate"],
        #["laptop", "computer", "machine", "device"],
        #["", "", "machine", ""],
        #["grass", "herb", "plant", "organism"],
        #["", "", "plant", ""],
        #["", "herb", "", ""],
        #["chair", "seat", "furniture", "object"],
        #["", "", "", "object"],
    ]
}

# ===============================
# Twitter API setup
# ===============================
client = tweepy.Client(
    bearer_token=BEARER_TOKEN,
    wait_on_rate_limit=True
)

# ===============================
# Database setup
# ===============================
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# NOTE: This CREATE TABLE is here only in case the DB is new.
# If your DB already exists, it won't overwrite anything.
c.execute("""
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    semantic_set TEXT,
    lexical_hierarchy INTEGER,
    search_term TEXT,
    text TEXT,
    usnmtext TEXT,
    term_present BOOLEAN,
    corr_def REAL,
    created_at TEXT,
    like_count INTEGER,
    retweet_count INTEGER,
    reply_count INTEGER,
    sentiment_score REAL,
    user_id TEXT,
    username TEXT,
    name TEXT,
    followers_count INTEGER,
    following_count INTEGER,
    tweet_count INTEGER,
    account_created_at TEXT
)
""")
conn.commit()

# ===============================
# Helper functions
# ===============================
def build_query(search_term):
    query = f'"{search_term}" -is:retweet -has:links -is:quote'
    for term in EXCLUDE_TERMS:
        query += f' -"{term}"'

    # EXCLUDE ONLY: tweets from or mentioning @equine__dentist
    query += " -from:equine__dentist -@equine__dentist"

    return query

def strip_leading_handles(text):
    """
    Removes leading tokens that start with '@' (only at the beginning).
    Does NOT remove @mentions later in the text.
    """
    if not text:
        return text
    tokens = text.split()
    i = 0
    while i < len(tokens) and tokens[i].startswith("@"):
        i += 1
    return " ".join(tokens[i:])

def whole_word_present(text, term):
    """
    Case-insensitive whole-word / whole-phrase match.
    - tax should NOT match taxation
    - multi-word phrase should match as a phrase
    """
    if not text or not term:
        return False
    pattern = r"\b" + re.escape(term) + r"\b"
    return re.search(pattern, text, flags=re.IGNORECASE) is not None

def insert_tweet(tweet, user, semantic_set, hierarchy_idx, search_term):
    usnmtext_value = strip_leading_handles(tweet.text)
    term_present_value = 1 if whole_word_present(usnmtext_value, search_term) else 0
    corr_def_value = None  # placeholder

    c.execute("""
    INSERT OR IGNORE INTO tweets (
        tweet_id, semantic_set, lexical_hierarchy, search_term,
        text, usnmtext, term_present, corr_def,
        created_at, like_count, retweet_count, reply_count, sentiment_score,
        user_id, username, name, followers_count, following_count, tweet_count, account_created_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """, (
        tweet.id,
        semantic_set,
        hierarchy_idx,      # lexical hierarchy position
        search_term,
        tweet.text,
        usnmtext_value,
        term_present_value,
        corr_def_value,
        tweet.created_at.isoformat(),
        tweet.public_metrics.get("like_count", 0),
        tweet.public_metrics.get("retweet_count", 0),
        tweet.public_metrics.get("reply_count", 0),
        None,               # sentiment score placeholder
        user.id,
        user.username,
        user.name,
        user.public_metrics.get("followers_count", 0),
        user.public_metrics.get("following_count", 0),
        user.public_metrics.get("tweet_count", 0),
        user.created_at.isoformat()
    ))
    conn.commit()

# ===============================
# Strategy C execution
# ===============================
script_start = datetime.now(timezone.utc)
print("Script start:", script_start.isoformat())

for semantic_set, hierarchies in SEMANTIC_SETS.items():
    for hierarchy_idx, hierarchy in enumerate(hierarchies):
        for term_idx, term in enumerate(hierarchy):
            # Skip empty placeholders
            if not term or not term.strip():
                continue

            query = build_query(term)
            print(f"\nSampling term: {term}, semantic set '{semantic_set}', hierarchy index {term_idx}")

            tweets_saved = 0

            for day_back in DAYS_BACK:
                # Calculate slice start and end based on user-defined time
                slice_start = datetime(
                    year=script_start.year,
                    month=script_start.month,
                    day=(script_start - timedelta(days=day_back)).day,
                    hour=SLICE_HOUR_UTC,
                    minute=0,
                    second=0,
                    tzinfo=timezone.utc
                )
                slice_end = slice_start + timedelta(minutes=SLICE_DURATION_MINUTES)

                # Ensure slice is at least 10 seconds before now (Twitter API requirement)
                if slice_end >= script_start - timedelta(seconds=10):
                    slice_end = script_start - timedelta(seconds=10)
                if slice_start >= slice_end:
                    print(f"  Skipping day {day_back}: slice start >= slice end")
                    continue

                try:
                    response = client.search_recent_tweets(
                        query=query,
                        tweet_fields=["author_id", "created_at", "public_metrics", "lang"],
                        expansions=["author_id"],
                        user_fields=["username", "name", "public_metrics", "created_at"],
                        start_time=slice_start,
                        end_time=slice_end,
                        max_results=MAX_TWEETS_PER_SLICE
                        # single page only, no paginator
                    )

                    if not response.data:
                        print(f"  Day {day_back}: 0 tweets returned")
                        continue

                    users = {u.id: u for u in response.includes["users"]} if response.includes and "users" in response.includes else {}

                    slice_saved = 0
                    for tweet in response.data:
                        user = users.get(tweet.author_id)
                        if user:
                            insert_tweet(tweet, user, semantic_set, term_idx, term)
                            tweets_saved += 1
                            slice_saved += 1

                    print(f"  Day {day_back}: returned={len(response.data)}, saved={slice_saved}")

                except Exception as e:
                    print(f"  Error on day {day_back}: {e}")
                    time.sleep(5)

            print(f"Finished term '{term}': saved {tweets_saved} tweets total")

conn.close()
print("Done collecting tweets.")
