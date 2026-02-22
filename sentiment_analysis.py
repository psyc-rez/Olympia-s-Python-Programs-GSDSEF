import sqlite3
import torch
import numpy as np
from collections import defaultdict
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax

# =========================
# CONFIG
# =========================
DB_PATH = "twitter_data.db"
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment"

# =========================
# MODEL LOAD
# =========================
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()

# =========================
# SENTIMENT FUNCTION
# =========================
def roberta_sentiment(text):
    encoded = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512
    )

    with torch.no_grad():
        output = model(**encoded)

    scores = softmax(output.logits.numpy()[0])

    return np.array(scores)  # [neg, neu, pos]


# =========================
# MAIN
# =========================
def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT parent_tweet_id, text
        FROM replies
    """)

    rows = cur.fetchall()
    conn.close()

    tweet_sentiments = defaultdict(list)

    for tweet_id, text in rows:
        tweet_sentiments[tweet_id].append(roberta_sentiment(text))

    mean_sentiment = {
        tweet_id: scores.mean(axis=0)
        for tweet_id, scores in tweet_sentiments.items()
    }

    for tweet_id, scores in list(mean_sentiment.items())[:5]:
        print(
            tweet_id,
            {
                "negative": scores[0],
                "neutral": scores[1],
                "positive": scores[2]
            }
        )


if __name__ == "__main__":
    main()
