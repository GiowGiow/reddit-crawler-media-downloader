# Import necessary libraries
import json
import logging
import re
from collections import Counter
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

logger = logging.getLogger(__name__)

# Load the data from the file
data = []
try:
    with open("paste.txt", "r") as file:
        for line in file:
            if line.strip():  # Skip empty lines
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    # Skip lines that aren't valid JSON
                    continue
except FileNotFoundError:
    logger.info(
        "File 'paste.txt' not found. Please make sure the file exists in the current directory."
    )

# Convert to DataFrame
df = pd.DataFrame(data)

# Display basic info
logger.info(f"Total number of posts: {len(df)}")
logger.info(f"Columns in the dataset: {df.columns.tolist()}")
df.head()


# Function to extract tags from title
def extract_tags(title):
    if isinstance(title, str):
        # Look for [tag] at the beginning of the title
        match = re.match(r"^\[(.*?)\]", title)
        if match:
            tag_text = match.group(1).strip()
            # Split multi-tags if separated by commas or slashes
            if "," in tag_text or "/" in tag_text:
                tags = re.split(r"[,/]", tag_text)
                return [tag.strip().lower() for tag in tags]
            else:
                return [tag_text.lower()]
    return []


# Apply the function to create a new column with lists of tags
df["tags"] = df["title"].apply(extract_tags)

# Create a column with the primary tag (first tag)
df["primary_tag"] = df["tags"].apply(lambda x: x[0] if x else None)

# Filter posts with tags
tagged_posts = df[df["tags"].apply(len) > 0].copy()
logger.info(f"Number of posts with tags: {len(tagged_posts)}")

# Get all tags (flattened)
all_tags = [tag for tags_list in tagged_posts["tags"] for tag in tags_list]
tag_counts = Counter(all_tags)

logger.info("\nTop 20 tags:")
for tag, count in tag_counts.most_common(20):
    logger.info(f"{tag}: {count}")

# Plot top 20 tags
plt.figure(figsize=(14, 8))
top_tags_df = pd.DataFrame(tag_counts.most_common(20), columns=["tag", "count"])
sns.barplot(y="tag", x="count", data=top_tags_df)
plt.title("Top 20 Tags in Post Titles")
plt.xlabel("Count")
plt.ylabel("Tag")
plt.tight_layout()
plt.show()

# Get tag distribution
tag_distribution = tagged_posts["primary_tag"].value_counts()
total_tags = tag_distribution.sum()

# Calculate percentage for each tag
tag_percentage = (tag_distribution / total_tags * 100).round(2)

# Display top tags with their percentage
logger.info("\nTop 10 primary tags by percentage:")
for tag, pct in tag_percentage.head(10).items():
    logger.info(f"{tag}: {pct}%")

# Create a pie chart for the top 10 tags
plt.figure(figsize=(10, 10))
plt.pie(
    tag_percentage.head(10),
    labels=tag_percentage.head(10).index,
    autopct="%1.1f%%",
    shadow=True,
    startangle=140,
)
plt.axis("equal")  # Equal aspect ratio ensures that pie is drawn as a circle
plt.title("Distribution of Top 10 Primary Tags")
plt.tight_layout()
plt.show()

# Analyze metrics by primary tag (if we have the needed columns)
if all(col in df.columns for col in ["score", "num_comments", "upvote_ratio"]):
    # Analyze metrics by primary tag
    primary_tag_metrics = (
        tagged_posts.groupby("primary_tag")
        .agg(
            {
                "score": ["mean", "median", "count"],
                "num_comments": ["mean", "median"],
                "upvote_ratio": ["mean", "median"],
            }
        )
        .reset_index()
    )

    # Flatten multi-level columns
    primary_tag_metrics.columns = [
        "_".join(col).strip("_") for col in primary_tag_metrics.columns.values
    ]

    # Sort by count
    primary_tag_metrics_sorted = primary_tag_metrics.sort_values(
        "score_count", ascending=False
    )

    # Display top primary tags by count
    logger.info("\nTop primary tags by post count with metrics:")
    logger.info(primary_tag_metrics_sorted.head(10))

    # Plot average score by primary tag (top 10)
    plt.figure(figsize=(14, 8))
    top_primary_tags = primary_tag_metrics_sorted.head(10)
    sns.barplot(y="primary_tag", x="score_mean", data=top_primary_tags)
    plt.title("Average Score by Primary Tag (Top 10)")
    plt.xlabel("Average Score")
    plt.ylabel("Primary Tag")
    plt.tight_layout()
    plt.show()


# Process title text (excluding tags)
def remove_tag(title):
    if isinstance(title, str):
        return re.sub(r"^\[.*?\]\s*", "", title)
    return title


tagged_posts["title_no_tag"] = tagged_posts["title"].apply(remove_tag)

# Get word frequencies
all_words = " ".join(tagged_posts["title_no_tag"].dropna()).lower()
all_words = re.sub(r"[^\w\s]", "", all_words)  # Remove punctuation
word_counts = Counter(all_words.split())

# Remove common English words (stopwords)
stopwords = [
    "the",
    "and",
    "to",
    "of",
    "a",
    "in",
    "for",
    "is",
    "on",
    "that",
    "by",
    "this",
    "with",
    "i",
    "you",
    "it",
    "be",
    "are",
]
for word in stopwords:
    if word in word_counts:
        del word_counts[word]

# Plot word frequencies
plt.figure(figsize=(14, 8))
words_df = pd.DataFrame(word_counts.most_common(20), columns=["word", "count"])
sns.barplot(y="word", x="count", data=words_df)
plt.title("Most Common Words in Post Titles (Excluding Tags)")
plt.xlabel("Count")
plt.ylabel("Word")
plt.tight_layout()
plt.show()

# Analyze tag co-occurrences
tag_pairs = []
for tags_list in tagged_posts["tags"]:
    if len(tags_list) > 1:
        # Create all pairs of tags
        for i in range(len(tags_list)):
            for j in range(i + 1, len(tags_list)):
                # Sort to ensure consistent ordering
                tag_pair = tuple(sorted([tags_list[i], tags_list[j]]))
                tag_pairs.append(tag_pair)

# Count tag co-occurrences
tag_pair_counts = Counter(tag_pairs)

if tag_pairs:
    # Display top co-occurring tag pairs
    logger.info("\nTop 20 co-occurring tag pairs:")
    for pair, count in tag_pair_counts.most_common(20):
        logger.info(f"{pair[0]} & {pair[1]}: {count}")

    # Visualize tag co-occurrences
    top_pairs = pd.DataFrame(tag_pair_counts.most_common(15), columns=["pair", "count"])
    top_pairs["tag_pair"] = top_pairs["pair"].apply(lambda x: f"{x[0]} & {x[1]}")

    plt.figure(figsize=(14, 8))
    sns.barplot(y="tag_pair", x="count", data=top_pairs)
    plt.title("Top 15 Co-occurring Tag Pairs")
    plt.xlabel("Count")
    plt.ylabel("Tag Pair")
    plt.tight_layout()
    plt.show()

# Check posting time patterns if created_utc exists
if "created_utc" in df.columns:
    # Convert created_utc to datetime
    tagged_posts["created_date"] = pd.to_datetime(tagged_posts["created_utc"], unit="s")

    # Extract day of week and hour
    tagged_posts["day_of_week"] = tagged_posts["created_date"].dt.day_name()
    tagged_posts["hour"] = tagged_posts["created_date"].dt.hour

    # Plot posts by day of week
    plt.figure(figsize=(10, 6))
    day_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    day_counts = tagged_posts["day_of_week"].value_counts().reindex(day_order)
    day_counts.plot(kind="bar")
    plt.title("Posts by Day of Week")
    plt.xlabel("Day of Week")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    plt.show()

    # Plot posts by hour
    plt.figure(figsize=(12, 6))
    hour_counts = tagged_posts["hour"].value_counts().sort_index()
    hour_counts.plot(kind="bar")
    plt.title("Posts by Hour of Day")
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Posts")
    plt.xticks(range(0, 24, 2))  # Show every 2 hours for readability
    plt.tight_layout()
    plt.show()

# Check domain analysis
if "domain" in df.columns:
    # Get domain distribution for tagged posts
    domain_counts = tagged_posts["domain"].value_counts().head(15)

    plt.figure(figsize=(14, 8))
    domain_counts.plot(kind="bar")
    plt.title("Top 15 Domains for Tagged Posts")
    plt.xlabel("Domain")
    plt.ylabel("Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

# Additional analysis: Check if certain tags are more common in certain post types
if "link_flair_text" in df.columns:
    # Get flair distribution
    flair_counts = tagged_posts["link_flair_text"].value_counts()

    plt.figure(figsize=(12, 6))
    flair_counts.head(10).plot(kind="bar")
    plt.title("Top 10 Flairs for Tagged Posts")
    plt.xlabel("Flair")
    plt.ylabel("Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

    # Look at primary tag distribution for top flairs
    top_flairs = flair_counts.head(5).index.tolist()

    plt.figure(figsize=(16, 12))
    for i, flair in enumerate(top_flairs):
        flair_data = tagged_posts[tagged_posts["link_flair_text"] == flair]
        tag_dist = flair_data["primary_tag"].value_counts().head(5)

        plt.subplot(len(top_flairs), 1, i + 1)
        tag_dist.plot(kind="bar")
        plt.title(f"Top 5 Tags for Flair: {flair}")
        plt.xlabel("Primary Tag")
        plt.ylabel("Count")
        plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.show()

# Summary of findings
logger.info("\n==== Summary of Findings ====")
logger.info(f"Total posts analyzed: {len(df)}")
logger.info(
    f"Posts with tags: {len(tagged_posts)} ({len(tagged_posts)/len(df)*100:.2f}%)"
)
logger.info(f"Unique tags found: {len(tag_counts)}")
if tag_counts:
    logger.info(
        f"Most common tag: {tag_counts.most_common(1)[0][0]} (appears {tag_counts.most_common(1)[0][1]} times)"
    )

# If we have enough data, logger.info more summary stats
if all(col in df.columns for col in ["score", "num_comments", "upvote_ratio"]):
    # Top performing tags by average score
    top_by_score = primary_tag_metrics_sorted.sort_values(
        "score_mean", ascending=False
    ).head(3)
    logger.info("\nTop 3 tags by average score:")
    for _, row in top_by_score.iterrows():
        logger.info(
            f"{row['primary_tag']}: {row['score_mean']:.2f} (appears in {row['score_count']} posts)"
        )

# Common tag co-occurrences summary
if tag_pairs:
    logger.info("\nMost common tag combination:")
    most_common_pair = tag_pair_counts.most_common(1)[0]
    logger.info(
        f"{most_common_pair[0][0]} & {most_common_pair[0][1]}: appears {most_common_pair[1]} times"
    )
