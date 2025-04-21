#!/usr/bin/env python3
"""
suno_ai_reddit_dataset_genre_tagger.py

Extracts Essentia embeddings and tags each downloaded Suno AI Reddit song with its genres,
leveraging pandas for JSONL I/O and iteration.
"""

import json
import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

import requests
from essentia.standard import (
    MonoLoader,
    TensorflowPredictEffnetDiscogs,
    TensorflowPredict2D,
)

# Paths
DATASET_PATH = Path("../song_downloader/suno_ai_posts_with_downloads.jsonl")
SONGS_DIRECTORY = Path("../song_downloader/dataset/")

# Essentia model files and labels
EMBEDDING_MODEL_PATH = Path("discogs-effnet-bs64-1.pb")
GENRE_MODEL_PATH = Path("mtg_jamendo_genre-discogs-effnet-1.pb")
GENRE_JSON_PATH = Path("mtg_jamendo_genre-discogs-effnet-1.json")

models_to_urls = {
    EMBEDDING_MODEL_PATH: "https://essentia.upf.edu/models/music-style-classification/discogs-effnet/discogs-effnet-bs64-1.pb",
    GENRE_MODEL_PATH: "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.pb",
    GENRE_JSON_PATH: "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.json",
}

# Constants
DEFAULT_SAMPLE_RATE = 16000
GENRE_SCORE_THRESHOLD = 0.5
TOP_N_GENRES = 3

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Namedtuple for score-label pairs
genre_tuple = tuple


def ensure_models_exist(models_to_urls: dict[Path, str]) -> None:
    for path, url in models_to_urls.items():
        if not path.exists():
            logger.info(f"Downloading {path.name}…")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)


def load_genre_labels(json_path: Path) -> list[str]:
    with json_path.open("r") as f:
        return json.load(f)["classes"]


def load_and_resample_audio(loader: MonoLoader, audio_path: Path, sample_rate: int):
    loader.configure(
        filename=str(audio_path), sampleRate=sample_rate, resampleQuality=4
    )
    return loader()


def select_top_genres(
    predictions, genre_labels: list[str], threshold: float, top_n: int
):
    scores = predictions[0]
    scored = [
        genre_tuple((label, float(scores[i]))) for i, label in enumerate(genre_labels)
    ]
    all_genres = [label for label, score in scored if score > threshold]
    top_sorted = sorted(scored, key=lambda x: x[1], reverse=True)[:top_n]
    top_labels = [label for label, _ in top_sorted]
    top_scores = [score for _, score in top_sorted]
    return all_genres, top_labels, top_scores


def enrich_with_pandas(
    jsonl_path: Path,
    songs_dir: Path,
    embedding_extractor,
    genre_classifier,
    genre_labels: list[str],
) -> pd.DataFrame:
    # Load JSONL into DataFrame
    df = pd.read_json(jsonl_path, lines=True)
    # Prepare new columns
    df[f"predicted_genres"] = [[] for _ in range(len(df))]
    df[f"top_{TOP_N_GENRES}_genres"] = [[] for _ in range(len(df))]
    df[f"genre_scores"] = [[] for _ in range(len(df))]

    loader = MonoLoader()
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Tagging songs"):
        download_path = row.get("download_path")
        if not download_path:
            continue

        rel = Path(download_path)
        try:
            rel = rel.relative_to("dataset")
        except Exception:
            pass
        audio_file = songs_dir / rel
        if not audio_file.exists():
            logger.warning(f"Missing file, skipping: {audio_file}")
            continue

        signal = load_and_resample_audio(loader, audio_file, DEFAULT_SAMPLE_RATE)
        embed = embedding_extractor(signal)
        preds = genre_classifier(embed)

        all_genres, top_labels, top_scores = select_top_genres(
            preds, genre_labels, GENRE_SCORE_THRESHOLD, TOP_N_GENRES
        )

        df.at[idx, "predicted_genres"] = all_genres
        df.at[idx, f"top_{TOP_N_GENRES}_genres"] = top_labels
        df.at[idx, "genre_scores"] = top_scores

    return df


def main():
    ensure_models_exist(models_to_urls)

    embedding_extractor = TensorflowPredictEffnetDiscogs(
        graphFilename=str(EMBEDDING_MODEL_PATH), output="PartitionedCall:1"
    )
    genre_classifier = TensorflowPredict2D(
        graphFilename=str(GENRE_MODEL_PATH), output="model/Sigmoid"
    )

    genre_labels = load_genre_labels(GENRE_JSON_PATH)

    df_enriched = enrich_with_pandas(
        DATASET_PATH,
        SONGS_DIRECTORY,
        embedding_extractor,
        genre_classifier,
        genre_labels,
    )

    output_path = DATASET_PATH.with_name(DATASET_PATH.stem + "_with_genres.jsonl")
    df_enriched.to_json(output_path, orient="records", lines=True)
    logger.info(f"Saved enriched data to {output_path}")


if __name__ == "__main__":
    main()
