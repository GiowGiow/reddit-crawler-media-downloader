#!/usr/bin/env python3
"""
mvsep_genre_tagger.py

Extracts Essentia embeddings and tags each audio file in DATASET_DIRECTORY with its top genres.
"""

import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from collections import namedtuple
from essentia.standard import (
    MonoLoader,
    TensorflowPredictEffnetDiscogs,
    TensorflowPredict2D,
)
import requests

DATASET_DIRECTORY = Path("../datasets/mvsep_multisong_dataset")

EMBEDDING_MODEL_PATH = Path("discogs-effnet-bs64-1.pb")
GENRE_MODEL_PATH = Path("mtg_jamendo_genre-discogs-effnet-1.pb")
GENRE_JSON_PATH = Path("mtg_jamendo_genre-discogs-effnet-1.json")

models_to_urls = {
    EMBEDDING_MODEL_PATH: "https://essentia.upf.edu/models/music-style-classification/discogs-effnet/discogs-effnet-bs64-1.pb",
    GENRE_MODEL_PATH: "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.pb",
    GENRE_JSON_PATH: "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.json",
}

# Do not change the sample rate. Essentia’s models are trained on 16kHz audio.
DEFAULT_SAMPLE_RATE = 16000
GENRE_SCORE_THRESHOLD = 0.5
TOP_N_GENRES = 3

GenreScore = namedtuple("GenreScore", ["label", "score"])


def ensure_models_exist(models_to_urls: dict[Path, str]) -> None:
    for path, url in models_to_urls.items():
        if not path.exists():
            print(f"Downloading {path.name}…")
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
    prediction_scores, genre_labels: list[str], threshold: float, top_n: int
):
    # prediction_scores is shape (1, C) even for one embedding.
    # Essentia’s 2D classifier always returns a 2D array: outer list = batch size = 1;
    # inner list = C class probabilities. We grab [0] to get that single row.
    scores_with_labels = [
        GenreScore(label=genre_labels[i], score=float(prediction_scores[0][i]))
        for i in range(len(genre_labels))
    ]

    # Pick all genres whose score exceeds our threshold
    genres_above_threshold = [
        gs.label for gs in scores_with_labels if gs.score > threshold
    ]

    # Sort by score (the .score attribute of our namedtuple).
    # reverse=True puts highest scores first; then slice [:top_n] picks the top N.
    # top_sorted now holds the highest-scoring GenreScore tuples, e.g.:
    # [GenreScore(label="rock", score=0.82), GenreScore(label="jazz", score=0.76), …]
    top_sorted = sorted(scores_with_labels, key=lambda gs: gs.score, reverse=True)[
        :top_n
    ]

    # Unzip that list into two parallel lists: labels and numeric scores
    top_labels = [gs.label for gs in top_sorted]
    top_scores = [gs.score for gs in top_sorted]

    return genres_above_threshold, top_labels, top_scores


def process_dataset(
    directory: Path, embedding_extractor, genre_classifier, genre_labels: list[str]
) -> pd.DataFrame:
    loader = MonoLoader()
    rows = []

    for audio_file in tqdm(directory.iterdir(), desc="Tagging audio files"):
        if audio_file.suffix.lower() not in (".wav", ".mp3"):
            continue

        audio_signal = load_and_resample_audio(loader, audio_file, DEFAULT_SAMPLE_RATE)
        embedding_vector = embedding_extractor(audio_signal)
        genre_prediction = genre_classifier(embedding_vector)

        all_genres, top_genres, top_scores = select_top_genres(
            genre_prediction, genre_labels, GENRE_SCORE_THRESHOLD, TOP_N_GENRES
        )

        rows.append(
            {
                "filename": audio_file.name,
                "predicted_genres": all_genres,
                f"top_{TOP_N_GENRES}_genres": top_genres,
                "scores": top_scores,
            }
        )

    return pd.DataFrame(rows)


def main():
    ensure_models_exist(models_to_urls)

    embedding_extractor = TensorflowPredictEffnetDiscogs(
        graphFilename=str(EMBEDDING_MODEL_PATH), output="PartitionedCall:1"
    )
    genre_classifier = TensorflowPredict2D(
        graphFilename=str(GENRE_MODEL_PATH), output="model/Sigmoid"
    )

    genre_labels = load_genre_labels(GENRE_JSON_PATH)
    predictions_df = process_dataset(
        DATASET_DIRECTORY, embedding_extractor, genre_classifier, genre_labels
    )

    output_csv = "mvsep_genre_predictions.csv"
    predictions_df.to_csv(output_csv, index=False)
    print(f"Saved predictions to {output_csv}")
    print(predictions_df.head())


if __name__ == "__main__":
    main()
