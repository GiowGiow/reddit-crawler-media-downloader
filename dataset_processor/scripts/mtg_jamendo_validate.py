#!/usr/bin/env python3
"""
mtgjamendo_genre_tagger_comparison.py

Extracts Essentia embeddings using multiple model configurations,
predicts genres for MTGJamendo dataset files,
compares predictions with ground truth tags, and reports comparative metrics.
Processes a maximum of 100 files.
"""

import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # Suppress TensorFlow logging
import json
import pandas as pd
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm
from collections import namedtuple
from essentia.standard import (
    MonoLoader,
    TensorflowPredictEffnetDiscogs,
    TensorflowPredict2D,
)
import requests
from sklearn.metrics import precision_recall_fscore_support

# --- CONFIGURAÇÕES GLOBAIS ---
DATASET_DIRECTORY = Path(
    "/media/giovanni/Giow Files/humaness_percept/datasets/mtg-jamendo-dataset/mtg-jamendo-raw_30s"
)
METADATA_PATH = Path("metadata/raw_30s_cleantags.tsv")
MODEL_CACHE_DIR = Path("models_cache")  # Diretório para salvar os modelos baixados

# Configuração dos modelos a serem testados
ALL_MODELS_CONFIG = [
    {
        "name": "discogs-effnet",
        "embedding_file": "discogs-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.json",
    },
    {
        "name": "discogs_artist",
        "embedding_file": "discogs_artist_embeddings-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs_artist_embeddings-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs_artist_embeddings-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_artist_embeddings-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs_artist_embeddings-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_artist_embeddings-effnet-1.json",
    },
    {
        "name": "discogs_label",
        "embedding_file": "discogs_label_embeddings-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs_label_embeddings-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs_label_embeddings-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_label_embeddings-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs_label_embeddings-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_label_embeddings-effnet-1.json",
    },
    {
        "name": "discogs_multi",
        "embedding_file": "discogs_multi_embeddings-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs_multi_embeddings-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs_multi_embeddings-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_multi_embeddings-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs_multi_embeddings-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_multi_embeddings-effnet-1.json",
    },
    {
        "name": "discogs_release",
        "embedding_file": "discogs_release_embeddings-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs_release_embeddings-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs_release_embeddings-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_release_embeddings-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs_release_embeddings-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_release_embeddings-effnet-1.json",
    },
    {
        "name": "discogs_track",
        "embedding_file": "discogs_track_embeddings-effnet-bs64-1.pb",
        "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs_track_embeddings-effnet-bs64-1.pb",  # CORRIGIDO
        "genre_file": "mtg_jamendo_genre-discogs_track_embeddings-effnet-1.pb",
        "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_track_embeddings-effnet-1.pb",
        "json_file": "mtg_jamendo_genre-discogs_track_embeddings-effnet-1.json",
        "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs_track_embeddings-effnet-1.json",
    },
]

# Parâmetros de processamento de áudio
DEFAULT_SAMPLE_RATE = 16000
GENRE_SCORE_THRESHOLD = 0.5
TOP_N_GENRES = 3
MAX_FILES_TO_PROCESS = 2000

GenreScore = namedtuple("GenreScore", ["label", "score"])

# --- FUNÇÕES ---


def ensure_model_file_exist(model_path: Path, model_url: str) -> None:
    """Download a model file if it doesn't exist locally."""
    if not model_path.exists():
        print(f"Downloading {model_path.name} from {model_url}…")
        MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        response = requests.get(model_url, stream=True)
        response.raise_for_status()  # Levanta um erro para códigos HTTP ruins (4xx ou 5xx)
        with open(model_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {model_path.name}")
    # else: # Opcional: descomente para verbosidade
    # print(f"Model file {model_path.name} already exists.")


def load_genre_labels(json_path: Path) -> list[str]:
    """Load genre labels from the JSON model metadata."""
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
    scores_with_labels = [
        GenreScore(label=genre_labels[i], score=float(prediction_scores[0][i]))
        for i in range(len(genre_labels))
    ]
    genres_above_threshold = [
        gs.label for gs in scores_with_labels if gs.score > threshold
    ]
    top_sorted = sorted(scores_with_labels, key=lambda gs: gs.score, reverse=True)[
        :top_n
    ]
    top_labels = [gs.label for gs in top_sorted]
    top_scores = [gs.score for gs in top_sorted]
    return genres_above_threshold, top_labels, top_scores


def parse_tsv_with_variable_columns(file_path: Path) -> pd.DataFrame:
    rows = []
    with open(file_path, "r", encoding="utf-8") as f:
        next(f)  # Skip header
        for line in f:
            parts = line.strip().split("\t", 5)
            if len(parts) < 6:
                continue
            track_id, _, _, _, _, tags_field = parts
            genre_tags = [
                tag.replace("genre---", "")
                for tag in tags_field.split("\t")
                if tag.startswith("genre---")
            ]
            rows.append({"TRACK_ID": track_id, "ground_truth_genres": genre_tags})
    return pd.DataFrame(rows)


def find_audio_files(dataset_directory: Path) -> list[tuple[Path, str]]:
    audio_files = []
    print(f"Searching for audio files in: {dataset_directory}")
    if not dataset_directory.exists():
        print(f"ERROR: Dataset directory does not exist: {dataset_directory}")
        return audio_files

    for i in range(100):
        folder_name = f"{i:02d}"
        folder_path = dataset_directory / folder_name
        if folder_path.exists():
            for audio_file_path in folder_path.glob("*.mp3"):
                match = re.search(r"(\d+)", audio_file_path.stem)
                if match:
                    numeric_part = match.group(1)
                    track_id = f"track_{numeric_part.zfill(7)}"
                    audio_files.append((audio_file_path, track_id))

    if not audio_files:
        print(
            "No audio files found in standard 00-99 structure. Trying alternative recursive search..."
        )
        for audio_file_path in dataset_directory.glob("**/*.mp3"):
            match = re.search(r"(\d+)", audio_file_path.stem)
            if match:
                numeric_part = match.group(1)
                track_id = f"track_{numeric_part.zfill(7)}"
                audio_files.append((audio_file_path, track_id))
    return audio_files


def compare_genres(predicted_genres: list, ground_truth_genres: list) -> dict:
    if not predicted_genres:
        predicted_genres = []
    if not ground_truth_genres:
        ground_truth_genres = []
    true_positives = set(predicted_genres).intersection(set(ground_truth_genres))
    precision = len(true_positives) / len(predicted_genres) if predicted_genres else 0
    recall = (
        len(true_positives) / len(ground_truth_genres) if ground_truth_genres else 0
    )
    f1 = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0
    )
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "true_positives": list(true_positives),
        "false_positives": list(set(predicted_genres) - set(ground_truth_genres)),
        "false_negatives": list(set(ground_truth_genres) - set(predicted_genres)),
    }


def process_dataset(
    model_name_tag: str,
    audio_files: list[tuple[Path, str]],
    metadata_df: pd.DataFrame,
    embedding_extractor,
    genre_classifier,
    genre_labels: list[str],
) -> tuple[pd.DataFrame, dict]:
    loader = MonoLoader()
    rows = []
    metadata_lookup = metadata_df.set_index("TRACK_ID").to_dict(orient="index")
    all_true_genres_binary, all_pred_genres_binary = [], []

    desc = f"Processing for model {model_name_tag}"
    for audio_file, track_id in tqdm(audio_files, desc=desc, unit="file", leave=False):
        if track_id not in metadata_lookup:
            tqdm.write(
                f"Warning [{model_name_tag}]: Track {track_id} for {audio_file.name} not in metadata, skipping."
            )
            continue
        ground_truth_genres = metadata_lookup[track_id]["ground_truth_genres"]
        try:
            audio_signal = load_and_resample_audio(
                loader, audio_file, DEFAULT_SAMPLE_RATE
            )
            embedding_vector = embedding_extractor(audio_signal)
            genre_prediction_scores = genre_classifier(embedding_vector)
            predicted_genres, top_genres, top_scores = select_top_genres(
                genre_prediction_scores,
                genre_labels,
                GENRE_SCORE_THRESHOLD,
                TOP_N_GENRES,
            )
            comparison = compare_genres(predicted_genres, ground_truth_genres)
            true_binary_vector = [
                1 if genre in ground_truth_genres else 0 for genre in genre_labels
            ]
            pred_binary_vector = [
                1 if genre in predicted_genres else 0 for genre in genre_labels
            ]
            all_true_genres_binary.append(true_binary_vector)
            all_pred_genres_binary.append(pred_binary_vector)
            rows.append(
                {
                    "track_id": track_id,
                    "filename": audio_file.name,
                    "predicted_genres": predicted_genres,
                    f"top_{TOP_N_GENRES}_genres": top_genres,
                    "top_scores": top_scores,
                    "ground_truth_genres": ground_truth_genres,
                    **comparison,
                }
            )
        except Exception as e:
            tqdm.write(f"Error processing {audio_file.name} with {model_name_tag}: {e}")

    overall_metrics = {}
    if all_true_genres_binary and all_pred_genres_binary:
        y_true = np.array(all_true_genres_binary)
        y_pred = np.array(all_pred_genres_binary)

        p_s, r_s, f1_s, _ = precision_recall_fscore_support(
            y_true, y_pred, average="samples", zero_division=0
        )
        p_micro, r_micro, f1_micro, _ = precision_recall_fscore_support(
            y_true, y_pred, average="micro", zero_division=0
        )
        p_w, r_w, f1_w, _ = precision_recall_fscore_support(
            y_true, y_pred, average="weighted", zero_division=0
        )

        overall_metrics = {
            "samples_precision": p_s,
            "samples_recall": r_s,
            "samples_f1": f1_s,
            "micro_precision": p_micro,
            "micro_recall": r_micro,
            "micro_f1": f1_micro,
            "weighted_precision": p_w,
            "weighted_recall": r_w,
            "weighted_f1": f1_w,
        }
        print(
            f"\n--- Metrics for model: {model_name_tag} (Threshold: {GENRE_SCORE_THRESHOLD}) ---"
        )
        print(
            f"Overall 'samples' average  | P: {p_s:.3f} | R: {r_s:.3f} | F1: {f1_s:.3f}"
        )
        print(
            f"Overall 'micro' average    | P: {p_micro:.3f} | R: {r_micro:.3f} | F1: {f1_micro:.3f}"
        )
        print(
            f"Overall 'weighted' average | P: {p_w:.3f} | R: {r_w:.3f} | F1: {f1_w:.3f}"
        )

    return pd.DataFrame(rows), overall_metrics


# --- FUNÇÃO PRINCIPAL ---
def main():
    print(
        f"Starting MTG-Jamendo Genre Tagger Comparison (Max files: {MAX_FILES_TO_PROCESS}, Threshold: {GENRE_SCORE_THRESHOLD})"
    )
    MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    metadata_df = parse_tsv_with_variable_columns(METADATA_PATH)
    print(f"Loaded metadata with {len(metadata_df)} tracks.")

    all_audio_files_found = find_audio_files(DATASET_DIRECTORY)
    print(f"Found {len(all_audio_files_found)} audio files in total.")

    if not all_audio_files_found:
        print("No audio files found. Exiting.")
        return

    audio_files_to_process = (
        all_audio_files_found[:MAX_FILES_TO_PROCESS]
        if len(all_audio_files_found) > MAX_FILES_TO_PROCESS
        else all_audio_files_found
    )
    if len(all_audio_files_found) > MAX_FILES_TO_PROCESS:
        print(f"Limiting processing to the first {MAX_FILES_TO_PROCESS} files.")
    print(
        f"Will process {len(audio_files_to_process)} audio files for each model configuration."
    )

    comparative_results_list = []

    for model_config in ALL_MODELS_CONFIG:
        model_name = model_config["name"]
        print(f"\n===== Testing Model: {model_name} =====")

        current_embedding_model_path = MODEL_CACHE_DIR / model_config["embedding_file"]
        current_genre_model_path = MODEL_CACHE_DIR / model_config["genre_file"]
        current_json_path = MODEL_CACHE_DIR / model_config["json_file"]

        try:
            print(f"Ensuring embedding model: {current_embedding_model_path.name}")
            ensure_model_file_exist(
                current_embedding_model_path, model_config["embedding_url"]
            )
            print(f"Ensuring genre model: {current_genre_model_path.name}")
            ensure_model_file_exist(current_genre_model_path, model_config["genre_url"])
            print(f"Ensuring JSON metadata: {current_json_path.name}")
            ensure_model_file_exist(current_json_path, model_config["json_url"])
        except requests.exceptions.RequestException as e:
            print(f"Download error for {model_name}: {e}. Skipping this model.")
            comparative_results_list.append(
                {
                    "model_name": model_name,
                    "error": f"Download error: {type(e).__name__}",
                    "samples_f1": 0,
                    "micro_f1": 0,
                    "weighted_f1": 0,
                    "samples_precision": 0,
                    "samples_recall": 0,
                    "micro_precision": 0,
                    "micro_recall": 0,
                    "weighted_precision": 0,
                    "weighted_recall": 0,
                }
            )
            continue

        genre_labels = load_genre_labels(current_json_path)
        print(
            f"Loaded {len(genre_labels)} genre labels from {current_json_path.name} for {model_name}"
        )

        try:
            embedding_extractor = TensorflowPredictEffnetDiscogs(
                graphFilename=str(current_embedding_model_path),
                output="PartitionedCall:1",
            )
            genre_classifier = TensorflowPredict2D(
                graphFilename=str(current_genre_model_path), output="model/Sigmoid"
            )
        except Exception as e:
            print(f"Essentia/TF model loading error for {model_name}: {e}. Skipping.")
            comparative_results_list.append(
                {
                    "model_name": model_name,
                    "error": f"Model load error: {type(e).__name__}",
                    "samples_f1": 0,
                    "micro_f1": 0,
                    "weighted_f1": 0,
                    "samples_precision": 0,
                    "samples_recall": 0,
                    "micro_precision": 0,
                    "micro_recall": 0,
                    "weighted_precision": 0,
                    "weighted_recall": 0,
                }
            )
            continue

        predictions_df, overall_metrics = process_dataset(
            model_name,
            audio_files_to_process,
            metadata_df,
            embedding_extractor,
            genre_classifier,
            genre_labels,
        )

        if not predictions_df.empty:
            output_csv_name = (
                f"predictions_{model_name}_thresh{GENRE_SCORE_THRESHOLD}.csv"
            )
            predictions_df.to_csv(output_csv_name, index=False)
            print(f"Saved predictions for {model_name} to {output_csv_name}")
        else:
            print(f"No predictions generated for {model_name}.")
            if not overall_metrics:
                overall_metrics = {
                    "samples_precision": 0,
                    "samples_recall": 0,
                    "samples_f1": 0,
                    "micro_precision": 0,
                    "micro_recall": 0,
                    "micro_f1": 0,
                    "weighted_precision": 0,
                    "weighted_recall": 0,
                    "weighted_f1": 0,
                }

        result_summary = {"model_name": model_name, "error": None, **overall_metrics}
        comparative_results_list.append(result_summary)

    print("\n\n===== Comparative Results Summary =====")
    if comparative_results_list:
        summary_df = pd.DataFrame(comparative_results_list)
        cols_to_show = [
            "model_name",
            "samples_f1",
            "samples_precision",
            "samples_recall",
            "micro_f1",
            "micro_precision",
            "micro_recall",
            "weighted_f1",
            "weighted_precision",
            "weighted_recall",
            "error",
        ]
        for col in cols_to_show:
            if col not in summary_df.columns:
                summary_df[col] = (
                    0.0
                    if col not in ["model_name", "error"]
                    else (None if col == "error" else "N/A")
                )

        summary_df = summary_df[cols_to_show].set_index("model_name")
        print(summary_df.to_string(float_format="%.3f"))
        summary_csv_path = f"comparative_summary_thresh{GENRE_SCORE_THRESHOLD}.csv"
        summary_df.to_csv(summary_csv_path)
        print(f"\nSaved comparative summary to {summary_csv_path}")
    else:
        print("No results to summarize.")

    print("\nComparison finished.")


if __name__ == "__main__":
    main()
