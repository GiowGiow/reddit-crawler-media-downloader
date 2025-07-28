#!/usr/bin/env python3
"""
mtg_jamendo_genre_tagger.py

Extracts Essentia embeddings using discogs-effnet model,
predicts genres for MTG-Jamendo dataset files,
and exports a new CSV file with predicted genres added.
Supports checkpointing and resuming.
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
import pickle
from datetime import datetime
import time

# --- GLOBAL CONFIGURATIONS ---
DATASET_DIRECTORY = Path(
    "/media/giovanni/Giow Files/humaness_percept/datasets/mtg-jamendo-dataset/mtg-jamendo-raw_30s"
)
METADATA_PATH = Path("metadata/raw_30s_cleantags.tsv")
OUTPUT_CSV_PATH = Path("mtg_jamendo_with_genres.csv")
MODEL_CACHE_DIR = Path("models_cache")
CHECKPOINT_DIR = Path("checkpoints")
CHECKPOINT_FILE = CHECKPOINT_DIR / "mtg_jamendo_progress.pkl"

# Model configuration for discogs-effnet
MODEL_CONFIG = {
    "name": "discogs-effnet",
    "embedding_file": "discogs-effnet-bs64-1.pb",
    "embedding_url": "https://essentia.upf.edu/models/feature-extractors/discogs-effnet/discogs-effnet-bs64-1.pb",
    "genre_file": "mtg_jamendo_genre-discogs-effnet-1.pb",
    "genre_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.pb",
    "json_file": "mtg_jamendo_genre-discogs-effnet-1.json",
    "json_url": "https://essentia.upf.edu/models/classification-heads/mtg_jamendo_genre/mtg_jamendo_genre-discogs-effnet-1.json",
}

# Audio processing parameters
DEFAULT_SAMPLE_RATE = 16000
GENRE_SCORE_THRESHOLD = 0.5
TOP_N_GENRES = 3

# Checkpoint parameters
CHECKPOINT_INTERVAL = 100  # Save checkpoint every N files
PROGRESS_UPDATE_INTERVAL = 10  # Update speed estimate every N files

GenreScore = namedtuple("GenreScore", ["label", "score"])

# --- FUNCTIONS ---


def ensure_model_file_exists(model_path: Path, model_url: str) -> None:
    """Download a model file if it doesn't exist locally."""
    if not model_path.exists():
        print(f"Downloading {model_path.name} from {model_url}...")
        MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        response = requests.get(model_url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))

        with open(model_path, "wb") as f:
            with tqdm(
                total=total_size, unit="B", unit_scale=True, desc=model_path.name
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))
        print(f"Successfully downloaded {model_path.name}")


def load_genre_labels(json_path: Path) -> list[str]:
    """Load genre labels from the JSON model metadata."""
    with json_path.open("r") as f:
        return json.load(f)["classes"]


def load_and_resample_audio(loader: MonoLoader, audio_path: Path, sample_rate: int):
    """Load and resample audio file."""
    loader.configure(
        filename=str(audio_path), sampleRate=sample_rate, resampleQuality=4
    )
    return loader()


def select_top_genres(
    prediction_scores, genre_labels: list[str], threshold: float, top_n: int
):
    """Select genres above threshold and top N genres by score."""
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


def parse_tsv_metadata(file_path: Path) -> pd.DataFrame:
    """Parse the TSV file with variable columns and extract relevant information."""
    rows = []
    with open(file_path, "r", encoding="utf-8") as f:
        # Read header
        header = next(f).strip().split("\t")

        for line in f:
            parts = line.strip().split("\t", 5)
            if len(parts) < 6:
                continue

            track_id, artist_id, album_id, path, duration, tags_field = parts

            # Extract genre tags
            genre_tags = [
                tag.replace("genre---", "")
                for tag in tags_field.split("\t")
                if tag.startswith("genre---")
            ]

            # Extract all tags for preservation
            all_tags = tags_field.split("\t")

            rows.append(
                {
                    "TRACK_ID": track_id,
                    "ARTIST_ID": artist_id,
                    "ALBUM_ID": album_id,
                    "PATH": path,
                    "DURATION": duration,
                    "ground_truth_genres": genre_tags,
                    "all_tags": all_tags,
                }
            )

    return pd.DataFrame(rows)


def find_audio_files(dataset_directory: Path) -> list[tuple[Path, str]]:
    """Find all audio files and extract their track IDs."""
    audio_files = []
    print(f"Searching for audio files in: {dataset_directory}")

    if not dataset_directory.exists():
        print(f"ERROR: Dataset directory does not exist: {dataset_directory}")
        return audio_files

    # Search in standard 00-99 folder structure
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

    # If no files found, try recursive search
    if not audio_files:
        print("No audio files found in standard structure. Trying recursive search...")
        for audio_file_path in dataset_directory.glob("**/*.mp3"):
            match = re.search(r"(\d+)", audio_file_path.stem)
            if match:
                numeric_part = match.group(1)
                track_id = f"track_{numeric_part.zfill(7)}"
                audio_files.append((audio_file_path, track_id))

    return audio_files


def save_checkpoint(processed_tracks: dict, checkpoint_path: Path):
    """Save checkpoint with processed tracks."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = checkpoint_path.with_suffix(".tmp")
    with open(temp_path, "wb") as f:
        pickle.dump(
            {
                "processed_tracks": processed_tracks,
                "timestamp": datetime.now(),
                "total_processed": len(processed_tracks),
            },
            f,
        )
    # Atomic rename to avoid corruption
    temp_path.replace(checkpoint_path)


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load checkpoint if it exists."""
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "rb") as f:
                checkpoint = pickle.load(f)
                print(f"Loaded checkpoint from {checkpoint['timestamp']}")
                print(
                    f"Total tracks already processed: {checkpoint['total_processed']}"
                )
                return checkpoint["processed_tracks"]
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
            return {}
    return {}


def process_dataset(
    audio_files: list[tuple[Path, str]],
    metadata_df: pd.DataFrame,
    embedding_extractor,
    genre_classifier,
    genre_labels: list[str],
    resume_from_checkpoint: bool = True,
) -> pd.DataFrame:
    """Process all audio files and add predicted genres to metadata."""
    loader = MonoLoader()

    # Create a copy of metadata to add predictions
    result_df = metadata_df.copy()

    # Initialize new columns with proper types
    result_df["predicted_genres"] = [[] for _ in range(len(result_df))]
    result_df[f"top_{TOP_N_GENRES}_genres"] = [[] for _ in range(len(result_df))]
    result_df["top_scores"] = [[] for _ in range(len(result_df))]
    result_df["audio_file_found"] = False

    # Create lookup dictionary for faster processing
    track_to_index = {row["TRACK_ID"]: idx for idx, row in result_df.iterrows()}

    # Load checkpoint if resuming
    processed_tracks = {}
    if resume_from_checkpoint:
        processed_tracks = load_checkpoint(CHECKPOINT_FILE)
        print(
            f"Resuming from checkpoint: {len(processed_tracks)} tracks already processed"
        )

    # Apply existing checkpoint data
    for track_id, data in processed_tracks.items():
        if track_id in track_to_index:
            idx = track_to_index[track_id]
            result_df.at[idx, "predicted_genres"] = data["predicted_genres"]
            result_df.at[idx, f"top_{TOP_N_GENRES}_genres"] = data["top_genres"]
            result_df.at[idx, "top_scores"] = data["top_scores"]
            result_df.at[idx, "audio_file_found"] = True

    # Filter out already processed files
    files_to_process = [
        (path, track_id)
        for path, track_id in audio_files
        if track_id not in processed_tracks
    ]

    if not files_to_process:
        print("All files already processed!")
        return result_df

    print(f"Files to process: {len(files_to_process)}")

    # Process each audio file
    start_time = time.time()
    files_processed_in_session = 0

    with tqdm(
        total=len(files_to_process), desc="Processing audio files", unit="file"
    ) as pbar:
        for i, (audio_file, track_id) in enumerate(files_to_process):
            if track_id not in track_to_index:
                pbar.write(f"Warning: Track {track_id} not in metadata, skipping.")
                pbar.update(1)
                continue

            try:
                # Load and process audio
                audio_signal = load_and_resample_audio(
                    loader, audio_file, DEFAULT_SAMPLE_RATE
                )

                # Extract embedding and predict genres
                embedding_vector = embedding_extractor(audio_signal)
                genre_prediction_scores = genre_classifier(embedding_vector)

                # Select genres
                predicted_genres, top_genres, top_scores = select_top_genres(
                    genre_prediction_scores,
                    genre_labels,
                    GENRE_SCORE_THRESHOLD,
                    TOP_N_GENRES,
                )

                # Update the dataframe using the index
                idx = track_to_index[track_id]
                result_df.at[idx, "predicted_genres"] = predicted_genres
                result_df.at[idx, f"top_{TOP_N_GENRES}_genres"] = top_genres
                result_df.at[idx, "top_scores"] = top_scores
                result_df.at[idx, "audio_file_found"] = True

                # Track processed files
                processed_tracks[track_id] = {
                    "predicted_genres": predicted_genres,
                    "top_genres": top_genres,
                    "top_scores": top_scores,
                }

                files_processed_in_session += 1

                # Update progress bar with speed estimate
                if files_processed_in_session % PROGRESS_UPDATE_INTERVAL == 0:
                    elapsed = time.time() - start_time
                    speed = files_processed_in_session / elapsed
                    remaining = len(files_to_process) - files_processed_in_session
                    eta = remaining / speed if speed > 0 else 0
                    pbar.set_postfix(
                        {
                            "speed": f"{speed:.1f} files/s",
                            "eta": (
                                f"{eta/3600:.1f}h" if eta > 3600 else f"{eta/60:.1f}m"
                            ),
                        }
                    )

            except Exception as e:
                pbar.write(f"Error processing {audio_file.name}: {e}")

            pbar.update(1)

            # Save checkpoint periodically
            if len(processed_tracks) % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(processed_tracks, CHECKPOINT_FILE)
                pbar.write(
                    f"Checkpoint saved ({len(processed_tracks)} total tracks processed)"
                )

    # Final checkpoint save
    save_checkpoint(processed_tracks, CHECKPOINT_FILE)
    print(f"Final checkpoint saved ({len(processed_tracks)} total tracks processed)")

    return result_df


def format_output_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Format the output dataframe for better readability."""
    # Convert list columns to string representations
    df["predicted_genres_str"] = df["predicted_genres"].apply(
        lambda x: "|".join(x) if isinstance(x, list) and len(x) > 0 else ""
    )
    df[f"top_{TOP_N_GENRES}_genres_str"] = df[f"top_{TOP_N_GENRES}_genres"].apply(
        lambda x: "|".join(x) if isinstance(x, list) and len(x) > 0 else ""
    )
    df["top_scores_str"] = df["top_scores"].apply(
        lambda x: (
            "|".join([f"{s:.3f}" for s in x])
            if isinstance(x, list) and len(x) > 0
            else ""
        )
    )
    df["ground_truth_genres_str"] = df["ground_truth_genres"].apply(
        lambda x: "|".join(x) if isinstance(x, list) and len(x) > 0 else ""
    )

    # Select columns for output
    output_columns = [
        "TRACK_ID",
        "ARTIST_ID",
        "ALBUM_ID",
        "PATH",
        "DURATION",
        "ground_truth_genres_str",
        "predicted_genres_str",
        f"top_{TOP_N_GENRES}_genres_str",
        "top_scores_str",
        "audio_file_found",
    ]

    return df[output_columns]


# --- MAIN FUNCTION ---
def main():
    print(f"Starting MTG-Jamendo Genre Tagger (Threshold: {GENRE_SCORE_THRESHOLD})")
    print(f"Using discogs-effnet model")
    print(f"Processing mode: Sequential with checkpointing")

    # Create directories
    MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    # Load metadata
    print(f"\nLoading metadata from {METADATA_PATH}")
    metadata_df = parse_tsv_metadata(METADATA_PATH)
    print(f"Loaded metadata with {len(metadata_df)} tracks.")

    # Find audio files
    audio_files = find_audio_files(DATASET_DIRECTORY)
    print(f"Found {len(audio_files)} audio files.")

    if not audio_files:
        print("No audio files found. Exiting.")
        return

    # Download models if needed
    embedding_model_path = MODEL_CACHE_DIR / MODEL_CONFIG["embedding_file"]
    genre_model_path = MODEL_CACHE_DIR / MODEL_CONFIG["genre_file"]
    json_path = MODEL_CACHE_DIR / MODEL_CONFIG["json_file"]

    print("\nEnsuring model files exist...")
    ensure_model_file_exists(embedding_model_path, MODEL_CONFIG["embedding_url"])
    ensure_model_file_exists(genre_model_path, MODEL_CONFIG["genre_url"])
    ensure_model_file_exists(json_path, MODEL_CONFIG["json_url"])

    # Load genre labels
    genre_labels = load_genre_labels(json_path)
    print(f"Loaded {len(genre_labels)} genre labels")

    # Load models
    print("\nLoading Essentia models...")
    embedding_extractor = TensorflowPredictEffnetDiscogs(
        graphFilename=str(embedding_model_path),
        output="PartitionedCall:1",
    )
    genre_classifier = TensorflowPredict2D(
        graphFilename=str(genre_model_path), output="model/Sigmoid"
    )
    print("Models loaded successfully!")

    # Check for existing checkpoint
    resume = True
    if CHECKPOINT_FILE.exists():
        resume_input = (
            input("\nFound existing checkpoint. Resume from checkpoint? (Y/n): ")
            .strip()
            .lower()
        )
        resume = resume_input != "n"

    # Process dataset
    print("\nProcessing audio files and predicting genres...")
    result_df = process_dataset(
        audio_files,
        metadata_df,
        embedding_extractor,
        genre_classifier,
        genre_labels,
        resume_from_checkpoint=resume,
    )

    # Format and save output
    print("\nFormatting and saving results...")
    output_df = format_output_dataframe(result_df)
    output_df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Saved predictions to {OUTPUT_CSV_PATH}")

    # Print summary statistics
    tracks_with_audio = output_df["audio_file_found"].sum()
    print(f"\nSummary:")
    print(f"- Total tracks in metadata: {len(output_df)}")
    print(f"- Tracks with audio files found: {tracks_with_audio}")
    print(
        f"- Tracks with predictions: {output_df['predicted_genres_str'].str.len().gt(0).sum()}"
    )

    # Show sample results
    print("\nSample results (first 5 tracks with predictions):")
    sample_df = output_df[output_df["predicted_genres_str"].str.len() > 0].head()
    if not sample_df.empty:
        print(
            sample_df[
                ["TRACK_ID", "ground_truth_genres_str", "predicted_genres_str"]
            ].to_string()
        )

    # Ask about checkpoint cleanup
    if CHECKPOINT_FILE.exists() and tracks_with_audio == len(audio_files):
        cleanup = (
            input("\nAll files processed successfully. Remove checkpoint file? (Y/n): ")
            .strip()
            .lower()
        )
        if cleanup != "n":
            CHECKPOINT_FILE.unlink()
            print("Checkpoint file removed.")


if __name__ == "__main__":
    main()
