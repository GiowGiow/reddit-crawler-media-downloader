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


def main():
    ensure_models_exist(models_to_urls)
    genre_labels = load_genre_labels(GENRE_JSON_PATH)

    audio_file = Path(
        "/media/giovanni/Giow Files/humaness_percept/datasets/mtg-jamendo-dataset/mtg-jamendo-raw_30s/00/903600.mp3"
    )

    # Load audio with MonoLoader
    loader = MonoLoader(filename=str(audio_file), sampleRate=DEFAULT_SAMPLE_RATE)
    audio_signal = loader()

    print(f"Audio signal type: {type(audio_signal)}")
    print(f"Audio signal length: {len(audio_signal)}")

    try:
        # Step 1: Extract embeddings using TensorflowPredictEffnetDiscogs
        # According to the JSON, this is the correct algorithm for the embedding model
        from essentia.standard import TensorflowPredictEffnetDiscogs

        # Create the embedding extractor with correct parameters
        # Based on the error message, lastLayer is not a parameter
        embedding_extractor = TensorflowPredictEffnetDiscogs(
            graphFilename=str(EMBEDDING_MODEL_PATH),
            output="PartitionedCall:1",  # From your original code
        )

        # Extract embeddings
        embedding_vector = embedding_extractor(audio_signal)

        print("Successfully extracted embeddings!")
        print(f"Embedding vector type: {type(embedding_vector)}")
        if hasattr(embedding_vector, "shape"):
            print(f"Embedding vector shape: {embedding_vector.shape}")
        else:
            print(f"Embedding vector length: {len(embedding_vector)}")

        # Step 2: Classify the embeddings using TensorflowPredict2D
        # According to the JSON, this is the correct algorithm for the classification
        from essentia.standard import TensorflowPredict2D

        # Create the genre classifier with correct parameters
        genre_classifier = TensorflowPredict2D(
            graphFilename=str(GENRE_MODEL_PATH),
            input="model/Placeholder",  # From the JSON schema
            output="model/Sigmoid",  # From the JSON schema
        )

        # Classify the embedding
        genre_prediction = genre_classifier(embedding_vector)

        # Process the genre prediction
        genres_above_threshold, top_labels, top_scores = select_top_genres(
            genre_prediction, genre_labels, GENRE_SCORE_THRESHOLD, TOP_N_GENRES
        )

        print(f"Genres: {genres_above_threshold}")
        print(f"Top genres: {top_labels}")
        print(f"Scores: {top_scores}")

    except Exception as e:
        print(f"Primary approach failed.")
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")

        # If the primary approach fails, try an alternative using TensorflowPredict
        try:
            print("\nTrying alternative approach with TensorflowPredict...")

            from essentia.standard import TensorflowPredict

            # First, extract the embedding using TensorflowPredict
            embedding_extractor = TensorflowPredict(
                graphFilename=str(EMBEDDING_MODEL_PATH),
                input="serving_default_melspectrogram",  # From error messages earlier
                output="PartitionedCall",  # From error messages earlier
            )

            # We need to convert the audio to a melspectrogram
            from essentia.standard import MelSpectrogram

            # Create melspectrogram
            mel_spec = MelSpectrogram(
                sampleRate=DEFAULT_SAMPLE_RATE,
                numberBands=96,  # Common value for audio models
                lowFrequencyBound=0,
                highFrequencyBound=DEFAULT_SAMPLE_RATE // 2,
            )

            # Compute melspectrogram
            mel_bands = mel_spec(audio_signal)

            # Reshape for the model
            import numpy as np

            mel_bands_reshaped = np.expand_dims(mel_bands.T, 0)

            # Extract embeddings
            embedding_vector = embedding_extractor(mel_bands_reshaped)

            print("Successfully extracted embeddings with alternative approach!")

            # Now classify the embedding
            genre_classifier = TensorflowPredict(
                graphFilename=str(GENRE_MODEL_PATH),
                input="model/Placeholder",  # From the JSON schema
                output="model/Sigmoid",  # From the JSON schema
            )

            # Classify the embedding
            genre_prediction = genre_classifier(embedding_vector)

            # Process the genre prediction
            genres_above_threshold, top_labels, top_scores = select_top_genres(
                genre_prediction, genre_labels, GENRE_SCORE_THRESHOLD, TOP_N_GENRES
            )

            print(f"Genres: {genres_above_threshold}")
            print(f"Top genres: {top_labels}")
            print(f"Scores: {top_scores}")

        except Exception as alt_e:
            print(f"Alternative approach also failed.")
            print(f"Error type: {type(alt_e)}")
            print(f"Error message: {str(alt_e)}")

            # Try direct TensorFlow approach
            try:
                print("\nTrying direct TensorFlow approach...")

                import tensorflow as tf
                import numpy as np

                # Load models
                embedding_model = tf.saved_model.load(str(EMBEDDING_MODEL_PATH))
                genre_model = tf.saved_model.load(str(GENRE_MODEL_PATH))

                # Convert audio to tensor
                audio_tensor = tf.convert_to_tensor(audio_signal.astype(np.float32))

                # Add batch dimension
                audio_tensor = tf.expand_dims(audio_tensor, 0)

                # Get the serving signature
                embedding_signature = list(embedding_model.signatures.keys())[0]
                genre_signature = list(genre_model.signatures.keys())[0]

                print(f"Embedding signature: {embedding_signature}")
                print(f"Genre signature: {genre_signature}")

                # Get the embedding
                embedding_fn = embedding_model.signatures[embedding_signature]
                embedding_result = embedding_fn(audio_tensor)

                # Get the first output tensor
                embedding_vector = list(embedding_result.values())[0]

                print("Successfully extracted embeddings with TensorFlow!")

                # Classify the embedding
                genre_fn = genre_model.signatures[genre_signature]
                genre_result = genre_fn(embedding_vector)

                # Get the prediction
                genre_prediction = list(genre_result.values())[0].numpy()

                # Process the genre prediction
                genres_above_threshold, top_labels, top_scores = select_top_genres(
                    [genre_prediction],  # Make it 2D as expected by select_top_genres
                    genre_labels,
                    GENRE_SCORE_THRESHOLD,
                    TOP_N_GENRES,
                )

                print(f"Genres: {genres_above_threshold}")
                print(f"Top genres: {top_labels}")
                print(f"Scores: {top_scores}")

            except Exception as tf_e:
                print(f"TensorFlow approach also failed.")
                print(f"Error type: {type(tf_e)}")
                print(f"Error message: {str(tf_e)}")

                # Let's try one more approach based on the JSON schema
                try:
                    print("\nTrying approach based on JSON schema...")

                    from essentia.standard import (
                        TensorflowPredictEffnetDiscogs,
                        TensorflowPredict,
                    )

                    # Extract embeddings with minimal parameters
                    embedding_extractor = TensorflowPredictEffnetDiscogs(
                        graphFilename=str(EMBEDDING_MODEL_PATH)
                    )

                    # Extract embeddings
                    embedding_vector = embedding_extractor(audio_signal)

                    print("Successfully extracted embeddings with minimal parameters!")

                    # Use TensorflowPredict for classification as specified in the JSON
                    genre_classifier = TensorflowPredict(
                        graphFilename=str(GENRE_MODEL_PATH),
                        input="model/Placeholder",
                        output="model/Sigmoid",
                    )

                    # Reshape the embedding if needed
                    if (
                        hasattr(embedding_vector, "shape")
                        and len(embedding_vector.shape) == 1
                    ):
                        import numpy as np

                        embedding_vector = np.expand_dims(embedding_vector, 0)

                    # Classify the embedding
                    genre_prediction = genre_classifier(embedding_vector)

                    # Process the genre prediction
                    genres_above_threshold, top_labels, top_scores = select_top_genres(
                        genre_prediction,
                        genre_labels,
                        GENRE_SCORE_THRESHOLD,
                        TOP_N_GENRES,
                    )

                    print(f"Genres: {genres_above_threshold}")
                    print(f"Top genres: {top_labels}")
                    print(f"Scores: {top_scores}")

                except Exception as final_e:
                    print(f"All approaches failed.")
                    print(f"Final error type: {type(final_e)}")
                    print(f"Final error message: {str(final_e)}")

                    # Final debugging suggestion
                    print("\nDebugging suggestions:")
                    print(
                        "1. Check if Essentia version is compatible with these models"
                    )
                    print(
                        "2. Try a different audio file to rule out file-specific issues"
                    )
                    print(
                        "3. Look for examples in the Essentia documentation or GitHub"
                    )


if __name__ == "__main__":
    main()
