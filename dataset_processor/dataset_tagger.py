import pickle
from pathlib import Path

import essentia.standard as es
import numpy as np


class MTGJamendoTagger:
    """
    A class for auto-tagging audio files using a pre-trained MTG-Jamendo tags model.

    This class leverages Essentia for feature extraction (here using MFCCs) and a
    pre-trained classifier (e.g., corresponding to the MTG-Jamendo top50 tags model)
    for multi-label tagging.

    Attributes:
        model (object): The pre-trained classifier loaded from disk.
        tag_list (list): List of tag strings corresponding to the classifier output.
    """

    def __init__(self, model_path: Path, tag_list: list):
        """
        Initialize the tagger.

        Args:
            model_path (Path): Path to the pre-trained classifier model (pickle file).
            tag_list (list): List of tag names corresponding to the model outputs.
        """
        self.model = self._load_model(model_path)
        self.tag_list = tag_list

    def _load_model(self, model_path: Path):
        """
        Load the pre-trained classifier model from a pickle file.

        Args:
            model_path (Path): Path to the classifier model.

        Returns:
            object: The loaded model.
        """
        with model_path.open("rb") as f:
            return pickle.load(f)

    def extract_features(self, audio_file: Path) -> np.ndarray:
        """
        Extract an averaged MFCC feature vector from an audio file.

        Uses Essentia to load the audio and compute MFCCs over frames. The resulting
        frame-level MFCCs are averaged to yield a single feature vector.

        Args:
            audio_file (Path): Path to the audio file.

        Returns:
            np.ndarray: The averaged MFCC feature vector.

        Raises:
            ValueError: If no valid frames are extracted (e.g., file too short).
        """
        # Load a mono audio file using Essentia's loader
        loader = es.MonoLoader(filename=str(audio_file))
        audio = loader()

        # Initialize algorithms for framing and MFCC extraction
        frame_cutter = es.FrameCutter(frameSize=1024, hopSize=512)
        windowing = es.Windowing(type="hann")
        spectrum = es.Spectrum()
        mfcc = es.MFCC(numberCoefficients=13)

        mfcc_frames = []
        for frame in frame_cutter(audio):
            if len(frame) < 1024:
                # Skip frames that are shorter than the expected size (e.g., at the end)
                continue
            # Apply the windowing function
            windowed_frame = windowing(frame)
            # Compute the magnitude spectrum
            spec = spectrum(windowed_frame)
            # Compute MFCCs (ignore the 0th coefficient if desired)
            m, _ = mfcc(spec)
            mfcc_frames.append(m)

        if not mfcc_frames:
            raise ValueError(
                "Audio file is too short or corrupted; no frames extracted."
            )

        # Average the MFCC vectors over all frames to get a single feature vector
        features = np.mean(np.array(mfcc_frames), axis=0)
        return features

    def predict(self, audio_file: Path, threshold: float = 0.5) -> list:
        """
        Predict the tags for a given audio file.

        This method extracts features from the audio file and then uses the classifier model
        to generate predictions. It assumes that the classifier supports `predict_proba()`
        for multi-label classification. If not, adjust accordingly.

        Args:
            audio_file (Path): Path to the audio file.
            threshold (float): Probability threshold for considering a tag as present.

        Returns:
            list: List of predicted tag strings.
        """
        # Extract features and reshape them for the classifier input
        features = self.extract_features(audio_file).reshape(1, -1)

        # Get the probability estimates for each tag
        # (Adjust if your classifier does not use predict_proba)
        probabilities = self.model.predict_proba(features)[0]

        # Select tags where the predicted probability exceeds the threshold
        predicted_tags = [
            tag for tag, prob in zip(self.tag_list, probabilities) if prob >= threshold
        ]
        return predicted_tags


if __name__ == "__main__":
    # Define the paths to your model and an audio file
    model_file = Path("/path/to/your/mtg_jamendo_classifier.pkl")
    audio_file = Path("/path/to/an/audio_file.wav")

    # Define the list of tags (for example, the 50 tags from MTG-Jamendo top50tags)
    tags = [
        "alternative",
        "ambient",
        "atmospheric",
        "chillout",
        "classical",
        "dance",
        "downtempo",
        "easylistening",
        "electronic",
        "experimental",
        "folk",
        "funk",
        "hiphop",
        "house",
        "indie",
        "instrumentalpop",
        "jazz",
        "lounge",
        "metal",
        "newage",
        "orchestral",
        "pop",
        "popfolk",
        "poprock",
        "reggae",
        "rock",
        "soundtrack",
        "techno",
        "trance",
        "triphop",
        "world",
        "acousticguitar",
        "bass",
        "computer",
        "drummachine",
        "drums",
        "electricguitar",
        "electricpiano",
        "guitar",
        "keyboard",
        "piano",
        "strings",
        "synthesizer",
        "violin",
        "voice",
        "emotional",
        "energetic",
        "film",
        "happy",
        "relaxing",
    ]

    # Initialize the MTGJamendoTagger
    tagger = MTGJamendoTagger(model_path=model_file, tag_list=tags)

    # Predict and print the tags for the audio file
    predicted = tagger.predict(audio_file)
    print(f"Predicted tags for {audio_file.name}: {predicted}")
