"""
RoBERTa Emotion Analysis Module

Applies the Hugging Face model:
j-hartmann/emotion-english-distilroberta-base

This script assumes lyrics are already present in the dataset.
"""

import pandas as pd
from transformers import pipeline
import logging
import os
import torch
from typing import Dict

# Configuration
INPUT_FILE = "lyrics_dataset.csv"
OUTPUT_FILE = "lyrics_with_roberta_emotions.csv"
EMOTION_MODEL = "j-hartmann/emotion-english-distilroberta-base"
MAX_TOKENS = 512

EMOTION_LABELS = [
    "anger", "disgust", "fear",
    "joy", "neutral", "sadness", "surprise"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_data(input_file: str) -> pd.DataFrame:
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"{input_file} not found")
    logging.info(f"Loading dataset: {input_file}")
    return pd.read_csv(input_file, encoding="utf-8")


def analyze_emotions(classifier, text: str) -> Dict[str, float]:
    if not isinstance(text, str) or not text.strip():
        return {f"score_{e}": None for e in EMOTION_LABELS}

    try:
        results = classifier(
            text,
            top_k=None,
            truncation=True,
            max_length=MAX_TOKENS
        )

        scores = {f"score_{e}": 0.0 for e in EMOTION_LABELS}

        for r in results:
            label = r["label"].lower()
            if label in EMOTION_LABELS:
                scores[f"score_{label}"] = r["score"]

        return scores

    except Exception as e:
        logging.warning(f"Emotion analysis failed: {e}")
        return {f"score_{e}": None for e in EMOTION_LABELS}


def main():
    logging.info("Initializing emotion classifier...")
    classifier = pipeline(
        "text-classification",
        model=EMOTION_MODEL,
        device=0 if torch.cuda.is_available() else -1
    )
    logging.info("✓ Model loaded")

    df = load_data(INPUT_FILE)

    if "lyrics" not in df.columns:
        raise ValueError("Dataset must contain a 'lyrics' column")

    logging.info("Running emotion analysis...")
    emotion_data = df["lyrics"].apply(
        lambda x: analyze_emotions(classifier, x)
    )

    emotion_df = pd.DataFrame(list(emotion_data))
    final_df = pd.concat([df, emotion_df], axis=1)

    logging.info(f"Saving results to {OUTPUT_FILE}")
    final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    logging.info("✓ RoBERTa emotion scoring complete")


if __name__ == "__main__":
    main()