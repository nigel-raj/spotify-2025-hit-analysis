# 🎧 Decoding the DNA of a Spotify Global Hit

A full-scale data pipeline and analytical study of Spotify’s Daily Global Top 200 (2025) to uncover patterns behind hit songs.

This project combines web scraping, API enrichment, large language model–assisted emotion classification, and cultural event analysis to identify measurable characteristics of globally successful tracks.

---

## 📊 Dataset Overview

- **Source:** Spotify Daily Global Top 200 (2025)
- **Timeframe:** 365 days
- **Data Points:** ~73,000 chart entries
- **Unique Tracks:** Enriched via Spotify & Genius APIs
- **Final Output:** Emotion-labeled, metadata-enriched dataset for quantitative analysis

---

## 🏗 Project Architecture

The project is structured as a reproducible multi-stage data pipeline:

### 1️⃣ Chart Scraping  
- Automated extraction of daily Spotify Global Top 200 rankings  
- Structured CSV storage per date  

### 2️⃣ Data Consolidation  
- Combined 365 daily files into a master dataset  
- Added temporal indexing  
- Sorted by date and rank  

### 3️⃣ Spotify API Enrichment  
- Pulled track-level metadata:
  - Duration  
  - Popularity  
  - Explicit flag  
  - Album/Single cover image URL  
  - Artist genre

### 4️⃣ Genius Lyrics Enrichment  
- Retrieved full song lyrics  
- Cleaned section headers and embedded metadata  
- Standardised text for NLP processing  

### 5️⃣ Emotion Classification: From RoBERTa to LLM Reasoning

To quantify the emotional composition of global hit songs, I experimented with multiple NLP approaches before finalising the framework.

---

#### 🧪 Phase 1: Transformer-Based Emotion Classification (RoBERTa)

All lyrics were passed through the Hugging Face model:

**j-hartmann/emotion-english-distilroberta-base**

This model predicts probabilities across seven emotion categories:
- Anger
- Disgust
- Fear
- Joy
- Neutral
- Sadness
- Surprise

While technically robust, manual validation revealed critical limitations.

The model frequently interpreted lyrics at face value, struggling with:
- Sarcasm
- Metaphor
- Narrative framing
- Emotional tension within romantic themes

Example:

“Cruel Summer” – Taylor Swift  
The model classified it primarily as Disgust/Anger due to negative lexical tokens (“cruel”, tension-based phrases).  
However, the song is narratively about anxious romantic longing — closer to Fear + Joy than hostility.

Conclusion:

Although transformer-based, the model lacked sufficient contextual understanding of complex lyrical storytelling.

---

#### 🤖 Phase 2: Constrained LLM-Assisted Classification (Gemini)

To improve contextual accuracy, I pivoted to a large language model approach using Google Gemini (Google Sheets AI, February 2025 version).

Workflow:

1. Lyrics translated to English (where necessary)
2. Emotion classification constrained to Paul Ekman’s six core emotions:
   - Joy
   - Sadness
   - Anger
   - Fear
   - Surprise
   - Disgust
3. Single dominant emotion per track
4. Manual validation performed to verify coherence
5. Final labeled dataset stored locally for structured analysis

By constraining output categories and validating results, this hybrid approach significantly improved narrative-level interpretation compared to the pretrained RoBERTa model.

---

### 🧠 Why This Matters

This experimentation highlights an important analytical insight:

Even modern transformer models may struggle with figurative language and cultural context in music.

Context-aware LLM reasoning, when constrained and validated properly, can outperform static pretrained classifiers in nuanced domains like lyrical emotion analysis.

---

## 🔍 Key Findings

### 🎵 Emotional Composition
- Joy: 42%  
- Sadness: 38%  
- Anger: 15%  
- Remaining categories: minority share  

Global hits are predominantly emotionally positive or introspective rather than aggressive.

---

### ⏱ Duration Sweet Spot
- Global hits cluster around **3:21 – 3:25 minutes**
- Songs appear optimised for attention retention and replay behavior

---

### 📅 Seasonality & Cultural Impact
Clear streaming spikes observed during:

- Super Bowl period  
- Major album releases (e.g. Taylor Swift's The Life of a Showgirl album release day)  
- Christmas Eve surge  

Indicating strong interaction between music performance and cultural events.

---

### 🔞 Content Patterns
- 29% of #1 hits were explicit  
- Only 0.45% of charting songs were purely instrumental  
- August releases produced the highest number of #1 hits  

---

## 🧠 Analytical Focus

This project explores:

- Emotional resonance in hit music  
- Duration optimisation trends  
- Cultural event amplification effects  
- Release timing strategy  
- Explicit content prevalence  

It demonstrates how structured data pipelines combined with LLM-assisted classification can produce measurable cultural insights.

---

## 🛠 Tech Stack

- Python  
- Pandas  
- Selenium  
- Spotipy (Spotify API)  
- LyricsGenius (Genius API)  
- Google Gemini (LLM classification via Google Sheets AI)  
- Excel / Google Sheets
- Flourish
- Canva

---

## 🔐 Credentials & Setup

This project requires API credentials:

- Spotify Developer API  
- Genius API  

Create a `.env` file based on `.env.example` before running scripts.

---

## 📈 Final Output

The enriched and emotion-labeled dataset was used to generate the analytical infographic:

![Spotify Hit DNA](assets/decoding-the-dna.png)

---

## 🚀 Future Improvements

- Fully programmatic LLM-based emotion classification via API  
- Genre-emotion interaction modeling  
- Survival analysis on chart longevity  
- Predictive modeling for hit probability

## ⚖️ Disclaimer

This is an independent personal project conducted for analytical and educational purposes.

It has no affiliation with Spotify, Genius, Google, or any artists referenced.  
All chart data was collected from publicly accessible rankings.
