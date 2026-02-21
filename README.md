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

### 5️⃣ Emotion Classification (From Lexicons to LLMs)

Lyrics were translated to English (where necessary) and classified into one of six discrete emotion categories based on **Paul Ekman’s Basic Emotions Framework**:

- Joy  
- Sadness  
- Anger  
- Fear  
- Surprise  
- Disgust  

Classification was performed using **Google’s Gemini large language model via Google Sheets AI (February 2025 release version)**.

To ensure consistency and reduce variance:

- Output was strictly constrained to the six predefined categories  
- Each track received a single dominant emotion label  
- Manual validation was conducted to verify classification quality  
- Final labeled dataset was stored locally for structured analysis  

This hybrid approach allowed scalable emotion modeling while maintaining controlled categorical outputs.

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
