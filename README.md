# Perfume Scraper - Complete Dataset Collection

A comprehensive and robust web scraping solution for collecting detailed perfume data from **Fragrantica** and **FragranceNet**.

## Overview

This project provides a production-ready scraping framework to extract perfume information from two major fragrance websites:

- **Fragrantica**: In-depth details including fragrance notes pyramid, main accords with percentages, user reviews, ratings, perfumers, and more.
- **FragranceNet**: Product listings with current pricing, sizes, availability, and scent notes.

The scraper is designed with scalability, reliability, and data quality in mind.

## Features

### Data Collection
- **Fragrantica**:
  - Perfume name, brand, year, perfumer, gender, and fragrance type
  - Top, Middle, and Base notes
  - Main accords with strength percentages
  - Overall rating and vote count
  - User reviews (rating, title, content, likes, date) — limited to 50 per perfume

- **FragranceNet**:
  - Product name, brand, gender, and fragrance family
  - Multiple price points with sizes and currencies
  - Stock availability
  - Scent notes

### Technical Features
- Smart rate limiting with domain-specific delays
- Robots.txt compliance checker
- Proxy rotation support
- User-Agent rotation (with `fake-useragent`)
- Cloudscraper integration for bypassing Cloudflare protection
- Persistent SQLite database for progress tracking and resume capability
- Duplicate detection using content fingerprinting
- Pydantic validation for data quality
- Graceful shutdown on SIGINT/SIGTERM
- Structured JSON logging
- Automatic export to JSON files

## Requirements

### Python Dependencies
```bash
pip install requests beautifulsoup4 pydantic cloudscraper fake-useragent