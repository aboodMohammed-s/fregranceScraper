# iPhone Price Scraper – MobileDirectOnline

A simple Python script that scrapes **iPhone prices** from [mobiledirectonline.co.uk](https://mobiledirectonline.co.uk/collections/iphone).  

It extracts: product name, storage capacity, color, current sale price, and original price (if available).

The results are saved into a clean Excel file: `iphone_products.xlsx`

## Features
- Automatically fetches current iPhone listings
- Cleans price data (removes currency symbols, extra characters)
- Parses capacity and color from product title using regex
- Saves structured data to Excel
- Basic error handling (timeouts, connection issues…)

## Requirements

```bash
pip install requests beautifulsoup4 pandas openpyxl