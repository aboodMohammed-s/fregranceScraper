import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

URL = "https://mobiledirectonline.co.uk/collections/iphone"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def get_iphone_prices():
    try:
        response = requests.get(URL, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching the page: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")


    products = soup.find_all("hdt-card-product")

    items = []

    for product in products:

        title_tag = product.find("a", class_="hdt-card-product__title")
        title = title_tag.get_text(strip=True) if title_tag else "Not Available"


        sale_price_tag = product.find("hdt-price", class_="hdt-price")
        sale_price = sale_price_tag.get_text(strip=True) if sale_price_tag else "Not Available"

        sale_price_clean = re.sub(r'[^\d.]', '', sale_price) if sale_price != "Not Available" else ""

        compare_tag = product.find("hdt-compare-at-price")
        compare_price = compare_tag.find("span", class_="hdt-money").get_text(strip=True) if compare_tag else "Not Available"

        capacity = "Unknown"
        color = "Unknown"

        match = re.search(r'(\d+(?:GB|TB))\s+([\w\s]+?)(?:\s*$|\s+[\(\[])', title, re.IGNORECASE)
        if match:
            capacity = match.group(1)
            color = match.group(2).strip()

        items.append({
            "Product Name": title,
            "Capacity": capacity,
            "Color": color,
            "Sale Price": f"£{sale_price_clean}" if sale_price_clean else sale_price,
            "Original Price": compare_price,
        })

    return items


def main():
    products_list = get_iphone_prices()

    if not products_list:
        print("No products found or an error occurred.")
        return

    print(f"Found {len(products_list)} products\n")
    print("-" * 80)

    df = pd.DataFrame(products_list)
    df.to_excel("iphone_products.xlsx", index=False)
    print("Saved data to iphone_products.xlsx")


if __name__ == "__main__":
    main()