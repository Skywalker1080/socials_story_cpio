import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from sqlalchemy import create_engine
import os
from jinja2 import Environment, FileSystemLoader
from instagrapi import Client

from DB_FETCH_ETL_TOKENOMICS import fetch_tokenomics_news

# Database connection configuration
DB_CONFIG = {
    'host': '34.55.195.199',        # GCP PostgreSQL instance public IP
    'database': 'dbcp',             # Database name
    'user': 'yogass09',             # Username
    'password': 'jaimaakamakhya',   # Password
    'port': 5432                    # PostgreSQL default port
}

def get_gcp_engine():
    """Create and return a SQLAlchemy engine for the GCP PostgreSQL database."""
    connection_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
                     f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_url)

# Initialize the GCP engine
gcp_engine = get_gcp_engine()

async def generate_image_from_html(output_html_file, output_image_path):
    """Launch Playwright, load the HTML file, and save a screenshot of it."""
    async with async_playwright() as p:
        # Launch a browser
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.set_viewport_size({"width": 1080, "height": 1920})

        # Load the rendered HTML file
        await page.goto('file://' + os.path.abspath(output_html_file))

        # Capture the screenshot of the page
        await page.screenshot(path=output_image_path)

        print(f"Screenshot saved as {output_image_path}.")

        # Close the browser
        await browser.close()

#-------------------------------------------------------
async def render_page():
    
    df_filtered = fetch_tokenomics_news()
    df_filtered = df_filtered.fillna("")
    df_filtered = df_filtered.head(2)

    df = df_filtered.to_dict(orient='records')

    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('tokenomics.html')

    output = template.render(df=df)

    # Save the output to an HTML file
    with open("tokenomics_output.html", "w", encoding="utf-8") as f:
        f.write(output)

    print("Rendered page saved as 'tokenomics_output.html'.")

    # Use Playwright to convert the HTML file to an image
    await generate_image_from_html("tokenomics_output.html", 'tokenomics_output.jpg')

if __name__=="__main__":

    asyncio.run(render_page())