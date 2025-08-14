# pdf_handler.py
import os
import re
import aiohttp
import tempfile
import pdfplumber
import pandas as pd
from bs4 import BeautifulSoup

def columns_match(cols1, cols2):
    return [str(c).strip().lower() for c in cols1] == [str(c).strip().lower() for c in cols2]

async def download_pdf_from_url(url: str, output_dir: str = None) -> str:
    """Download PDF from URL and return local file path."""
    if output_dir is None:
        output_dir = tempfile.gettempdir()

    filename = os.path.basename(url.split("?")[0]) or "downloaded.pdf"
    file_path = os.path.join(output_dir, filename)

    print(f"📥 Downloading PDF from {url}...")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise Exception(f"Failed to download PDF from {url}, status: {resp.status}")
            content = await resp.read()
            with open(file_path, "wb") as f:
                f.write(content)
    print(f"✅ Downloaded PDF → {file_path}")
    return file_path

async def find_pdf_links_on_webpage(url: str) -> list:
    """Find all PDF links on a webpage."""
    print(f"🔍 Searching for PDF links on {url}...")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise Exception(f"Failed to fetch page {url}, status: {resp.status}")
            html_content = await resp.text()

    soup = BeautifulSoup(html_content, "html.parser")
    pdf_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if ".pdf" in href.lower():
            if href.startswith("http"):
                pdf_links.append(href)
            else:
                base = url.rstrip("/")
                pdf_links.append(base + "/" + href.lstrip("/"))

    print(f"📑 Found {len(pdf_links)} PDF link(s) on {url}")
    return list(set(pdf_links))

async def process_pdf(file_path: str, output_dir: str = None) -> list:
    """
    Extract tables from a PDF file using pdfplumber, group by header, save as CSV.
    Returns list of generated CSV file paths.
    """
    if output_dir is None:
        output_dir = tempfile.gettempdir()

    print(f"📄 Starting PDF processing → {file_path}")

    # Step 1: Extract text (optional)
    try:
        with pdfplumber.open(file_path) as pdf_doc:
            text = "\n".join([page.extract_text() or "" for page in pdf_doc.pages])
        print(f"📝 Extracted text from {file_path} ({len(text)} characters)")
    except Exception as e:
        print(f"⚠️ Could not extract plain text: {e}")

    # Step 2: Extract tables (No Java required)
    print(f"📊 Extracting tables from {file_path} using pdfplumber...")
    tables = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for page_number, page in enumerate(pdf.pages, start=1):
                page_tables = page.extract_tables()
                for table in page_tables:
                    if table and len(table) > 1:
                        df = pd.DataFrame(table[1:], columns=table[0])
                        tables.append(df)
                        print(f"   ✅ Extracted table from page {page_number} ({df.shape[0]} rows)")
    except Exception as e:
        print(f"❌ Failed to extract tables from {file_path}: {e}")
        return []

    if not tables:
        print(f"⚠️ No tables found in {file_path}")
        return []

    # Step 3: Group by header
    raw_tables = [{"table_number": i + 1, "columns": list(df.columns), "dataframe": df}
                  for i, df in enumerate(tables)]
    groups = []
    for tbl in raw_tables:
        placed = False
        for grp in groups:
            if columns_match(tbl["columns"], grp["reference_columns"]):
                grp["tables"].append(tbl)
                placed = True
                break
        if not placed:
            groups.append({"reference_columns": tbl["columns"], "tables": [tbl]})
    print(f"📦 Grouped into {len(groups)} table group(s) by matching headers")

    # Step 4: Merge & save as CSV
    generated_csvs = []
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    for g_idx, grp in enumerate(groups, start=1):
        merged_df = pd.concat([t["dataframe"].copy() for t in grp["tables"]], ignore_index=True)
        if len(groups) == 1:
            csv_filename = os.path.join(output_dir, f"{base_name}.csv")
        else:
            first_col = grp["reference_columns"][0] if grp["reference_columns"] else f"group_{g_idx}"
            safe_part = re.sub(r'[^A-Za-z0-9_]+', '_', str(first_col))[:20]
            csv_filename = os.path.join(output_dir, f"{base_name}_{safe_part or 'group'}_{g_idx}.csv")

        merged_df.to_csv(csv_filename, index=False, encoding="utf-8")
        generated_csvs.append(csv_filename)
        print(f"💾 Saved group {g_idx} → {csv_filename}")

    print(f"🎯 Completed PDF processing for {file_path}, {len(generated_csvs)} CSV(s) created")
    return generated_csvs
