import os
import re
import tempfile
import tabula
import pandas as pd
import pdfplumber
from fastapi import UploadFile

def columns_match(cols1, cols2):
    """Compare two header lists ignoring case and whitespace."""
    return [str(c).strip().lower() for c in cols1] == [str(c).strip().lower() for c in cols2]

async def process_pdf(file_path: str) -> list:
    """
    Process a PDF file → extract tables, group by header, save to CSV.
    Returns a list of generated CSV file paths.
    """
    print(f"📄 Starting PDF processing: {file_path}")

    # Try extracting text (optional for debugging/LLM context)
    try:
        with pdfplumber.open(file_path) as pdf_doc:
            text = "\n".join([page.extract_text() or "" for page in pdf_doc.pages])
        print(f"📝 Extracted {len(text)} characters of plain text")
    except Exception as e:
        print(f"⚠️ Text extraction failed: {e}")

    # Extract tables
    try:
        tables = tabula.read_pdf(
            file_path,
            pages='all',
            multiple_tables=True,
            pandas_options={'header': 'infer'},
            lattice=True,
            silent=True
        )
        if not tables or all(df.empty for df in tables):
            print("🔄 Retrying with stream mode...")
            tables = tabula.read_pdf(
                file_path,
                pages='all',
                multiple_tables=True,
                pandas_options={'header': 'infer'},
                stream=True,
                silent=True
            )
    except Exception as e:
        print(f"❌ Table extraction failed: {e}")
        return []

    if not tables or all(df.empty for df in tables):
        print("⚠️ No tables found in PDF")
        return []

    print(f"✅ Found {len(tables)} table(s) in {os.path.basename(file_path)}")

    # Group tables by header
    raw_tables = []
    for idx, df in enumerate(tables):
        if df.empty:
            print(f"⏭️ Skipping empty table {idx+1}")
            continue
        raw_tables.append({"table_number": idx + 1, "columns": list(df.columns), "dataframe": df})

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
    print(f"📦 Grouped into {len(groups)} group(s) by header")

    # Merge and save CSVs
    generated_csvs = []
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    for g_idx, grp in enumerate(groups, start=1):
        merged_df = pd.concat([t["dataframe"].copy() for t in grp["tables"]], ignore_index=True)
        if len(groups) == 1:
            csv_filename = f"{base_name}.csv"
        else:
            first_col = grp["reference_columns"][0] if grp["reference_columns"] else f"group_{g_idx}"
            safe_part = re.sub(r'[^A-Za-z0-9_]+', '_', str(first_col))[:20]
            csv_filename = f"{base_name}_{safe_part or 'group'}_{g_idx}.csv"

        merged_df.to_csv(csv_filename, index=False, encoding="utf-8")
        generated_csvs.append(csv_filename)
        print(f"💾 Saved group {g_idx} → {csv_filename}")

    return generated_csvs


async def process_uploaded_pdf(pdf: UploadFile, created_files: set) -> list:
    """Handle directly uploaded PDF."""
    temp_pdf_path = f"uploaded_{pdf.filename}" if pdf.filename else "uploaded_file.pdf"
    with open(temp_pdf_path, "wb") as f:
        f.write(await pdf.read())
    created_files.add(os.path.normpath(temp_pdf_path))
    csv_paths = await process_pdf(temp_pdf_path)
    return [{"filename": path, "source_pdf": temp_pdf_path} for path in csv_paths]


async def process_extracted_pdfs(pdf_paths: list, created_files: set) -> list:
    """Handle PDFs extracted from archives."""
    results = []
    for pdf_path in pdf_paths:
        csv_paths = await process_pdf(pdf_path)
        for path in csv_paths:
            created_files.add(os.path.normpath(path))
            results.append({"filename": path, "source_pdf": pdf_path})
    return results
