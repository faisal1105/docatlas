# DocAtlas
— DocAtlas —

DocAtlas is a CLI + GUI tool to extract content from PDFs/DOC/DOCX/PPTX/XLSX, summarize, categorize, tag, detect duplicates, and organize files into category folders.

## Quick Start (Windows)
1. Install Python (3.10+).
2. Ensure Python is on PATH.
3. Install dependencies:
```bash
pip install -r requirements.txt
```
4. (Optional) Install OCR dependencies (see below).
5. Set Azure/OpenAI environment variables (see below).
6. Run:
```bash
python docatlas.py
```

## Build Portable EXE (Windows)
This builds a single-file executable so other PCs can run the tool without installing Python.

```bash
cd C:\Users\faisal.islam\Desktop\Codex\docatlas
.\build.ps1
```

Output:
```
dist\docatlas.exe
```

Notes:
- The EXE still needs OCR tools installed on the target machine if you want OCR.
- You still need environment variables for the API keys on the target machine.

## What It Produces
- `{application}__docatlas_summaries.xlsx` (falls back to `uncategorized__docatlas_summaries.xlsx`)
  - `Documents` sheet: summaries, categories, tags, duplicate flags
  - `Articles` sheet: per-article summaries (PDF only)
- `{application}__docatlas_full_text.xlsx` (falls back to `uncategorized__docatlas_full_text.xlsx`)
  - `FullText` sheet: metadata + full extracted text
- `summary_report.txt`
  - counts by category, duplicates, extraction status

## Requirements
Install dependencies:

```bash
pip install -r requirements.txt
```

## Install Python (Windows)
1. Download Python from https://www.python.org/downloads/windows/ (recommended) or Microsoft Store.
2. In the installer, check **Add Python to PATH**.
3. Verify:
```bash
python --version
pip --version
```

## Linux Server Setup (OCR + Scale)
Install system OCR tools (Ubuntu/Debian):
```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr ghostscript qpdf poppler-utils
```

Create a virtualenv and install Python deps:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run server entrypoint (parallel workers):
```bash
python docatlas_server.py --input "/data/input" --output "/data/output" --app "qPCR" --workers 4
```

## Server/CLI Entry Point
Use `docatlas_server.py` for headless servers:
```bash
python docatlas_server.py --input "/data/input" --output "/data/output" --app "qPCR" --workers 4
```

## Azure OpenAI Configuration
Set environment variables before running:

```bash
# Required
setx AZURE_OPENAI_API_KEY "<your-key>"

# Optional (defaults shown)
setx AZURE_OPENAI_API_VERSION "2025-03-01-preview"
setx AZURE_OPENAI_API_KEY_HEADER "api-key"
setx AZURE_CHAT_BASE_URL "https://api.geneai.thermofisher.com/dev/gpt5"
setx AZURE_EMBEDDINGS_BASE_URL "https://api.geneai.thermofisher.com/dev/embeddingsv2"
setx AZURE_CHAT_DEPLOYMENT "gpt-5.2"
setx AZURE_EMBEDDINGS_DEPLOYMENT "text-embedding-3-small"
setx AZURE_CHAT_PATH "/openai/deployments/{deployment}/chat/completions"
setx AZURE_EMBEDDINGS_PATH "/openai/deployments/{deployment}/embeddings"
setx AZURE_INCLUDE_MODEL_IN_BODY "1"
setx DOCATLAS_API_DELAY "0.3"

# Optional: separate keys for chat/embeddings (if your gateway uses different keys)
setx AZURE_CHAT_API_KEY "<your-chat-key>"
setx AZURE_EMBEDDINGS_API_KEY "<your-embeddings-key>"
```

If your API gateway expects a different path, override `AZURE_CHAT_PATH` and `AZURE_EMBEDDINGS_PATH`.

`DOCATLAS_API_DELAY` adds a small delay (seconds) before each LLM/embeddings call to reduce
transient Windows socket exhaustion errors (e.g., WinError 10048). Default is 0.3 seconds.

In the GUI flow, if keys are missing, DocAtlas will prompt separately for the **LLM key** and the **embeddings key**.

## OCR Dependencies (Windows)
OCR is optional. The tool runs without it, but scanned/locked PDFs may yield little or no text. In that case, `extraction_status` will indicate OCR was unavailable and summaries may be weak.

### Tesseract
Download: https://github.com/tesseract-ocr/tesseract  
Add to PATH: `C:\Program Files\Tesseract-OCR\`  
Test:
```bash
tesseract --version
```

### Poppler (for pdftoppm)
Download: https://github.com/oschwartz10612/poppler-windows  
Add to PATH: `C:\poppler\Library\bin` (or your install path)  
Test:
```bash
pdftoppm -h
```

### Ghostscript
Download: https://www.ghostscript.com/download/gsdnld.html  
Add to PATH: `C:\Program Files\gs\gs10.xx\bin`  
Test:
```bash
gswin64c -v
```

### qpdf
Download: https://github.com/qpdf/qpdf/releases  
Add to PATH: `C:\qpdf\bin`  
Test:
```bash
qpdf --version
```

## Run (GUI)

```bash
python docatlas.py
```

The GUI will:
1. Ask for input folder
2. Ask for output folder (Cancel = use input folder)
3. Ask for application (dropdown) and categories (one per line)
4. Ask for API key if `AZURE_OPENAI_API_KEY` is not set
5. Ask whether to use OCRmyPDF for PDFs
6. Optional: click `Edit Apps` to edit the application/category config
7. Optional: click `Test OCR` to check dependencies before running
8. Shows a progress window with ETA during processing

## Run (CLI)

```bash
python docatlas.py --input "C:\path\to\docs" --output "C:\path\to\out" --categories "Finance;HR;Legal"
```

Or use an application from config:

```bash
python docatlas.py --input "C:\path\to\docs" --output "C:\path\to\out" --app "Sequencing"
```

The default config is `applications.json` in the same folder. You can override it:

```bash
python docatlas.py --config "C:\path\to\applications.json" --input "C:\path\to\docs" --output "C:\path\to\out" --app "qPCR"
```

### Options
- `--dry-run`: do not call APIs or move files (hash-based duplicates only)
- `--no-resume`: disable resume cache
- `--no-ocrmypdf`: disable OCRmyPDF and use Tesseract fallback
- `--embeddings-source summary|full_text|none`: choose embeddings input (default: `full_text`)
- `--overwrite-excel`: overwrite Excel outputs instead of appending (default is append)
- `--limit N`: process only the first N files (useful for time estimation)
- `--no-move`: do not move files (useful for estimation runs)
- `--config`: path to applications config JSON
- `--app`: application name from config (use instead of `--categories`)
- `--edit-config`: open the applications config editor
- `--test-embeddings`: test embeddings endpoint/key and exit
- `--test-chat`: test chat endpoint/key and exit

If you run with `--dry-run`, the API key is not required.

## Notes
- Duplicates are detected by SHA-256 hash and embeddings (cosine similarity >= 0.97).
- Excel outputs are appended by default; use `--overwrite-excel` to rebuild from scratch.
- If `--limit` is used, DocAtlas logs a rough total-time estimate.
- Token usage estimates are added to the summary report.
- In the GUI, you can toggle append vs overwrite in the “Embeddings Source” step.
- Embeddings can be computed from the **full text** (default, stricter), **long summary** (lower cost), or **disabled** (hash-only duplicates).
- Duplicate files are moved to `<category>_Duplicate`.
- PDF article splitting uses a heading-based heuristic.
- Resume cache stored as `resume.json` in the output folder.
- OCR fallback for PDFs: OCRmyPDF is used by default if extracted text is too short, then Tesseract OCR as a fallback.
- If OCR is enabled, embedded images inside `.docx` and `.pptx` are also OCR-processed.
- Embeddings are skipped for very short texts to reduce cost (configurable in code).
- `.doc` files are supported by auto-conversion to `.docx` via LibreOffice (`soffice`) if installed.
- Tags are deduplicated and capped to a reasonable size.
- `summary_report.txt` includes file type breakdown, category percentages, OCR usage count, duplicate group stats, and document length stats.
- Errors are captured per file and reported in `summary_report.txt` without stopping the run.
- `extraction_status` column values:
  - `ok`: text extracted normally
  - `ocrmypdf_used`: OCRmyPDF used successfully
  - `ocrmypdf_failed_then_ocr_used`: OCRmyPDF failed/no text, Tesseract OCR used
  - `ocr_used`: Tesseract OCR used (OCRmyPDF unavailable)
  - `no_text`: no text after extraction/OCR
  - `no_text_ocr_unavailable`: OCR libraries not available
  - `no_text_ocr_failed`: OCR attempted but failed
  - `no_text_ocrmypdf_unavailable`: OCRmyPDF not installed
  - `no_text_ocrmypdf_failed`: OCRmyPDF failed
- `no_text_ocrmypdf`: OCRmyPDF produced no text

### OCR Dependencies (Optional)
OCR uses OCRmyPDF (default), Tesseract, and Poppler.
- Install Tesseract (Windows): https://github.com/tesseract-ocr/tesseract
- Install Poppler (Windows): https://github.com/oschwartz10612/poppler-windows
- Install Ghostscript (Windows): https://www.ghostscript.com/download/gsdnld.html
- Install qpdf (Windows): https://github.com/qpdf/qpdf/releases

OCRmyPDF docs: https://ocrmypdf.readthedocs.io/

If these are not installed, the tool will still run and mark `extraction_status` as `no_text_ocr_unavailable` or `no_text_ocrmypdf_unavailable` when needed.

The tool will warn at startup if OCR dependencies are missing.

### Excel Full Text
Excel cells have a 32,767 character limit. Full text is split across columns:
- `full_text_part_1`, `full_text_part_2`, ...
- `full_text` contains the first part for quick viewing.
