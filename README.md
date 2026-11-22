# MoodleCrawler

Command-line tool for downloading and incrementally organizing academic course material from eGela (UPV/EHU Moodle platform).

## Tech Stack

- **Language:** Python 3.10+
- **Automation & Scraping:** Selenium WebDriver, ChromeDriver / Chromium
- **State & Storage:** SQLite

## Architecture & Key Decisions

- **Automated SSO Authentication:** Handles university LDAP / CAS single sign-on flows through headless Selenium with environment credentials.
- **Incremental Synchronization:** Computes SHA-256 hashes of discovered documents and compares against local SQLite index to skip unchanged slides, assignments, and PDFs.
- **Clean Folder Hierarchy:** Maps eGela course categories and sections directly to clean, sanitized local directory trees.

## Local Setup

### Prerequisites

- Python 3.10+
- Google Chrome or Chromium installed

### Installation & Execution

```bash
git clone https://github.com/eneekoruiz/moodlecrawler.git
cd moodlecrawler

python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Set university credentials via environment variables
export EGELA_USER="your-ldap-user"
export EGELA_PASS="your-ldap-password"

# Run crawler
python legacy_crawlers/refactored_crawler.py
```
