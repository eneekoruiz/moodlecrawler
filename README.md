# moodlecrawler

Python script for downloading and organizing course material from eGela, the UPV/EHU Moodle platform.

The script logs in with user-provided credentials, reads a list of course URLs, and saves resources locally while keeping the course structure readable. It is intended for personal backups of your own material.

## What it does

- reads course URLs from `cursos.txt`
- uses Selenium for pages that need browser interaction
- downloads resources with `requests`
- keeps track of processed files with SQLite
- writes local indexes for easier offline browsing

## Requirements

- Python 3.12+
- Google Chrome
- access to the eGela account that owns the courses

Install the Python dependencies:

```bash
pip install requests selenium webdriver-manager
```

## Configuration

Set your credentials as environment variables. Do not commit them.

PowerShell:

```powershell
$env:EGELA_USER="your-user"
$env:EGELA_PASS="your-password"
```

Then create `cursos.txt` with one course URL per line:

```text
https://egela.ehu.eus/course/view.php?id=12345
https://egela.ehu.eus/course/view.php?id=67890
```

## Usage

```bash
python crawler.py
```

Downloaded content is written to `EGELA_ENTERPRISE_TIMECAPSULE` by default.

## Notes

- Some Moodle resources need a real browser session, so the script uses Selenium in headless mode.
- Failed or manually reviewed resources are logged in the generated course output.
- Use it responsibly and avoid unnecessary load on university servers.

## Social preview

GitHub social preview asset: `docs/images/social-preview.png`

## Documentation

- DeepWiki: https://deepwiki.com/eneekoruiz/moodlecrawler
