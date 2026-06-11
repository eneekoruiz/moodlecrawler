# eGela Downloader & Archiver

Personal tool for downloading and organizing eGela course materials offline.

## What it does

- crawls course resources and downloads files concurrently
- keeps downloads organized by course and topic
- stores progress so interrupted runs can resume
- uses local state to avoid re-downloading duplicates

## Requirements

- Python 3.12+
- Google Chrome

## Run

```bash
pip install requests selenium webdriver-manager
python egela_downloader.py
```

## Notes

- credentials are read from environment variables
- some interactive or external links may need manual review

## Links

- DeepWiki: https://deepwiki.com/eneekoruiz/moodlecrawler
