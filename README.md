# Quickstart

- [Create and activate venv](https://www.w3schools.com/python/python_virtualenv.asp)
- `pip install -r requirements.txt`
- Adjust `config.py` according to your preferences
- `python -m scrape`

_NOTE:_ There are two csvs "...articles" which contains all articles and "...head_only" which contains a handful of rows for testing.

_NOTE:_ There is a `PROCESS_ES` config var that controls whether or not to process Spanish language articles. The english prompt _should_ work to chunk out spanish language articles, but a spanish language prompt might be better.
