# Simple CSW crawler

Fetch all available records from a list of CSW endpoints.


## Usage

```bash
uv run scrapy runspider csw-spider.py -a endpoints=endpoints.csv [-s KEY=VALUE]
```


## Input

A CSV file following the [datagouv export-harvest-*.csv](https://www.data.gouv.fr/datasets/catalogue-des-donnees-de-data-gouv-fr) format.

The CSV must at least contain the `id`, `name` and `url` columns. If the `backend` and `validation` columns are also present, only "accepted" "csw-iso-19139" endpoints will be processed.


## Output

A directory tree of ISO-19139 XML records, structured according to the following layout:

```
OUTPUT_DIR/
├── endpoint-a/
│   ├── record-a-1.xml
│   ├── record-a-2.xml
│   └── ...
├── endpoint-b/
│   ├── record-b-1.xml
│   ├── record-b-2.xml
│   └── ...
┆
```


## Settings

Defaults can be overridden on the command line with `-s KEY=VALUE`:

| Setting | Default | Description |
|---|---|---|
| `CONCURRENT_REQUESTS` | `100` | Parallel requests across all endpoints |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | `2` | Parallel requests to a given domain |
| `DOWNLOAD_DELAY` | `0.5` | Seconds between requests per domain |
| `DOWNLOAD_TIMEOUT` | `60` | Seconds before request timeout |
| `LOG_LEVEL` | `INFO` | Log level |
| `OUTPUT_DIR` | `output` | Root directory for harvested output |
| `RETRY_TIMES` | `1` | Retries on failure (in addition to inital request) |
