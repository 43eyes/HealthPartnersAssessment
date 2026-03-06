# CMS Hospital Data Downloader

Python script that pulls all Hospital-themed datasets from the CMS Provider Data metastore, cleans up the column headers to snake_case, and saves them as CSVs. Designed to run daily. It tracks what's been downloaded and only grabs files that are new or have been updated since the last run.

## How It Works

1. Hits the [CMS Provider Data API](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items) to get the full dataset catalog
2. Filters down to datasets tagged with the "Hospitals" theme
3. Checks each dataset against `tracking.json` to see if it's new or has a newer modified date
4. Downloads new/updated CSVs in parallel (5 threads)
5. Converts all column headers to snake_case (strips special characters, lowercases, replaces spaces with underscores)
6. Saves processed CSVs to `downloads/` and updates `tracking.json`

If a dataset's download URL changes on an update (the URL contains a hash that can change), the old file gets cleaned up automatically after the new one downloads successfully.

## Requirements

- Python 3.7+
- Install dependencies: `pip install -r requirements.txt`

## Usage

```bash
python main.py
```

First run will download all Hospital datasets. Subsequent runs will only download files that have changed.

To force a full re-download, just delete `tracking.json`.

## Scheduling

The script is built to be idempotent so it can be scheduled to run daily:

- **Linux**: cron job, e.g. `0 2 * * * cd /path/to/project && python main.py`
- **Windows**: Task Scheduler pointing at the script

## Project Structure

```
├── main.py              # Main script
├── requirements.txt     # Python dependencies
├── tracking.json        # Auto-generated, tracks last download state per dataset
├── downloads/           # Auto-generated, where processed CSVs are saved
└── README.md
```

## Notes

- Downloads run in parallel with 5 worker threads to balance speed with not hammering CMS's servers
- CSV column names like `"Patients' rating of the facility linear mean score"` get converted to `patients_rating_of_the_facility_linear_mean_score`
- Tracking is only updated after a successful download, so failed downloads will be retried on the next run
- The `low_memory=False` flag is set on CSV reads because some CMS datasets have mixed types in footnote columns
