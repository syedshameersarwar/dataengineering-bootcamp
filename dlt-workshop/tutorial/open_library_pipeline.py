"""Pipeline to ingest data from the Open Library REST API (Books API)."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from the Open Library REST API (books endpoint)."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
            # No auth required for public GET /api/books
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "endpoint": {
                "params": {
                    "format": "json",
                    "jscmd": "data",
                }
            },
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    "method": "GET",
                    "params": {
                        "bibkeys": "ISBN:0451526538,ISBN:0201558025,LCCN:93005405",
                    },
                    "data_selector": "*",
                    "paginator": {"type": "single_page"},
                },
                "primary_key": "url",
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
