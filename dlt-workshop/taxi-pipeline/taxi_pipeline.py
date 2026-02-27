import dlt
from dlt.sources.rest_api import rest_api_source


def taxi_api_source():
    """NYC taxi ride data from the Data Engineering Zoomcamp API."""
    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
            },
            "resources": [
                {
                    "name": "rides",
                    "endpoint": {
                        "path": "data_engineering_zoomcamp_api",
                        "paginator": {
                            "type": "page_number",
                            "base_page": 1,
                            "total_path": None,
                            "stop_after_empty_page": True,
                        },
                    },
                },
            ],
        }
    )


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_api_source())
    print(load_info)
