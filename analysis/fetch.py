import requests


DEFAULT_URL = "http://localhost:8081/search"
DEFAULT_QUERY = "*"

session = requests.Session()


def _fetch_page(page, /, url, query):
    global session

    response = session.get(
        url,
        params={"query": query, "page": page, "results_per_page": 100},
        headers={"accept": "application/json"},
    )

    response.raise_for_status()

    return response.json()


def fetch(*, url=DEFAULT_URL, query=DEFAULT_QUERY):
    results = _fetch_page(1, url, query)

    for result in results["results"]:
        yield result["source"], result["id"], result["dataset"]

    for page in range(2, results["pages"] + 1):
        results = _fetch_page(page, url, query)

        for result in results["results"]:
            yield result["source"], result["id"], result["dataset"]
