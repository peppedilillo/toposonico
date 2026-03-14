import os

import meilisearch


meili_url = os.environ.get("MEILI_URL", "http://localhost:7700")
meili_key = os.environ.get("MEILI_MASTER_KEY")

client = meilisearch.Client(meili_url, meili_key)
indexes_response = client.get_indexes({"limit": 1000})
for index in indexes_response["results"]:
    print(f"Deleting index: {index.uid}")
    client.delete_index(index.uid)

print("All indexes have been sent for deletion.")