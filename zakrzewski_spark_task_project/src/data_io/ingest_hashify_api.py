import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def get_md4_hash(claim_id):
    try:
        url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get("Digest")
        else:
            return None
    except Exception:
        return None

get_md4_hash_udf = udf(get_md4_hash, StringType())