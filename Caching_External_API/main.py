import json
import redis
import hashlib
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

redis_client = redis.Redis(host="localhost", port=6379, db=0)


class PostRequest(BaseModel):
    post_id: int


def make_cache_key(post_id: int):
    raw = f"post_id:{post_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


@app.post("/get-post")
async def get_post_data(data: PostRequest):
    cache_key = make_cache_key(data.post_id)

    cached_result = redis_client.get(cache_key)

    if cached_result:
        print("Serving from redis cache!!")
        return json.loads(cached_result)

    print("Callling External API Calls...")
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"https://jsonplaceholder.typicode.com/posts/{data.post_id}"
        )
        if response.status_code != 200:
            return {"message": "Post Not found!!"}

        post_data = response.json()
        redis_client.setex(cache_key, 600, json.dumps(post_data))
        print("Data Fetched and stored in Cache!!")
        return post_data
