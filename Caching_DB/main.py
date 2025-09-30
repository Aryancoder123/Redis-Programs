import json
import hashlib
import redis
import joblib
import sqlite3
from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI()

redis_client = redis.Redis(host="localhost", port=6379, db=0)


def get_db_connection():
    conn = sqlite3.connect("db.sqlite3")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
CREATE TABLE IF NOT EXISTS USERS(
                   id INTEGER PRIMARY_KEY,
                   name TEXT NOT NULL,
                   age INTEGER
                   )
"""
    )
    cursor.execute("INSERT INTO USERS(id, name, age) values (1, 'Aryan', 22)")
    cursor.execute("INSERT INTO USERS(id, name, age) values (2, 'Modi', 30)")
    cursor.execute("INSERT INTO USERS(id, name, age) values (3, 'Shah', 25)")
    conn.commit()
    conn.close()


init_db()


class UserQuery(BaseModel):
    user_id: str


def make_cache_key(user_id: str):
    raw = f"user_id:{user_id}"
    return hashlib.sha256(raw.encode()).hexdigest()


@app.post("/get-user")
def get_user(query: UserQuery):
    key = make_cache_key(query.user_id)

    cached_result = redis_client.get(key)

    if cached_result:
        print("Serving from Redis cahche!!")
        return json.loads(cached_result)

    connect = get_db_connection()
    cursor = connect.cursor()
    cursor.execute("SELECT * from USERS where id = ?", (query.user_id,))
    user_row = cursor.fetchone()
    connect.close()

    if user_row is None:
        return {"message": "User not found"}

    result = {"id": user_row["id"], "name": user_row["name"], "age": user_row["age"]}
    redis_client.setex(key, 3600, json.dumps(result))
    print("Fetched from DB and cached!")
    return result
