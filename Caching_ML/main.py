import redis
import hashlib
import joblib
import json
from fastapi import FastAPI
from pydantic import BaseModel, StrictInt, Field

app = FastAPI()

redis_client = redis.Redis(host="localhost", port=6379, db=0)

model = joblib.load("model.joblib")


class HousingData(BaseModel):
    longitude: float
    latitude: float
    housing_median_age: StrictInt
    total_rooms: StrictInt = Field(..., gt=0)
    total_bedrooms: StrictInt = Field(..., gt=0)
    population: int = Field(..., gt=0)
    households: int = Field(..., gt=0)
    median_income: float = Field(..., gt=0)

    def to_list(self):
        return [
            self.longitude,
            self.latitude,
            self.housing_median_age,
            self.total_rooms,
            self.total_bedrooms,
            self.population,
            self.households,
            self.median_income,
        ]

    def cache_key(self):
        data_dict = self.model_dump()
        raw = json.dumps(data_dict, sort_keys=True)
        return f"prediction : {hashlib.sha256(raw.encode()).hexdigest()}"


@app.post("/predict")
async def predict(data: HousingData):
    key = data.cache_key()

    cached_result = redis_client.get(key)
    if cached_result:
        print("Serving prediction from cache")
        return json.loads(cached_result)

    prediction = model.predict([data.to_list()])[0]

    result = {"prediction": int(prediction)}
    redis_client.set(key, json.dumps(result), ex=3600)
    return result
