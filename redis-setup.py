import redis

r = redis.Redis(host="localhost", port=6379, db=0)

try:
    if r.ping():
        print("Connected to the redis server!")

except redis.ConnectionError:
    print("Redis Connection failed!")

r.set("Brain", "relaxed")

value = r.get("Brain")

print(f"Brain is {value.decode()}")
