import random
from datetime import datetime, timedelta

# Define elements
ips = [f"192.168.1.{i}" for i in range(1, 256)]
methods = ["GET", "POST", "PUT", "DELETE"]
urls = ["/home", "/products", "/cart", "/checkout", "/search", "/product/1", "/product/2"]
status_codes = [200, 304, 404, 500, 503]
user_agents = ["Mozilla/5.0 (Windows)", "Mozilla/5.0 (Mac)", "Mozilla/5.0 (Mobile)"]

with open("spark-data/ecommerce/web_logs.txt", "w") as f:
    for _ in range(10000):
        ip = random.choice(ips)
        timestamp = (datetime.now() - timedelta(minutes=random.randint(0, 10000))).strftime("%d/%b/%Y:%H:%M:%S %z")
        method = random.choice(methods)
        url = random.choice(urls)
        status = random.choice(status_codes)
        response_time = random.randint(50, 5000)  # in ms
        user_agent = random.choice(user_agents)
        line = f'{ip} - - [{timestamp}] "{method} {url} HTTP/1.1" {status} {response_time} "{user_agent}"\n'
        f.write(line)

print("10,000 synthetic log lines generated.")
