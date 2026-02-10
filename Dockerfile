FROM m.daocloud.io/docker.io/library/python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
COPY . .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "19898"]
