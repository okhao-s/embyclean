FROM python:3.9-slim

WORKDIR /app

# 设置时区
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制源码
COPY . .

# 挂载点和端口 (这里修改为 19898)
VOLUME ["/app/data"]
EXPOSE 19898

# 启动命令 (端口修改为 19898)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "19898"]
