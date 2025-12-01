FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV prefect_api_url="http://127.0.0.1:4200/api"

CMD ["bash"]
