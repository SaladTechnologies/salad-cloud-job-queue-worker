# syntax=docker/dockerfile:1

FROM python:3.13.3-slim-bookworm

EXPOSE 80/tcp

WORKDIR /app
COPY ./requirements.txt *.py ./
RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["fastapi", "run", "/app/main.py", "--host", "*", "--port", "80"]
