FROM python:3.12-slim

WORKDIR /src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY notification_adapter.py .

RUN useradd --uid 10001 --create-home adapter
USER 10001

EXPOSE 8080
ENTRYPOINT ["python", "/src/notification_adapter.py"]
