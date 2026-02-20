FROM python:3.11-slim

WORKDIR /scripts

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./scripts /scripts
COPY ./data /data

CMD ["python", "/scripts/setup_import.py"]
