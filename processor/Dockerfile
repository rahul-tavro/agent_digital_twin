FROM tabulario/spark-iceberg:latest

USER root

COPY requirements.txt /opt/processor/requirements.txt
RUN pip install -r /opt/processor/requirements.txt --break-system-packages

COPY webhook.py /opt/processor/webhook.py
COPY process.py /opt/processor/process.py

WORKDIR /opt/processor

EXPOSE 8000

ENTRYPOINT []
CMD ["python", "webhook.py"]