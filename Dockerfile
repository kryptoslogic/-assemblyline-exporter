FROM python:3.9-slim-bullseye

COPY . /exporter

WORKDIR /exporter

RUN pip install -r requirements.txt

EXPOSE 8000
ENTRYPOINT ["python3", "-m", "assemblyline_exporter.exporter"] 
