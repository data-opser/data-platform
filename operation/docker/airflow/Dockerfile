FROM apache/airflow:2.10.5

COPY requirements.txt .

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/.config/gcloud/application_default_credentials.json
ENV ENV=prod

RUN pip install -r requirements.txt