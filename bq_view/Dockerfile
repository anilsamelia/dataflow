FROM python:3.9.7
RUN set -ex; \
    mkdir -p /techshed_apps/config/ && cd /techshed_apps

COPY . ./techshed_apps
WORKDIR /techshed_apps

RUN pip install --no-cache-dir -r requirements.txt
#ENV GOOGLE_APPLICATION_CREDENTIALS=service-account.json
CMD [ "python", "./run.py", "--config", "config/config.yml" ]
