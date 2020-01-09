# Docker file bundling gcp sdk and python 3.6
# To push changes to gitlab run
# docker build -t registry.gitlab.com/potato-oss/google-cloud/django-gcloud-connectors/python36 .
# docker push registry.gitlab.com/potato-oss/google-cloud/django-gcloud-connectors/python36:latest

FROM python:3.6-buster

RUN echo "deb http://ftp.us.debian.org/debian sid main" | tee /etc/apt/sources.list.d/sid-main.list
RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-buster main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-get update
RUN apt-get -y install google-cloud-sdk google-cloud-sdk-datastore-emulator google-cloud-sdk
RUN pip install tox