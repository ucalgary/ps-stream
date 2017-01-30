FROM python:3.6.0-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app
RUN apk add --no-cache --virtual .build-deps \
      gcc && \
    pip install -r requirements.txt && \
		apk del .build-deps

LABEL maintainer King Chung Huang <kchuang@ucalgary.ca>
