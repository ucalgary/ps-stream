FROM python:3.6.0-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

LABEL maintainer King Chung Huang <kchuang@ucalgary.ca>
