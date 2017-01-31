FROM python:3.6.0-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app
RUN apk add --no-cache --virtual .build-deps \
      gcc \
      musl-dev && \
    pip install -r requirements.txt && \
		apk del .build-deps

COPY setup.py /usr/src/app
COPY pssync /usr/src/app/pssync
RUN python setup.py install

ENTRYPOINT ["/usr/local/bin/pssync"]
CMD ["--help"]

LABEL maintainer King Chung Huang <kchuang@ucalgary.ca>
