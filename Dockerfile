FROM ucalgary/python-librdkafka:3.7.0-0.11.6

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY setup.py /usr/src/app
COPY ps_stream /usr/src/app/ps_stream
RUN apk add --no-cache --virtual .build-deps \
      gcc \
      git \
      musl-dev && \
    python setup.py install && \
    apk del .build-deps

ENTRYPOINT ["/usr/local/bin/ps-stream"]
CMD ["--help"]

LABEL maintainer="King Chung Huang <kchuang@ucalgary.ca>" \
      org.label-schema.vcs-url="https://github.com/ucalgary/ps-stream"
