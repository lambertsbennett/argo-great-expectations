FROM python:3.9

RUN pip install faker great_expectations pandas minio

RUN mkdir /data && mkdir /ge-store

ENTRYPOINT [ "/bin/bash" ]