FROM python:3.9

RUN apt get update 

RUN pip install faker great_expectations pandas 

ENTRYPOINT [ "/bin/bash" ]