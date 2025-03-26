FROM python:3.10-slim-bullseye

RUN apt-get update
RUN pip install mlflow==2.14.3


ENTRYPOINT ["mlflow", "server", "--host", "0.0.0.0"]