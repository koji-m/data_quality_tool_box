FROM python:3.9-slim

COPY . /app/

WORKDIR /app

RUN pip install -r requirements.txt

ENV STREAMLIT_SERVER_PORT 8080

ENTRYPOINT [ "streamlit", "run" ]