FROM python:3.11-slim
WORKDIR /home/ubuntu/

LABEL authors="BN"
RUN apt-get update && apt-get upgrade -y \
    build-essential \
    software-properties-common \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/throneofshadow/RTMonitoringApp.git .
RUN pip3 install -r requirements.txt
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["top", "-b"]