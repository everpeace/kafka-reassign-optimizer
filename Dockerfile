FROM python:2.7-slim
MAINTAINER Shingo Omura <everpeace@gmail.com>

WORKDIR /opt
COPY requirements.txt /tmp/requirements.txt
COPY kafka-reassign-optimizer.py kafka-reassign-optimizer.py
RUN pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

CMD ["python", "kafka-reassign-optimizer.py"]
