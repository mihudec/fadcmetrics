FROM python:3.10-bullseye

ENV branch=v0.1.0-beta

# RUN apk update && apk add git
WORKDIR /usr/local/src


RUN git clone https://github.com/mihudec/fadcclient.git
RUN git clone https://github.com/mihudec/fadcmetrics.git

RUN pip3 install -e fadcclient/
RUN pip3 install -e fadcmetrics/

CMD ["fadcmetrics", "--config-file" "/config/fadcmetrics.yml"]