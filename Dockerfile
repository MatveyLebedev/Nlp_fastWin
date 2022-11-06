FROM ubuntu:latest

COPY for_docker/PredictorMain.py PredictorMain.py

RUN sudo apt-get update -y
RUN sudo apt-get install -y python3.8
RUN sudo pip3.8 install psycopg2-binary
RUN sudo pip3.8 install wiki_ru_wordnet
RUN sudo pip3.8 install pymorphy2

RUN sudo python3.8 PredictorMain.py
