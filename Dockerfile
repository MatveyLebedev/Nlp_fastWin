FROM python:3.8

COPY /for_docker/PredicterMain.py  /home/mehdi/Download/PredicterMain.py

RUN python -m pip install --upgrade pip
RUN pip3 install pandas
RUN pip3 install psycopg2-binary
RUN pip3 install wiki_ru_wordnet
RUN pip3 install pymorphy2

RUN cd /home/mehdi/Download/

CMD ["python","/home/mehdi/Download/PredicterMain.py"]

# docker build -t nlp-fw .