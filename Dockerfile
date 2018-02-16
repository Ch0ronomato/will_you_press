FROM python:3.5.2

MAINTAINER Ian Schweer <schweer@adobe.com>

ADD ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV PTB_HOME /opt/ptb
RUN useradd -ms /bin/bash -d $PTB_HOME ptb
WORKDIR PTB_HOME

COPY ./src src
RUN python -m nltk.downloader genesis
RUN python -m nltk.downloader stopwords

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["src/etl.py"]
