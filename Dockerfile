FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.2

COPY . /res

ENV discovery.type=single-node

RUN sh /res/build.sh

EXPOSE 9200 9300

ENTRYPOINT ["/bin/bash"]

CMD []