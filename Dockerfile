FROM confluentinc/cp-kafka:7.3.0

COPY create_topics.sh /create_topics.sh
USER root
RUN chmod +x /create_topics.sh
ENTRYPOINT ["/bin/bash", "/create_topics.sh"]