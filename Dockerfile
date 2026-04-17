FROM tabulario/spark-iceberg:latest

COPY init/02-apply-ddl.sh /opt/init/02-apply-ddl.sh
COPY init/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY ddl/ /opt/ddl/

RUN chmod +x /opt/init/02-apply-ddl.sh

ENTRYPOINT ["/bin/bash", "/opt/init/02-apply-ddl.sh"]