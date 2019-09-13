FROM openjdk:8u151-jdk-stretch

#MAKE THE SAMbA Diretory
RUN mkdir -p /SAMbAFiles/SAMbA
RUN mkdir -p /SAMbAFiles/WebInterface
RUN mkdir -p /SAMbAFiles/apache-cassandra

#Copying the files
COPY SAMbA /SAMbAFiles/SAMbA
COPY apache-cassandra /SAMbAFiles/apache-cassandra
COPY SAMbAWebApplication.jar /SAMbAFiles/WebInterface
COPY scala /scala
COPY startSAMbA /bin
COPY StartCassandra.sh /

#Setting ROOT ENVs
ENV SPARK_HOME /SAMbAFiles/SAMbA
ENV WORKSPACE /home/samba/workspace
ENV SAMBA_SPARK_REPOSITORY /SAMbAFiles/SAMbA/gitRepository

#Softwares
RUN useradd -ms /bin/bash samba
RUN apt update && apt install nano sudo libfuse2 fuse
RUN chmod +x /bin/startSAMbA

#Setting the gitconfig
COPY gitconfig /home/samba/
RUN mv /home/samba/gitconfig /home/samba/.gitconfig

#######Setting ports
#Web Interface
EXPOSE 8000
#Cassandra database
EXPOSE 9042

#Changing the owner of folder
RUN echo "export SAMBA_SPARK_CLASS_PATH='$( ls $SPARK_HOME/jars/*.jar | paste -sd ':' -)'" >> /home/samba/.bashrc
RUN echo "export PATH=:/SAMbAFiles/SAMbA/sbin/:/SAMbAFiles/SAMbA/bin/:/scala/bin/:$PATH" >> /home/samba/.bashrc

ENV PATH "/scala/bin/:${PATH}"
ENV PATH "/SAMbAFiles/SAMbA/bin/:${PATH}"
ENV PATH "/SAMbAFiles/SAMbA/sbin/:${PATH}"
RUN echo "export SAMBA_SPARK_CLASS_PATH='$( ls $SPARK_HOME/jars/*.jar | paste -sd ':' -)'" >>  ~/.bashrc

RUN chown -R samba /SAMbAFiles

VOLUME /SAMbAFiles/SAMbA/gitblit/data/git
VOLUME /SAMbAFiles/apache-cassandra/data
VOLUME /home/samba/workspace

RUN echo "samba ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/samba
RUN chmod 0440 /etc/sudoers.d/samba

#Starting
WORKDIR /home/samba/workspace
USER samba
CMD /bin/startSAMbA && /bin/bash
