#test : sudo docker run -t -i -p 9999:9999 -v /home/file:/home/pi/neural-velib hhoareau/neural-velib
#Remplacement : sudo docker ps
#Construction : cd ./NeuralVelib/docker-dir && sudo docker build -t hhoareau/neural-velib .
#Installation : sudo docker run --restart=always -d -p 9999:9999 -v /home/pi/neural-velib:/home/files hhoareau/neural-velib
FROM jsurf/rpi-java
ADD *.jar /home/
RUN apt-get update

#Installation de BLAS


WORKDIR /home
EXPOSE 9999 9999
CMD ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xmx1024m", "-cp", "/home/NeuralVelib4J.jar","org.neural.Main"]