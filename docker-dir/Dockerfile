#test : sudo docker run --restart=always -t -i -p 9999:9999 -p 9998:4040 -v /home/file:/home/pi/neural-velib hhoareau/neural-velib
#Remplacement : sudo docker ps
#Construction : cd ./NeuralVelib/docker-dir && sudo docker build -t hhoareau/neural-velib .
#Installation : sudo docker run --restart=always -d -p 9999:9999 -p 9998:4040 -v /home/pi/neural-velib:/home/files hhoareau/neural-velib
FROM openjdk
ADD /out/artifacts/*.jar /home/
RUN apt-get update

#Installation de BLAS
RUN apt-get install gfortran
RUN apt-get install libatlas3-base libopenblas-base
#RUN update-alternatives --set libblas.so /usr/lib/openblas-base/libblas.so.3
RUN update-alternatives --set libblas.so.3 /usr/lib/openblas-base/libblas.so.3
#RUN update-alternatives --set liblapack.so /usr/lib/openblas-base/liblapack.so.3
RUN update-alternatives --set liblapack.so.3 /usr/lib/openblas-base/liblapack.so.3

WORKDIR /home
RUN mkdir files
VOLUME /files

EXPOSE 9999
EXPOSE 4040

CMD ["java", "-Xmx1024m", "-cp", "/home/NeuralVelib4J.jar","org.neural.Main"]
