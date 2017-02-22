FROM jsurf/rpi-java
ADD *.jar /home/
RUN apt-get update

EXPOSE 9999 9999
CMD ["/usr/lib/jvm/java-8-oracle/bin/java", "-Xmx2048m", "-cp", "/home/NeuralVelib4J.jar","org.neural.Main"]