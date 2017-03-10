#test : sudo docker run -t -i -p 9999:9999 -v /home/file:/home/pi/neural-velib hhoareau/neural-velib
#Remplacement : sudo docker ps
#Construction : cd ./NeuralVelib/docker-dir && sudo docker build -t hhoareau/neural-velib-pc .
#publication : sudo docker login && docker push hhoareau/neural-velib-pc
#Installation : sudo docker run --restart=always -d -p 9999:9999 -v /home/pi/neural-velib:/home/files hhoareau/neural-velib-pc
FROM openjdk

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -q && apt-get install --no-install-recommends -yq wget unzip clinfo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Download and setup AMD OpenCL runtime
RUN export SDK_URL="http://www2.ati.com/drivers/linux-amd-14.41rc1-opencl2-sep19.zip" \
    && wget ${SDK_URL} -q -O download.zip --referer support.amd.com \
    && unzip download.zip \
    && rm -f download.zip \
    && bash fglrx-14.41/amd-driver-installer-*.run --extract scratch \
    && export TGT_DIR="/opt/amd/opencl/lib" \
    && mkdir -p $TGT_DIR \
    && cp -r scratch/arch/x86_64/usr/lib64/* "$TGT_DIR" \
    && ln -s "$TGT_DIR/libOpenCL.so.1" "$TGT_DIR/libOpenCL.so" \
    && mkdir -p /etc/OpenCL/vendors/ \
    && echo "$TGT_DIR/libamdocl64.so" > /etc/OpenCL/vendors/amd.icd \
    && rm -rf scratch fglrx*

# Download the OpenCL 2.0 headers
RUN export TGT_DIR="/opt/amd/opencl/include/CL" \
    && export URL="https://raw.githubusercontent.com/KhronosGroup/OpenCL-Headers/opencl20" \
    && mkdir -p $TGT_DIR && cd $TGT_DIR \
    && for u in opencl cl_platform cl cl_ext cl_gl cl_gl_ext; do \
         wget -q --no-check-certificate $URL/$u.h; \
       done;

# Let the system know where to find the OpenCL library at runtime
ENV OCL_INC /opt/amd/opencl/include
ENV OCL_LIB /opt/amd/opencl/lib
ENV LD_LIBRARY_PATH $OCL_LIB


ADD *.jar /home/
RUN apt-get install -y libgfortran3
RUN apt-get install -y clinfo

#Installation de BLAS
RUN apt-get install -y libatlas3-base libopenblas-base
#RUN update-alternatives --set libblas.so /usr/lib/openblas-base/libblas.so.3
RUN update-alternatives --set libblas.so.3 /usr/lib/openblas-base/libblas.so.3
#RUN update-alternatives --set liblapack.so /usr/lib/openblas-base/liblapack.so.3
RUN update-alternatives --set liblapack.so.3 /usr/lib/openblas-base/liblapack.so.3

WORKDIR /home
EXPOSE 9999 9999
CMD ["java", "-Xmx1024m", "-cp", "/home/NeuralVelib4J.jar","org.neural.Main"]