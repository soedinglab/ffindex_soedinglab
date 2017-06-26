FROM alpine:latest as ffindex-builder

RUN apk add --no-cache gcc cmake musl-dev ninja

WORKDIR /opt/ffindex
ADD . .

WORKDIR build
RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=. ..
RUN ninja && ninja install

FROM alpine:latest
MAINTAINER Milot Mirdita <milot@mirdita.de>
RUN apk add --no-cache gawk bash grep libstdc++

COPY --from=ffindex-builder /opt/ffindex/build/bin /usr/local/bin/
