FROM ghcr.io/browserless/chrome:latest

# install system dependencies for pak
USER root
RUN apt-get update && apt-get install -y \
  # C++ compiler required to build packages from source
  g++ \
  # {Matrix}
  gfortran \
  libblas-dev \
  liblapack-dev \
  && rm -rf /var/lib/apt/lists/*
