# Use an official Rust nightly compiler as a parent image
FROM archlinux/base

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

RUN pacman -Sy --noconfirm rustup cmake gcc make intel-tbb openmpi; \
  rustup install nightly; \
  rustup default nightly;

# Build flow cutter -DUSE_KAHIP=OFF
RUN mkdir -p lib/InertialFlowCutter/build/ && cd lib/InertialFlowCutter/build/ && cmake -DCMAKE_BUILD_TYPE=Release .. && make console && cd ../../..

# Install any needed dependencies and compile
RUN cargo build --release -p bmw_routing_engine --bin import_here
RUN cargo build --release -p bmw_routing_server --bin bmw_routing_server

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV ROCKET_ENV prod

# Run server when the container launches
CMD ["bash", "cch_complete.sh"]
