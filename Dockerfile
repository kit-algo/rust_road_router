# Use an official Rust nightly compiler as a parent image
FROM rustlang/rust:nightly

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed dependencies and compile
RUN cargo build --release -p bmw_routing_engine --bin import_here
RUN cargo build --release -p bmw_routing_server --bin bmw_routing_server

# Build flow cutter
RUN cd lib/flow-cutter/ && ./build.py --clean --ignore-warnings --no-gpl && cd ../..

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV ROCKET_ENV prod

# Run server when the container launches
CMD ["bash", "cch_complete.sh"]
