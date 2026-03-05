FROM frikky/shuffle:qradar-reference-set-loader_1.1.2

# Only overwrite the app entrypoint, keep the working SDK inside the base image
COPY src/app.py /app/app.py

WORKDIR /app
CMD python app.py --log-level DEBUG
