FROM frikky/shuffle:app_sdk as base

FROM base as builder
RUN apk --no-cache add --update alpine-sdk libffi libffi-dev musl-dev openssl-dev git
RUN mkdir /install
WORKDIR /install
COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip && pip install --no-cache-dir --prefix="/install" -r /requirements.txt

FROM base
COPY --from=builder /install /usr/local
COPY src /app
WORKDIR /app
CMD python app.py --log-level DEBUG
