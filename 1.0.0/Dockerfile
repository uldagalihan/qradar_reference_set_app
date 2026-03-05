FROM frikky/shuffle:app_sdk as base

FROM base as builder
RUN apk --no-cache add --update alpine-sdk libffi libffi-dev musl-dev openssl-dev git
RUN mkdir /install
WORKDIR /install

# App'in kendi bağımlılıkları (sadece requests + truststore)
COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip 
 && pip install --no-cache-dir --prefix="/install" -r /requirements.txt 
 && pip install --no-cache-dir --prefix="/install" walkoff-app-sdk

FROM base
COPY --from=builder /install /usr/local

# Eğer base image'da bozuk vendored SDK varsa onu yok et (önlem)
RUN rm -rf /app/walkoff_app_sdk || true

COPY src /app
WORKDIR /app
CMD ["python","app.py","--log-level","DEBUG"]
