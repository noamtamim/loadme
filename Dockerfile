FROM python:3.9.5-slim

RUN pip install -U pip wheel && pip install requests

COPY entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

