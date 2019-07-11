FROM terrillo/python3flask:latest
MAINTAINER Khoa Nguyen Dang

ENV STATIC_URL /static
ENV STATIC_PATH /app/static

COPY ./app /app
WORKDIR /app

RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./bash/nginx.sh /nginx.sh
RUN chmod +x /nginx.sh

ENV PYTHONPATH=/app
# App environment
ENV FLASK_APP=run.py
ENV SECRET=b'\xdd\xa2\xb3\xcf0\x86m\x93\x9e\xee\xfe\x168\xb3\xb2:\xf5\xdf\xa3\x1b:\xad\xe1\r'
ENV APP_SETTINGS=development
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENTRYPOINT ["/nginx.sh"]

CMD ["/start.sh"]

EXPOSE 80 443
