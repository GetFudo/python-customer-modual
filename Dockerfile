FROM python:3.6
RUN apt-get update && \
    apt-get install nodejs npm -y && \
    apt-get clean
RUN npm install -g pm2

RUN mkdir /searchApi
WORKDIR /searchApi
ADD ./req.txt /searchApi/


RUN pip3.6 install --upgrade virtualenv
RUN virtualenv -p python3.6 fenve
RUN . /searchApi/fenve/bin/activate
RUN pip3.6 install -r req.txt
ADD . /searchApi/
ENV PYTHONUNBUFFERED TRUE
RUN pm2 start echo --interpreter=python
EXPOSE 8002
# # CMD ["gunicorn", "-bind", "0.0.0.0:8002", "search_api.wsgi", "--timeout 120"]
# CMD exec gunicorn --bind 0.0.0.0:8002 --workers 1 --threads 8 --timeout 0 search_api.wsgi:application
ENTRYPOINT ["pm2-runtime", "start", "ecosystem.config.js"]