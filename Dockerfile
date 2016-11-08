FROM python:3.5

WORKDIR /var/www

RUN apt-get update
RUN apt-get install -y libjudy-dev

COPY ./requirements.txt /var/www/requirements.txt
COPY ./simhash_db /var/www/simhash_db
COPY ./setup.py /var/www/setup.py

RUN pip3 install Cython
RUN pip3 install -r requirements.txt
