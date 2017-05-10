FROM tiangolo/uwsgi-nginx-flask:flask-python3.5

COPY ./app /app
 
RUN pip install --upgrade pip

RUN pip install -r requirements.txt

RUN pip install -e git+https://github.com/schrockntemp/graphscaletemp.git#egg=graphscale
