# pymysql does not work on mysql 8 because of a bug with charsets.
# They take a query from information_schema.collations and hardcode
# it as python objects. See charset.py#46 in pymysql for more details

#FROM mysql:8.0.1 pymysql SQL does not work on 
FROM mysql:5.5

# this is clowntown insecure and should not be emulated in production

ENV MYSQL_ROOT_PASSWORD passwordfortest

ENV MYSQL_DATABASE graphscale-unittest

ENV MYSQL_USER magnus

ENV MYSQL_PASSWORD magnus

# Set the working directory to /app
# WORKDIR /app
# 
# # Copy the current directory contents into the container at /app
# ADD . /app
# 
# # Install any needed packages specified in requirements.txt
# RUN pip install -r requirements.txt
# 
# # Make port 80 available to the world outside this container
# EXPOSE 80
# 
# # Define environment variable
# ENV NAME World
# 
# # Run app.py when the container launches
# CMD ["python", "app.py"]
