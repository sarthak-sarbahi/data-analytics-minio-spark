### Getting started
- Set up Docker Desktop and WSL 2 on Windows: https://www.youtube.com/watch?v=2ezNqqaSjq8
- Set up JupyterLab and minIO using Docker
    - Prepare docker-compose.yml
    - Run `docker-compose up -d` in root directory
    - Access minIO web portal using http://localhost:9001
    - Create bucket using web portal: `mybucket`
    - Access JupyterLab using URL in logs (can find in Docker Desktop)

### Scripts
- Analysis notebook: `country_analysis.ipynb`

### Extra
- Download JAR files (Using **PySpark 3.5.0** and **Hadoop 3.3.4**)
    > wget -O hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
    
    > wget -O aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar
- Access docker container as root user: `docker exec -it --user root <container_name_id> /bin/bash`
- `docker ps` (show running containers)
- Maven repository: https://mvnrepository.com/
- Remove docker containers running: `docker-compose down` (This only deletes the containers but not the images)
- API data fields: https://gitlab.com/restcountries/restcountries/-/blob/master/FIELDS.md