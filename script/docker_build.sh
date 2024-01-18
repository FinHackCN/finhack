docker rmi $(docker images -f "dangling=true" -q)
docker rmi -f finhack
docker rm -f finhack-docker
docker build -t finhack .
docker run -d --name finhack-docker finhack

