Билд образа `docker build -t viewers-api .`

Запуск образа `docker run -d -p 7702:7702 viewers-api`

Просмотр запущенных контейнеров `docker ps`

Остановка контейнера `docker stop [ID_КОНТЕЙНЕРА]`

Удаление контейнера `docker rm [ID_КОНТЕЙНЕРА]`

Просмотр всех образов `docker images`

Строгое удаление образа `docker rmi [ID_ОБРАЗА]`

Остановка и удаление контейнера одной командой `docker rm -f <container_id_or_name>`