# Описание

Заключительное задание первого модуля.

Реализован отказоустойчивый перенос данных из Postgres в Elasticsearch

# Инструкция к запуску:

## Билд

Сервисы Postgres, Redis и Elasticsearch запускаются из контейнеров.
Если установлен Postgres, то перед запуском остановите сервис командой:

     sudo service postgresql stop

Заполните .env файл по примеру из env.sample и запустить билд командой docker: 

    sudo docker-compose build --no-cache


Запустить контейнеры командой:

    sudo docker-compose up -d

Здесь поднимутся все инфраструктурные сервисы.
Перейти в нужную директорию и запустить скрипт etl командой:

    cd etl/ | python3 main.py
