# Наследуемся от CentOS 7
FROM centos:7

# Выбираем рабочую папку
WORKDIR /root

RUN yum install -y unzip

# Задаем переменные окружения для работы Go
ENV PATH=${PATH}:/usr/local/go/bin GOROOT=/usr/local/go GOPATH=/root/go

ADD highload .

# Открываем 80-й порт наружу
EXPOSE 80

# Запускаем наш сервер
CMD unzip /tmp/data/data.zip -d /root > /dev/null && ./highload
