FROM golang:1.18

RUN apt-get update 

RUN apt-get install ffmpeg -y

WORKDIR /usr/src/app

COPY go.mod go.sum ./

RUN go mod download && go mod verify

COPY . .

RUN go build -o server

CMD [ "./server" ]