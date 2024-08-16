protoc heartbeat.proto --go_out=. --go_opt=paths=source_relative 
protoc membership.proto --go_out=plugins=grpc:. 
protoc service.proto --go_out=plugins=grpc:. 