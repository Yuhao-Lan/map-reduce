#!/bin/bash
mkdir rpc_generated
protoc master-worker.proto --cpp_out=rpc_generated
protoc master-worker.proto --grpc_out=rpc_generated --plugin=protoc-gen-grpc=/usr/local/bin/grpc_cpp_plugin
