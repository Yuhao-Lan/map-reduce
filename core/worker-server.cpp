#include <iostream>
#include <memory>
#include <grpc++/grpc++.h>
#include <string>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include "rpc_generated/master-worker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::Filenames;
using masterworker::Filename;
using masterworker::Worker;

// Logic and data behind the server's behavior.
class WorkerServiceImpl final : public Worker::Service {

  Status StartMapper(ServerContext* context, 
    const Filename* request, Filename* response) override {
        LOG(INFO) << "A mapper is running with input file: " <<  request->filename();
        //download 

        //exec 
        std::string command = "cat " + request->filename() + " | python mapper.py > " + request->filename() + "_aftermapper";
        LOG(INFO) << "A mapper is running command: " <<  command;
        system(command.c_str());

        //upload
        response->set_filename("Hello " + request->filename());
        LOG(INFO) << "The mapper is done with output file: ";
        return Status::OK;
  }
  
  Status StartReducer(ServerContext* context, 
    const Filenames* request, Filename* response) override {
        LOG(INFO) << "A reducer is running with input files: " <<  request->filenames();
        // download all the mappers' output files
        // exec sort, [partition]
        // exec("cat filename | python reduce.py > output.txt");


        std::string command = "cat " + request->filenames() + " | sort | python reducer.py > " + request->filenames() + "| sort -k2nr";
        LOG(INFO) << "A reducer is running command: " <<  command;
        system(command.c_str());

        // upload(output.txt);
        response->set_filename("Hello " + request->filenames());
        LOG(INFO) << "The reducer is done with output file: ";
        return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  WorkerServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}