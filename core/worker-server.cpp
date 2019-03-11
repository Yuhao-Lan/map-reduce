#include <iostream>
#include <memory>
#include <grpc++/grpc++.h>
#include <string>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include "rpc_generated/master-worker.grpc.pb.h"
#include <sys/types.h> 
#include <sys/wait.h>
#include <vector> 
typedef basic_stringstream<char>  stringstream;
typedef basic_string<char> string;
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

        pid_t pid = fork();

        if(pid == -1) {

          LOG(WARNING) << hostname << ".Mapper(" <<  request->filename() << ") failed";
          return Status::FAIL;

        }
        else if (pid == 0) {

           string blobpath = request->filename(); 
            // Vector of string to save tokens 
            vector <string> tokens; 
            // stringstream class check1 
            stringstream check1(blobpath); 
            string intermediate; 
            // Tokenizing w.r.t. space ' ' 
            while(getline(check1, intermediate, '/')) { 
                tokens.push_back(intermediate); 
            } 
              
            LOG(INFO) << "A mapper is downloading : " <<  download_file;
            LOG(INFO) << "The blobpath is : " <<  blobpath;
            string download_file = tokens[1];
            //download 
            download_file(download_file,blobpath);
            //exec 
            std::string command = "cat " + download_file + " | python mapper.py > " + download_file + "_aftermapper";
            string file_after_mapper = download_file + "_aftermapper";
            LOG(INFO) << "A mapper is running command: " <<  command;
            system(command.c_str());
            string blobname = "mapper/" + file_after_mapper;
            //upload
            upload_to_blob(file_after_mapper,blobname);

        } else {

          int status;
          waitpid(pid, &status,0);

        }








        response->set_filename("Finished:  " + request->filename());
        LOG(INFO) << "The mapper is done with output file: ";
        return Status::OK;
  }

  Status StartReducer(ServerContext* context, 
    const Filenames* request, Filename* response) override {
        LOG(INFO) << "A reducer is running with input files: " <<  request->filenames();
        // download all the mappers' output files
        // exec sort, [partition]
        // exec("cat filename | python reduce.py > output.txt");


        std::string command = "cat " + request->filenames() + " | sort | python reducer.py | sort -k2nr > " + request->filenames() + "_final";
        LOG(INFO) << "A reducer is running command: " <<  command;
        system(command.c_str());

        // upload(output.txt);
        response->set_filename("Finished:  " + request->filenames());
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