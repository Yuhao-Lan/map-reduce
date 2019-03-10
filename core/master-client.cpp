#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "rpc_generated/master-worker.grpc.pb.h"
//#include "split.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::Filename;
using masterworker::Filenames;
using masterworker::Worker;

class MasterClient {
 public:
  MasterClient(std::shared_ptr<Channel> channel)
      : stub_(Worker::NewStub(channel)) {}

  // Assambles the client's payload, sends it and presents the response back
  // from the server.
  std::string StartMapper(const std::string& str_filename) {
    // Data we are sending to the server.
    Filename filename;
    filename.set_filename(str_filename);

    // Container for the data we expect from the server.
    Filename return_filename;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->StartMapper(&context, filename, &return_filename);

    // Act upon its status.
    if (status.ok()) {
      return return_filename.filename();
    } else {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
};

int main(int argc, char** argv) {
  // upload input file to blob

  // split input file into N chunks

  // create M clients, where M is the number of worker nodes

  // start N pthreads, each thread selects a client based on round robin, and then calls cli.startmapper();

  // wait all N pthreds to finish, and start reducers
  MasterClient cli = new MasterClient(grpc::CreateChannel("myVMDeployed5:50051", grpc::InsecureChannelCredentials()));
  std::string input_filename("this_is_the_file_for_map_reduce.txt");
  std::string output_filename = cli.StartMapper(input_filename);
  std::cout << "Worker received: " << output_filename << std::endl;

  return 0;
}