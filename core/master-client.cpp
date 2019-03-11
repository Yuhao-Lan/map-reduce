#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "rpc_generated/master-worker.grpc.pb.h"
#include "split.h"
#include<stdio.h> 
#include<string.h> 
#include<pthread.h> 
#include<stdlib.h> 
#include<unistd.h> 
  

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::Filename;
using masterworker::Filenames;
using masterworker::Worker;

#define NUM_CHUNK 10;
pthread_t tid1[NUM_CHUNK]; 


struct thread_data {

   string machineip;
   string filename;

};

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
 std::string StartReducer(const std::string& str_filenames) {
  
    Filenames filenames;
    filenames.set_filenames(str_filenames);
    Filename return_filename;
    ClientContext context;
    Status status = stub_->StartReducer(&context, filenames, &return_filename);
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


void* startmapper(void *arg) { 
    //pthread_mutex_lock(&lock); 
    struct thread_data *my_data;
    my_data = (struct thread_data *) arg;

    MasterClient cli(grpc::CreateChannel(my_data->machineip, grpc::InsecureChannelCredentials()));
    std::string input_filename(my_data->filename);
    std::string output_filename = cli.StartMapper(input_filename);
    std::cout << "Worker received: " << output_filename << std::endl;
    //pthread_mutex_unlock(&lock);
    pthread_exit(NULL);
    // return NULL; 
} 

void* startreducer(void *arg) { 

  MasterClient cli(grpc::CreateChannel("myVMDeployed3:50051", grpc::InsecureChannelCredentials()));
  std::string input_filename("split.1.txt_aftermapper");
  std::string output_filename = cli.StartReducer(input_filename);
  std::cout << "Worker-reducer received: " << output_filename << std::endl;  
} 


int main(int argc, char** argv) {
  // upload input file to blob
  if(argc != 2)
    cout << "Usage: ./master <input_file_name>" << endl;
  else {

    string inputfile = argv[1];
    string blobfilename = inputfile + "_blob";
    upload_to_blob(inputfile, blobfilename);
    int error; 
    struct thread_data td1[NUM_CHUNK];
    // split input file into N chunks
    split_file(inputfile,"temp", NUM_CHUNK);

   for(int i = 1; i <= NUM_CHUNK; i++) {

        string inputfile = "./temp." + to_string(i);
        string blob = "split/splitblob." + to_string(i);
        cout << inputfile << endl;
        cout << blob << endl;
        upload_to_blob(inputfile, blob);
        LOG(INFO) << "upload " << inputfile << " to " << blob << " successfully... " << endl;
        cout << "upload " << inputfile << " to " << blob << " successfully... " << endl;
        //assign the blob file name to filename
        td1[i-1].filename = blob;

    }

    // create M clients, where M is the number of worker nodes
    // TODO mutli-threading
    // start N pthreads, each thread selects a client based on round robin, and then calls cli.startmapper();
    // wait all N pthreds to finish, and start reducers

    for(int i = 0; i < NUM_CHUNK; i++) {

      if(i == 0 || i % 3 == 0) {

         td1[i].machineip = "myVMDeployed3:50051";

      } else if(i % 3 == 2) {

         td1[i].machineip = "myVMDeployed4:50051";

      } else if(i % 3 == 1) {

         td1[i].machineip = "myVMDeployed5:50051";
      }

    }

    while(i < NUM_CHUNK) { 
        error = pthread_create(&(tid1[i]), NULL, &startmapper, (void*) &td1[i]); 
        if (error != 0) 
            printf("\nThread can't be created :[%s]", strerror(error)); 
        i++; 
    } 
   

    for(int i = 0; i < NUM_CHUNK; i++) {

      pthread_join(tid1[i], NULL); 

    }

    //startreducer();

    return 0;

  }


}