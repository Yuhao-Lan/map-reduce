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
pthread_t tid1[3]; 
pthread_t tid2[3]; 


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
   
  //  std::string StartReducer(const std::string& str_filename) {
  //   // Data we are sending to the server.
  //   Filename filename;
  //   filename.set_filename(str_filename);

  //   // Container for the data we expect from the server.
  //   Filename return_filename;

  //   // Context for the client. It could be used to convey extra information to
  //   // the server and/or tweak certain RPC behaviors.
  //   ClientContext context;

  //   // The actual RPC.
  //   Status status = stub_->StartReducer(&context, filename, &return_filename);

  //   // Act upon its status.
  //   if (status.ok()) {
  //     return return_filename.filename();
  //   } else {
  //     std::cout << status.error_code() << ": " << status.error_message() << std::endl;
  //     return "RPC failed";
  //   }
  // }

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
    //pthread_mutex_lock(&lock); 
  
    struct thread_data *my_data;
    my_data = (struct thread_data *) arg;

    MasterClient cli(grpc::CreateChannel(my_data->machineip, grpc::InsecureChannelCredentials()));
    std::string input_filename(my_data->filename);
    std::string output_filename = cli.StartReducer(input_filename);
    std::cout << "Worker received: " << output_filename << std::endl;

  
    //pthread_mutex_unlock(&lock);
    pthread_exit(NULL);
    // return NULL; 
} 


int main(int argc, char** argv) {
  // upload input file to blob

  // split input file into N chunks

  // create M clients, where M is the number of worker nodes
  // TODO mutli-threading

  // start N pthreads, each thread selects a client based on round robin, and then calls cli.startmapper();

  // wait all N pthreds to finish, and start reducers

  int i = 0; 
  int error; 
  struct thread_data td1[3];
  struct thread_data td2[3];

  td1[0].machineip = "myVMDeployed3:50051";
  td1[0].filename = "split.1.txt";
  td1[1].machineip = "myVMDeployed4:50051";
  td1[1].filename = "split.2.txt";
  td1[2].machineip = "myVMDeployed5:50051";
  td1[2].filename = "split.3.txt";


    
  while(i < 3) { 
      error = pthread_create(&(tid1[i]), NULL, &startmapper, (void*) &td1[i]); 
      if (error != 0) 
          printf("\nThread can't be created :[%s]", strerror(error)); 
      i++; 
  } 
  pthread_join(tid1[0], NULL); 
  pthread_join(tid1[1], NULL); 
  pthread_join(tid1[2], NULL); 

  td2[0].machineip = "myVMDeployed3:50051";
  td2[0].filename = "split.1.txt_aftermapper";
  td2[1].machineip = "myVMDeployed4:50051";
  td2[1].filename = "split.2.txt_aftermapper";
  td2[2].machineip = "myVMDeployed5:50051";
  td2[2].filename = "split.3.txt_aftermapper";

  while(i < 3) { 
      error = pthread_create(&(tid2[i]), NULL, &startreducer, (void*) &td2[i]); 
      if (error != 0) 
          printf("\nThread can't be created :[%s]", strerror(error)); 
      i++; 
  } 

  pthread_join(tid2[0], NULL); 
  pthread_join(tid2[1], NULL); 
  pthread_join(tid2[2], NULL); 



  
  // MasterClient cli1(grpc::CreateChannel("myVMDeployed5:50051", grpc::InsecureChannelCredentials()));
  // std::string input_filename1("this_is_from_5.txt");
  // std::string output_filename1 = cli1.StartMapper(input_filename1);
  // std::cout << "Worker received: " << output_filename1 << std::endl;

  // MasterClient cli2(grpc::CreateChannel("myVMDeployed4:50051", grpc::InsecureChannelCredentials()));
  // std::string input_filename2("this_is_from_4.txt");
  // std::string output_filename2 = cli2.StartMapper(input_filename2);
  // std::cout << "Worker received: " << output_filename2 << std::endl;

  return 0;
}