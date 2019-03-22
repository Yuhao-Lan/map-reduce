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
#include <glog/logging.h>
#include <glog/raw_logging.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::Filename;
using masterworker::Filenames;
using masterworker::Worker;


pthread_t tid1[NUM_CHUNK]; 



struct thread_data {

   string machineip;
   string filename;

};

std::vector<thread_data> redomapping;
string MACHINEONE = "myVMDeployed3:50051";
string MACHINETWO = "myVMDeployed4:50051";
string MACHINETHREE = "myVMDeployed5:50051";
bool MACHINEONEOK = true;
bool MACHINETWOOK = true;
bool MACHINETHREEOK = true;





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
    // //set the timer
    // std::chrono::steady_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    // context.set_deadline(deadline);

    // The actual RPC.
    Status status = stub_->StartMapper(&context, filename, &return_filename);

    // Act upon its status.
    if (status.ok()) {
      return return_filename.filename();
    } else {
      //status is not ok

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
    //RPC failed or splitblob.499.map 
    //store the failer results
    //TODO add the timer

    if(output_filename == "RPC failed") {

      if(my_data->machineip == MACHINEONE)
        MACHINEONEOK = false;

      if(my_data->machineip == MACHINETWO)
        MACHINETWOOK = false;

      if(my_data->machineip == MACHINETHREE)
        MACHINETHREEOK = false;

      redomapping.push_back({my_data->machineip, input_filename});

    }


    std::cout << "Worker received: " << output_filename << std::endl;
    int count = 0;

    //pthread_mutex_unlock(&lock);
    pthread_exit(NULL);
    // return NULL; 
} 

void startreducer() { 

  MasterClient cli(grpc::CreateChannel(MACHINEONE, grpc::InsecureChannelCredentials()));
  std::string input_filenames("mapresults/");
  std::string output_filename = cli.StartReducer(input_filenames);
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
    download_file("splitblob.5","split/splitblob.5");
    // create M clients, where M is the number of worker nodes
    // TODO mutli-threading
    // start N pthreads, each thread selects a client based on round robin, and then calls cli.startmapper();
    // wait all N pthreds to finish, and start reducers

    for(int i = 0; i < NUM_CHUNK; i++) {

      if(i == 0 || i % 3 == 0) {

         td1[i].machineip = MACHINEONE;

      } else if(i % 3 == 2) {

         td1[i].machineip = MACHINETWO;

      } else if(i % 3 == 1) {

         td1[i].machineip = MACHINETHREE;
      }

    }

    for(int i = 0; i < NUM_CHUNK; i++) { 

        error = pthread_create(&(tid1[i]), NULL, &startmapper, (void*) &td1[i]); 

        if (error != 0) 
            printf("\nThread can't be created :[%s]", strerror(error)); 
       
    } 
   

    for(int i = 0; i < NUM_CHUNK; i++) {

      pthread_join(tid1[i], NULL); 

    }



    //check which part needs redo:

    int REMAP_SIZE = redomapping.size();


    if(REMAP_SIZE != 0) {

      cout << "Rescheduling the failed worker tasks..." << endl;

      int count = 0;

      struct thread_data td2[REMAP_SIZE];

      for(int i = 0; i < redomapping.size(); i++) {

        if(MACHINEONEOK) {

          cout << "Rescheduling to Machine: " << MACHINEONE << endl;

          td2[i].machineip = MACHINEONE;

        } else if(MACHINETWOOK) {

          cout << "Rescheduling to Machine: " << MACHINETWO << endl;

          td2[i].machineip = MACHINETWO;

        } else {

          cout << "Rescheduling to Machine: " << MACHINETHREE << endl;

          td2[i].machineip = MACHINETHREE;

        }
        td2[i].filename = redomapping[i].filename;
      }
    

     for(int i = 0; i < REMAP_SIZE; i++) { 

        error = pthread_create(&(tid2[i]), NULL, &startmapper, (void*) &td2[i]); 

        if (error != 0) 
            printf("\nThread can't be created :[%s]", strerror(error)); 
       
    } 
   
    for(int i = 0; i < REMAP_SIZE; i++) {

      pthread_join(tid2[i], NULL); 

    }

  }



    startreducer();

    return 0;

  }


}