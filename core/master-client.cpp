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
#include <fstream>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::Filename;
using masterworker::Filenames;
using masterworker::Worker;
using namespace std;
ofstream log_file;
#include <thread>         // std::this_thread::sleep_for
#include <chrono>         // std::chrono::seconds






pthread_t tid1[NUM_CHUNK]; 

struct thread_data {

   string machineip;
   string filename;

};

std::queue<thread_data> redomapping;
std::queue<thread_data> redoreducing;
std::queue<thread_data> replicating;
string MACHINEONE = "myVMDeployed3:50051";
string MACHINETWO = "myVMDeployed4:50051";
string MACHINETHREE = "myVMDeployed5:50051";
string REDUCERMAHINE;
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

      redomapping.push({my_data->machineip, input_filename});


    } 
    //keep track of inputfile successfully
    
    log_file.open("log_file.txt", fstream::app);

    if (log_file.is_open())
      log_file << output_filename + "\n";
    else
      cout << "log_file failed to open" << endl;

    upload_to_blob("log_file.txt","log/log_file.txt");

    log_file.close();

    cout << "Worker received: " << output_filename << std::endl;

    //pthread_mutex_unlock(&lock);
    pthread_exit(NULL);
    // return NULL; 
} 

void startreducer() { 

  if(MACHINEONEOK)
    REDUCERMAHINE = MACHINEONE;
  else if(MACHINETWOOK)
    REDUCERMAHINE = MACHINETWO;
  else
    REDUCERMAHINE = MACHINETHREE;

  MasterClient cli(grpc::CreateChannel(REDUCERMAHINE, grpc::InsecureChannelCredentials()));
  std::string input_filenames("mapresults1/");
  std::string output_filename = cli.StartReducer(input_filenames);

  if(output_filename == "RPC failed") {

      if(REDUCERMAHINE == MACHINEONE)
        MACHINEONEOK = false;

      if(REDUCERMAHINE == MACHINETWO)
        MACHINETWOOK = false;

      if(REDUCERMAHINE == MACHINETHREE)
        MACHINETHREEOK = false;

      redoreducing.push({REDUCERMAHINE, input_filenames});

  } 


  std::cout << "Worker-reducer received: " << output_filename << std::endl;  
} 

void split_and_map_process(string inputfile) {

  struct thread_data td1[NUM_CHUNK];
  string blobfilename = inputfile + "_blob";
  upload_to_blob(inputfile, blobfilename);
  int error; 
  // split input file into N chunks
  split_file(inputfile,"temp", NUM_CHUNK);
  //uopload split files
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

   log_file.open("log_file.txt", fstream::app);

    if (log_file.is_open())
      log_file << "split successfully\n";
    else
      cout << "log_file failed to open" << endl;

    upload_to_blob("log_file.txt","log/log_file.txt");

    log_file.close();

  

 for(int i = 0; i < NUM_CHUNK; i++) {

    if(i == 0 || i % 3 == 0) {
       td1[i].machineip = MACHINEONE;
    } else if(i % 3 == 2) {
       td1[i].machineip = MACHINETWO;
    } else if(i % 3 == 1) {
       td1[i].machineip = MACHINETHREE;
    }
  }

   //download_file("splitblob.5","split/splitblob.5");
  // create M clients, where M is the number of worker nodes
  // TODO mutli-threading
  // start N pthreads, each thread selects a client based on round robin, and then calls cli.startmapper();
  // wait all N pthreds to finish, and start reducers



    for(int i = 0; i < NUM_CHUNK; i++) { 

        error = pthread_create(&(tid1[i]), NULL, &startmapper, (void*) &td1[i]); 

        if (error != 0) 
            printf("\nThread can't be created :[%s]", strerror(error)); 
       
    } 
   

    for(int i = 0; i < NUM_CHUNK; i++) {

      pthread_join(tid1[i], NULL); 

    }

    log_file.close();


}




void start_remapping() {

    //check which part needs redo:

    int REMAP_SIZE = redomapping.size();
    int error;

    //chekc the remaping
    if(REMAP_SIZE != 0) {

      do {

          REMAP_SIZE = redomapping.size();

          pthread_t tid2[REMAP_SIZE]; 

          cout << "Rescheduling the failed worker tasks..." << endl;

          int index = 0;

          struct thread_data td2[REMAP_SIZE];

          while(!redomapping.empty()) {

            thread_data current = redomapping.front();
            redomapping.pop();

            if(MACHINEONEOK) {

              cout << "Rescheduling to Machine: " << MACHINEONE << endl;

              td2[index].machineip = MACHINEONE;

            } else if(MACHINETWOOK) {

              cout << "Rescheduling to Machine: " << MACHINETWO << endl;

              td2[index].machineip = MACHINETWO;

            } else {

              cout << "Rescheduling to Machine: " << MACHINETHREE << endl;

              td2[index].machineip = MACHINETHREE;

            }
            //target message
            td2[index].filename = current.filename;
            index++;

          }
         for(int i = 0; i < REMAP_SIZE; i++) { 

            error = pthread_create(&(tid2[i]), NULL, &startmapper, (void*) &td2[i]); 
            if (error != 0) 
                printf("\nThread can't be created :[%s]", strerror(error)); 
        } 
       
        for(int i = 0; i < REMAP_SIZE; i++) {
          pthread_join(tid2[i], NULL); 
        }

      } while(!redomapping.empty());

  }

}


void rescheduling() {
  //check which part needs redo:

    int REMAP_SIZE = replicating.size();
    int error;

    //chekc the remaping
    if(REMAP_SIZE != 0) {

      do {

          REMAP_SIZE = replicating.size();

          pthread_t tid2[REMAP_SIZE]; 

          cout << "Rescheduling the failed worker tasks..." << endl;

          int index = 0;

          struct thread_data td2[REMAP_SIZE];

          while(!replicating.empty()) {

            thread_data current = replicating.front();
            replicating.pop();

            td2[index].machineip = current.machineip;
            td2[index].filename = current.filename;
            index++;

          }
         for(int i = 0; i < REMAP_SIZE; i++) { 

            error = pthread_create(&(tid2[i]), NULL, &startmapper, (void*) &td2[i]); 
            if (error != 0) 
                printf("\nThread can't be created :[%s]", strerror(error)); 
        } 
       
        for(int i = 0; i < REMAP_SIZE; i++) {
          pthread_join(tid2[i], NULL); 
        }

      } while(!replicating.empty());

  }


}





int main(int argc, char** argv) {
  // upload input file to blob
  if(argc != 3)
    cout << "Usage: ./master <input_file_name> <Is this first time to run master: 1 or 0>" << endl;

  else {

    string inputfile = argv[1];
    string flag_log = argv[2];

  
    if(flag_log.compare("1") == 0) {

          split_and_map_process(inputfile);

          start_remapping();

          cout << "starting reducer" << endl;
          startreducer();

          //check reducing whether need to redo
          if(!redoreducing.empty()) {
            cout << "Rescheduling Reducing... " << endl;
            startreducer();
          }

    } else {
            download_file("log_file.txt","log/log_file.txt");
            cout << "replicating master data..." << endl;
            cout << "continue running master unfinished jobs..." << endl;
            std::this_thread::sleep_for (std::chrono::seconds(2));

            std::ifstream infile("log_file.txt");

            std::string line;
            unordered_set<int> numbers;
            std::vector<string> replicating_filenames;

            std::getline(infile, line);
            //1. check split
            if(line.compare("split successfully") != 0) {

              cout << "rodo: splitting..." << endl;

              string inputfile = argv[1];
               split_and_map_process(inputfile);
               start_remapping();
               startreducer();
              //check reducing whether need to redo
              if(!redoreducing.empty()) {
                cout << "Rescheduling Reducing... " << endl;
                startreducer();
              }


            } else {
                  //check remaining rodoing
                  cout << "checked: split file successfully..." << endl;
                   while (std::getline(infile, line,'.')) {

                    if(line.compare("RPC failed")== 0)
                      continue;

                    if(line.compare("splitblob") == 0)
                      continue;
                    if(line.compare("map") == 0)
                      continue;

                    numbers.insert(atoi(line.c_str()));

                  }

                  string input_filename;
                  //find the non-mapped file
                  for(int i = 1; i <= NUM_CHUNK; i++) {

                    if (numbers.find(i) == numbers.end()) {

                      input_filename = "split/splitblob." + to_string(i);

                      if(i % 3 == 0)
                        replicating.push({MACHINEONE, input_filename});
                      else if(i % 3 == 1)
                        replicating.push({MACHINETWO, input_filename});
                      else
                        replicating.push({MACHINETHREE, input_filename});
                    }
                }

                rescheduling();
                start_remapping();
                startreducer();
                //check reducing whether need to redo ///
                if(!redoreducing.empty()) {
                  cout << "Rescheduling Reducing... " << endl;
                  startreducer();

                }

          }

    }
   
  }
  return 0;
}