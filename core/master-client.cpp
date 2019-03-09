#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "rpc_generated/master-worker.grpc.pb.h"

#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/filestream.h>  
#include <cpprest/containerstream.h>
#include <iostream>
#include <string>
#include <iostream>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib> 
#include <fstream>
using namespace std;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::Filename;
using masterworker::Filenames;
using masterworker::Worker;

// Define the connection-string with your values.
const utility::string_t storage_connection_string(U("DefaultEndpointsProtocol=https;AccountName=yuhaostore;AccountKey=HbHTkS+eQAV2FxKOl/Ec0zRK8MqDk8Nybn0E/q7EoKJ3/NmpKSfzvavx+XywCQopwEyZ1O59tTJ1NSrnmp44vw=="));

// Retrieve storage account from connection string.
azure::storage::cloud_storage_account storage_account;
azure::storage::cloud_blob_client blob_client;
azure::storage::cloud_blob_container container;
azure::storage::cloud_block_blob blockBlob;

int upload_to_blob(string inputfile, string blobname) {

    

    // Retrieve storage account from connection string.
    storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

    // Create the blob client.
    blob_client = storage_account.create_cloud_blob_client();

    // Retrieve a reference to a previously created container.
    container = blob_client.get_container_reference(U("my-sample-container"));

    // Retrieve reference to a blob named "my-blob-1".
    blockBlob = container.get_block_blob_reference(U(blobname));

    // Create or overwrite the "my-blob-1" blob with contents from a local file.
    concurrency::streams::istream input_stream = concurrency::streams::file_stream<uint8_t>::open_istream(U(inputfile)).get();
    blockBlob.upload_from_stream(input_stream);
    input_stream.close().wait();

    return 0;
}

int download_file(string download_file_name, string blobname) {

    storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);
    blob_client = storage_account.create_cloud_blob_client();

    // Retrieve a reference to a previously created container.
    container = blob_client.get_container_reference(U("my-sample-container"));

    // Retrieve reference to a blob named "my-blob-1".
    blockBlob = container.get_block_blob_reference(U(blobname));

    // Save blob contents to a file.
    concurrency::streams::container_buffer<std::vector<uint8_t>> buffer;
    concurrency::streams::ostream output_stream(buffer);
    blockBlob.download_to_stream(output_stream);

    std::ofstream outfile(download_file_name, std::ofstream::binary);
    std::vector<unsigned char>& data = buffer.collection();

    outfile.write((char *)&data[0], buffer.size());
    outfile.close();

}

std::ifstream::pos_type filesize(string filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}
 
// Chunks a file by breaking it up into chunks of "chunkSize" bytes.
void split_file(string fullFilePath, string chunkName, int chunk_number) {

    unsigned long size = filesize(fullFilePath);
    unsigned long chunkSize = size/chunk_number + 1;

    ifstream fileStream;
    fileStream.open(fullFilePath, ios::in | ios::binary);

    // File open a success
    if (fileStream.is_open()) {
        ofstream output;
        int counter = 1;

        string fullChunkName;

        // Create a buffer to hold each chunk
        char *buffer = new char[chunkSize];

        // Keep reading until end of file
        while (!fileStream.eof()) {

            // Build the chunk file name. Usually drive:\\chunkName.ext.N
            // N represents the Nth chunk
            fullChunkName.clear();
            fullChunkName.append(chunkName);
            fullChunkName.append(".");

            // Convert counter integer into string and append to name.
            fullChunkName.append(to_string(counter));

            // Open new chunk file name for output
            output.open(fullChunkName.c_str(),ios::out | ios::trunc | ios::binary);

            // If chunk file opened successfully, read from input and 
            // write to output chunk. Then close.
            if (output.is_open()) { 
                fileStream.read(buffer,chunkSize);
                // gcount() returns number of bytes read from stream.
                output.write(buffer,fileStream.gcount());
                output.close();
                counter++;
            }
        }

        // Cleanup buffer
        delete[] buffer;

        // Close input file stream.
        fileStream.close();
        cout << "Chunking complete! " << counter - 1 << " files created." << endl;
    }
    else { cout << "Error opening file!" << endl; }
}


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
  MasterClient cli(grpc::CreateChannel("myVMDeployed5:50051", grpc::InsecureChannelCredentials()));
  std::string input_filename("this_is_the_file_for_map_reduce.txt");
  std::string output_filename = cli.StartMapper(input_filename);
  std::cout << "Worker received: " << output_filename << std::endl;

  return 0;
}