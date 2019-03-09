#ifndef SPLIT_H    // To make sure you don't declare the function more than once by including the header multiple times.
#define SPLIT_H

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

// Define the connection-string with your values.
const utility::string_t storage_connection_string(U("DefaultEndpointsProtocol=https;AccountName=yuhaostore;AccountKey=HbHTkS+eQAV2FxKOl/Ec0zRK8MqDk8Nybn0E/q7EoKJ3/NmpKSfzvavx+XywCQopwEyZ1O59tTJ1NSrnmp44vw=="));

// Retrieve storage account from connection string.
azure::storage::cloud_storage_account storage_account;
azure::storage::cloud_blob_client blob_client;
azure::storage::cloud_blob_container container;
azure::storage::cloud_block_blob blockBlob;


int upload_to_blob(string inputfile, string blobname);
int download_file(string download_file_name, string blobname);
void split_file(string fullFilePath, string chunkName, int chunk_number);
std::ifstream::pos_type filesize(string filename);


#endif
