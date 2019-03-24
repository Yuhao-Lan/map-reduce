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

#define NUM_CHUNK 64


int upload_to_blob(string inputfile, string blobname);
int download_file(string download_file_name, string blobname);
void split_file(string fullFilePath, string chunkName, int chunk_number);
std::ifstream::pos_type filesize(string filename);


#endif
