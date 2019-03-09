#include "split.h"

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

std::ifstream::pos_type filesize(string filename) {
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



// int main() {

//     download_file("DowloadData.txt","my-blob-1");
//     split_file("DowloadData.txt","temp",4);
//     for(int i = 1; i < 5; i++) {

//         string inputfile = "./temp." + to_string(i);

//         string blob = "split/splitblob." + to_string(i);

//         cout << inputfile << endl;
//         cout << blob << endl;

//         upload_to_blob(inputfile, blob);
//         cout << "upload " << inputfile << " to " << blob << " successfully... " << endl;

//     }

//     return 0;
// }