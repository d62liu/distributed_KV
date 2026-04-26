#include <grpcpp/grpcpp.h>                                                                                                                                               
#include "kv_service.grpc.pb.h" 
#include <chrono>


//be able to parse something like: dkv-cli --addr localhost:50051 put mykey myvalue
int main(int argc, char* argv[]){
    int i = 0;
    std::string port;
    std::string cmd;
    std::string val;
    std::string key;
    while (i < argc){
        std::string cur = argv[i];
        if (cur == "--addr" && i + 1 < argc) {                                                                                                     
            port = argv[i + 1];                                                                                                                    
        }
        if (cur == "put" && i + 2 < argc){
            cmd = "put";
            key = std::string(argv[i + 1]);
            val = std::string(argv[i + 2]);
        }
        if (cur == "get" && i + 1 < argc){
            cmd = "get";
            key = std::string(argv[i + 1]);
        }
        if (cur == "delete" && i + 1 < argc){
            
            cmd = "delete";
            key = std::string(argv[i + 1]);
        }
        ++i;
    }
    if (port.empty() || cmd.empty()) {                                                                                                         
      std::cerr << "usage: dkv-cli --addr <host:port> <put|get|delete> [key] [value]";
      return 1;
    }      
    auto channel = grpc::CreateChannel(port, grpc::InsecureChannelCredentials());                                                              
    auto stub = KVService::NewStub(channel); 
    grpc::ClientContext ctx;                
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));  
    if (cmd == "put") {                                                                                                                        
        PutRequest req;                     
        PutResponse resp;                                                                                                                      
        req.set_key(key);                                                                                                                      
        req.set_value(val);
        grpc::Status status = stub->Put(&ctx, req, &resp);
        if (status.ok()) std::cout << "put_OK" << std::endl;
        else std::cout << status.error_message() << std::endl;
    }else if(cmd == "get"){
        GetRequest req;                     
        GetResponse resp;                                                                                                                      
        req.set_key(key);                                                                                                                      
        grpc::Status status = stub->Get(&ctx, req, &resp);
        if (status.ok()) {
            if (resp.found()) std::cout << resp.value() << std::endl;
            else std::cout << "not found" << std::endl;
        } else std::cout << status.error_message() << std::endl;
    }else if(cmd == "delete"){
        DeleteRequest req;                     
        DeleteResponse resp;                                                                                                                      
        req.set_key(key);                                                                                                                      
        grpc::Status status = stub->Delete(&ctx, req, &resp);
        if (status.ok()) std::cout << "delete_OK" << std::endl;
        else std::cout << status.error_message() << std::endl;
    }
}