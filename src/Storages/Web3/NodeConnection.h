#pragma once
#include <functional>
#include <memory>
#include <IO/ReadBuffer.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>

namespace DB
{

struct NodeCall
{
    String method;
    std::vector<String> params;
    String id;
    String version = "2.0";

    std::string dumpToJsonString() const;
};

using NodeRequestCallback = std::function<void(String &&)>;

class INodeConnection
{
    struct Configuration
    {
        String url;
        String compression_method = "auto";
        std::vector<std::pair<String, String>> headers = {{"content-type", "application/json"}};
        ConnectionTimeouts timeouts{20, 20, 20};
        Poco::Net::HTTPBasicCredentials credentials;
        UInt8 http_max_tries = 2;
    };

public:
    INodeConnection(const std::string & url);
    virtual ~INodeConnection();
    void call(const NodeCall & request, NodeRequestCallback callback);

private:
    /**
     * Method to send a JSON-RPC request to a remote node
     * @param request NodeCall object containing method, id, version, and params for the request
     * @param callback NodeRequestCallback function to handle the response from the remote node
     * @throws Exception if the response status code is not 200 or if the response contains an error
     */
    virtual void callImplementation(const NodeCall & request, NodeRequestCallback callback) = 0;

protected:
    Configuration configuration;
};

class NodeConnection : public INodeConnection
{
public:
    NodeConnection(const std::string & url);

private:
    void callImplementation(const NodeCall & request, NodeRequestCallback callback) override;
};

using NodeConnectionPtr = std::shared_ptr<NodeConnection>;
using INodeConnectionPtr = std::shared_ptr<INodeConnection>;


}
