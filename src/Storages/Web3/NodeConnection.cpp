
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/Web3/NodeConnection.h>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/prettywriter.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;

}

std::string NodeCall::dumpToJsonString() const
{
    rapidjson::Document object;
    object.SetObject();
    rapidjson::Value method_;
    method_.SetString(method.c_str(), static_cast<rapidjson::SizeType>(method.size()), object.GetAllocator());
    rapidjson::Value id_;
    id_.SetString(id.c_str(), static_cast<rapidjson::SizeType>(id.size()), object.GetAllocator());
    rapidjson::Value version_;
    version_.SetString(version.c_str(), static_cast<rapidjson::SizeType>(version.size()), object.GetAllocator());
    rapidjson::Value params_(rapidjson::kArrayType);

    if (!params.empty())
    {
        for (const auto & i : params)
        {
            rapidjson::Value s;
            if (i[0] == 't' || i[0] == 'f')
            {
                s.SetBool(i[0] == 't');
                params_.PushBack(s, object.GetAllocator());
            }
            else
            {
                s.SetString(i.c_str(), static_cast<rapidjson::SizeType>(i.size()), object.GetAllocator());
                params_.PushBack(s, object.GetAllocator());
            }
        }
    }

    object.AddMember("id", id_, object.GetAllocator());
    object.AddMember("method", method_, object.GetAllocator());
    object.AddMember("version", version_, object.GetAllocator());
    object.AddMember("params", params_, object.GetAllocator());

    rapidjson::StringBuffer body;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(body);
    object.Accept(writer);
    return body.GetString();
}

void INodeConnection::call(const NodeCall & request, NodeRequestCallback callback)
{
    try
    {
        callImplementation(request, callback);
    }
    catch (...)
    {
        throw;
    }
}

INodeConnection::~INodeConnection()
{
}

INodeConnection::INodeConnection(const std::string & url_, Poco::Logger* log_)
    : log(log_)
{
    configuration.url = url_;
}

void NodeConnection::callImplementation(const NodeCall & request, NodeRequestCallback callback)
{
    Poco::Net::HTTPResponse res;
    Poco::URI uri(configuration.url);

    Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());

    String path(uri.getPathAndQuery());
    if (path.empty())
        path = "/";

    Poco::Net::HTTPRequest req(Poco::Net::HTTPRequest::HTTP_POST, path, Poco::Net::HTTPMessage::HTTP_1_1);
    req.setContentType("application/json");

    if (!configuration.headers.empty())
    {
        for (auto & header : configuration.headers)
        {
            req.set(header.first, header.second);
        }
    }

    auto payload = request.dumpToJsonString();

    req.setContentLength(payload.size());
    LOG_DEBUG(log, "Connecting to node: {}, method {}", configuration.url, request.method);
    std::ostream & os = session.sendRequest(req);
    os << payload; // sends the body

    if (res.getStatus() != 200)
    {
        std::string error = "Web3Client Response Error. " + res.getReason();
        throw Exception(ErrorCodes::BAD_ARGUMENTS, error.c_str());
    }

    LOG_DEBUG(log, "Received response from node: {}, method {}, size {}", configuration.url, request.method, res.size());

    rapidjson::Document response_json;
    rapidjson::IStreamWrapper isw(session.receiveResponse(res));
    response_json.ParseStream(isw);
    rapidjson::Value result;

    if (response_json.HasMember("error"))
    {
        rapidjson::Value error;
        error = response_json["error"];
        rapidjson::StringBuffer error_buffer;
        error_buffer.Clear();
        rapidjson::Writer<rapidjson::StringBuffer> writer_error(error_buffer);
        error.Accept(writer_error);
        throw Exception(ErrorCodes::LOGICAL_ERROR, error_buffer.GetString());
    }

    result = response_json["result"];

    if (result.IsString())
    {
        callback(std::move(result.GetString()));
    }
    else if (result.IsNull())
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "A node has returned an empty result");
    }
    else
    {
        rapidjson::StringBuffer result_buffer;
        result_buffer.Clear();
        rapidjson::Writer<rapidjson::StringBuffer> writer_response(result_buffer);
        result.Accept(writer_response);
        callback(std::move(result_buffer.GetString()));
    }
}

NodeConnection::NodeConnection(const std::string & url_, Poco::Logger* log_) : INodeConnection(url_, log_)
{
}

}
