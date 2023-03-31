#include <Storages/Web3/Web3Client.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/hex.h>
#include <mutex>
#include <memory>

namespace DB {

		ReadBufferPtr Web3Client::consume()
		{
            if(message_queue.empty() || !message_queue.tryPop(current))
                return nullptr;
            return std::make_shared<ReadBufferFromMemory>(current.data(), current.size());
		}

        Web3Client::Web3Client(const std::string node_url, Poco::Logger* log_)
        :
            log(log_),
            connect(std::make_shared<NodeConnection>(node_url, log_)),
            message_queue(100)
        {

        }

        void Web3Client::getLastBlockNumber()
        {
            auto request = NodeCall{"eth_blockNumber", {}, "1"};
            connect->call(request, [this](String&& buffer){ receiveRequestCallback(std::move(buffer));});
        }

        void Web3Client::getBlock(uint64_t block_number)
        {
            auto block_hex = getHexUIntLowercase(block_number);
            if(block_hex[0] == '0')
            {
                for(size_t i=0; i < block_hex.size(); i++)
                {
                    if(block_hex[i] != '0')
                    {
                        block_hex = block_hex.substr(i);
                    }
                }
            }

            auto request = NodeCall{"eth_getBlockByNumber", {"0x" + block_hex, "true"}, "1"};
            connect->call(request, [this](String&& buffer){ receiveRequestCallback(std::move(buffer));});
        }

        void Web3Client::receiveRequestCallback(String&& data_string)
        {
            if(message_queue.push(std::move(data_string)))
            {

            }
        }

        String Web3Client::popRawData()
        {
            if(message_queue.empty() || !message_queue.tryPop(current))
                return {};
            return current;
        }

        size_t Web3Client::responseMessageSize() const
        {
            return message_queue.size();
        }

}
