#ifndef WEB3CLIENT_H
#define WEB3CLIENT_H

#include <IO/EmptyReadBuffer.h>
#include <Storages/Web3/NodeConnection.h>
#include <Storages/Web3/StorageWeb3BlockPollerSettings.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <queue>

namespace DB{

		class Web3Client
		{
            using Web3MessageQueue = ConcurrentBoundedQueue<String>;

		public:
            Web3Client(const String& node_url_, Poco::Logger* log_);

            //@returns last fetched message from Web3Client;
            ReadBufferPtr consume();

            // Web3 calls
            void getLastBlockNumber();
            void getBlock(uint64_t block_number, bool include_transactions = false);
            // --------

            // Returns raw response
            String popRawData();
            size_t responseMessageSize() const;

		private:
                void receiveRequestCallback(String&& buffer);

                [[maybe_unused]]Poco::Logger* log;
        public:
                NodeConnectionPtr connect;
                Web3MessageQueue message_queue;
                String current;
		};

		using Web3ClientPtr = std::shared_ptr<Web3Client>;

}

#endif // WEB3CLIENT_H
