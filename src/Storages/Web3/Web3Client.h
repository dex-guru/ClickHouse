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

            // Create a stand-alone config for web3
            Web3Client(const std::string node_url_);

            /*
                    @returns last fetched message from Web3Client;
            */
            ReadBufferPtr consume();


            // Web3 calls
            void getLastBlockNumber();
            void getBlock(uint64_t block_number);
            void getTransactionReceipt(String transaction_hash);
            // --------

            // Returns raw response
            String popRawData();
            size_t responseMessageSize() const;

		private:
				void callRequest();
                void receiveRequestCallback(String&& buffer);

        public:
                NodeConnectionPtr connect;
                Web3MessageQueue message_queue;
                String current;
		};

		using Web3ClientPtr = std::shared_ptr<Web3Client>;

}

#endif // WEB3CLIENT_H
