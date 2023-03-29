#ifndef STORAGEWEB3BLOCKPOLLER_H
#define STORAGEWEB3BLOCKPOLLER_H

#include <Core/BackgroundSchedulePool.h>
#include "Storages/Web3/StorageWeb3BlockPollerSettings.h"
#include "Interpreters/Context.h"
#include <Storages/Web3/NodeConnection.h>
#include <Storages/Web3/Web3Client.h>
#include <Storages/IStorage.h>
#include <atomic>
#include <bitset>

namespace DB {

        class Web3NumerableType : public DB::IDataType
        {
        public:
            void updateSerializer() const;
        };

        template <typename T>
        class Web3Serializer : public SerializationNumber<T>
        {
        public:
            Web3Serializer() = default;

            void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
            void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
        };

		/*
				@todo Create an interface for all web3 storages to pass it into Web3Source class
		*/
		class StorageWeb3BlockPoller : public IStorage, WithContext
		{

		public:
				StorageWeb3BlockPoller(const StorageID & table_id_,
														ContextPtr context_,
														const ColumnsDescription & columns_,
														std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
														bool is_attach_);

				std::string getName() const override { return "Web3Block"; }

				bool noPushingToViews() const override { return true; }

				const String & getFormatName() const { return format_name; }

				void startup() override;
				void shutdown() override;


				void checkTableCanBeDropped() const override { drop_table = true; }

				/// Always return virtual columns in addition to required columns
				void read(
						QueryPlan & query_plan,
						const Names & column_names,
						const StorageSnapshotPtr & storage_snapshot,
						SelectQueryInfo & query_info,
						ContextPtr context,
						QueryProcessingStage::Enum processed_stage,
						size_t max_block_size,
						size_t num_streams) override;

				SinkToStoragePtr write(
						const ASTPtr & query,
						const StorageMetadataPtr & metadata_snapshot,
						ContextPtr context) override;

				bool prefersLargeBlocks() const override { return false; }
				uint64_t getLastBlock() const { return last_block; }

		private:
				ContextMutablePtr web3block_context;
				std::unique_ptr<StorageWeb3BlockPollerSettings> web3block_settings;
				const String format_name;
				Poco::Logger * log;
		        bool is_attach;

				mutable bool drop_table = false;
				std::atomic<bool> mv_attached = false;

				BackgroundSchedulePool::TaskHolder streaming_task;
				BackgroundSchedulePool::TaskHolder polling_task;
				BackgroundSchedulePool::TaskHolder retrieving_task;

		        Web3ClientPtr w3_check_new_block;
		        Web3ClientPtr w3_block_retrieve;
                uint64_t last_check_timestamp;

		        uint64_t last_block = 0;
		        uint8_t polling_delay = 2;

				void streamToViews();
				void streamingToViewsFunc();

                // Get last block number
                void fetchingNewBlock();

                // Load a new block
                void retrieveNewBlock();
		};
}

#endif // STORAGEWEB3BLOCKPOLLER_H
