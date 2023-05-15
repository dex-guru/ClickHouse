//
// Created by user on 3/14/23.
//

#ifndef CLICKHOUSE_STORAGEWEB3TRANSACTIONPOLLER_H
#define CLICKHOUSE_STORAGEWEB3TRANSACTIONPOLLER_H

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/Web3/StorageWeb3BlockPollerSettings.h>
#include <Storages/Web3/Web3Source.h>
#include <Storages/Web3/Web3Client.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/Web3/BaseWeb3Storage.h>


namespace DB
{

    class TransactionSink;

    using HashQueue = ConcurrentBoundedQueue<String>;

    class StorageWeb3TransactionPoller : public BaseWeb3Storage
    {
    public:
        StorageWeb3TransactionPoller(
            const StorageID & table_id_,
            ContextPtr context_,
            const ColumnsDescription & columns_,
            std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
            bool is_attach_);

        std::string getName() const override { return "Web3Transactions"; }

        void startup() override;
        void shutdown() override;

        void pushTransactionHash(const String& hash_);

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

        Block getSampleBlockNonMaterialized() { return getOutputBlock(); }

    private:
        NamesAndTypesList output_cols; // Columns for ISource, hardcoded;

        // Blocks that we received from Web3Blockpoller
        HashQueue hashes;
        Web3ClientPtr w3;

        std::atomic<bool> mv_attached = false;

        void retrieveTransaction();
        void streamingToViewsFunc();
        Block getOutputBlock();
    };

    class TransactionSink : public SinkToStorage
    {
    public:
        TransactionSink(StorageWeb3TransactionPoller & storage_,
                        StorageMetadataPtr metadata_snapshot_,
                        size_t max_parts_per_block_,
                        ContextPtr context_);

        ~TransactionSink() override;

        String getName() const override { return "TransactionSink"; }
        void consume(Chunk chunk) override;
        void onStart() override;
        void onFinish() override;

    private:
        StorageWeb3TransactionPoller& storage;
    };
}
#endif //CLICKHOUSE_STORAGEWEB3TRANSACTIONPOLLER_H
