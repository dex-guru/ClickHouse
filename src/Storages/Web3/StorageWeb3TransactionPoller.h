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

namespace DB
{
    class StorageWeb3TransactionPoller : public IStorage, WithContext
    {
    public:
        StorageWeb3TransactionPoller(
            const StorageID & table_id_,
            ContextPtr context_,
            const ColumnsDescription & columns_,
            std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
            bool is_attach_);

        std::string getName() const override { return "Web3Transactions"; }

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

    private:
        ContextMutablePtr web3transaction_context;
        std::unique_ptr<StorageWeb3BlockPollerSettings> web3_transaction_settings;
        mutable bool drop_table = false;
        const String format_name;
    [[maybe_unused]] bool is_attach;
        Poco::Logger * log;
    };
}
#endif //CLICKHOUSE_STORAGEWEB3TRANSACTIONPOLLER_H
