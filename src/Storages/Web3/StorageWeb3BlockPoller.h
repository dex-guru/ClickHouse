#ifndef STORAGEWEB3BLOCKPOLLER_H
#define STORAGEWEB3BLOCKPOLLER_H

#include <atomic>
#include <bitset>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/Web3/BaseWeb3Storage.h>
#include <Storages/Web3/NodeConnection.h>
#include <Storages/Web3/Web3Client.h>
#include "Interpreters/Context.h"
#include "Storages/Web3/StorageWeb3BlockPollerSettings.h"

namespace DB
{

class StorageWeb3BlockPoller : public BaseWeb3Storage
{
public:
    StorageWeb3BlockPoller(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
        bool is_attach_);

    std::string getName() const override { return "Web3Block"; }

    void startup() override;
    void shutdown() override;


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

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    uint64_t getLastBlock() const { return last_block; }

private:

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
