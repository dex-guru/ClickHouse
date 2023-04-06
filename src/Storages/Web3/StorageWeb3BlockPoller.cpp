#include "StorageWeb3BlockPoller.h"
#include <ctime>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/Web3/Web3Source.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int QUERY_NOT_ALLOWED;
}

StorageWeb3BlockPoller::StorageWeb3BlockPoller(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
    bool is_attach_)
    : BaseWeb3Storage(
        table_id_,
        context_,
        std::move(web3engine_settings_),
        is_attach_,
        &Poco::Logger::get("StorageWeb3Block (" + table_id_.table_name + ")"))
    , w3_check_new_block(std::make_shared<Web3Client>(web3engine_settings_->node_host_port, log))
    , w3_block_retrieve(std::make_shared<Web3Client>(web3engine_settings_->node_host_port, log))
    , last_check_timestamp(std::time(nullptr))
    , polling_delay(web3engine_settings_->polling_delay)
{
    // Set a Custom Serializer for all Number fields
    auto cols = columns_.getAll();
    for (auto & col : cols)
    {
        auto type = col.getTypeInStorage();
        if (type->isValueRepresentedByNumber())
        {
            std::reinterpret_pointer_cast<const Web3NumerableType>(type)->updateSerializer();
        }
    }
    const ColumnsDescription & updated_col_description(columns_);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(updated_col_description);
    setInMemoryMetadata(storage_metadata);

    w3_context = Context::createCopy(getContext());
    w3_context->makeQueryContext();

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockStreamingTask", [this] { streamingToViewsFunc(); });
    streaming_task->deactivate();

    polling_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockPollingTask", [this] { fetchingNewBlock(); });
    retrieving_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockRetrievingTask", [this] { retrieveNewBlock(); });
}

void StorageWeb3BlockPoller::read(
    QueryPlan & /*query_plan*/,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed");
}

SinkToStoragePtr
StorageWeb3BlockPoller::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    return std::make_shared<NullSinkToStorage>(metadata_snapshot->getSampleBlock());
}

void StorageWeb3BlockPoller::startup()
{
    polling_task->activateAndSchedule();
    streaming_task->activateAndSchedule();
}

void StorageWeb3BlockPoller::shutdown()
{
}

void StorageWeb3BlockPoller::streamingToViewsFunc()
{
    auto table_id = getStorageID();

    size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
    if (num_views)
    {
        mv_attached.store(true);
        LOG_DEBUG(log, "Started streaming to {} attached views", num_views);
        streamToViews();
        mv_attached.store(false);
    }
    streaming_task->activateAndSchedule();
}

void StorageWeb3BlockPoller::streamToViews()
{
    auto insert = std::make_shared<ASTInsertQuery>();
    auto table_id = getStorageID();
    insert->table_id = table_id;
    InterpreterInsertQuery interpreter(insert, w3_context, false, true, true);
    auto block_io = interpreter.execute();
    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    auto block_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

    auto column_names = block_io.pipeline.getHeader().getNames();
    auto ws = std::make_shared<Web3Source<StorageWeb3BlockPoller>>(
        *this,
        getStorageSnapshot(getInMemoryMetadataPtr(), getContext()),
        w3_context,
        column_names,
        max_block_size,
        *w3_block_retrieve);
    Pipes pipes;

    std::vector<std::shared_ptr<Web3Source<StorageWeb3BlockPoller>>> sources;

    sources.emplace_back(ws);
    pipes.emplace_back(ws);

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));
    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
}

void StorageWeb3BlockPoller::fetchingNewBlock()
{
    auto current_timestamp = static_cast<uint64_t>(std::time(nullptr));
    if (current_timestamp > last_check_timestamp + polling_delay)
    {
        w3_check_new_block->getLastBlockNumber();
        last_check_timestamp = static_cast<uint64_t>(std::time(nullptr));
        if (w3_check_new_block->responseMessageSize())
        {
            retrieving_task->activateAndSchedule();
        }
    }
    polling_task->activateAndSchedule();
}

void StorageWeb3BlockPoller::retrieveNewBlock()
{
    auto block = w3_check_new_block->popRawData();
    if (!block.empty())
    {
        auto block_number = static_cast<uint64_t>(std::stoll(block, nullptr, 16));
        if (block_number > last_block)
        {
            retrieving_task->activateAndSchedule();
            last_block = block_number;
            w3_block_retrieve->getBlock(last_block);
        }
    }
}

void registerStorageWeb3BlockPoller(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto web3block_settings = std::make_unique<StorageWeb3BlockPollerSettings>();
        bool with_named_collection = getExternalDataSourceConfiguration(args.engine_args, *web3block_settings, args.getLocalContext());
        if (!with_named_collection && !args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Web3Block engine must have settings");

        if (args.storage_def->settings)
            web3block_settings->loadFromQuery(*args.storage_def);

        if (!web3block_settings->message_format.changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `message_format` setting");

        return std::make_shared<StorageWeb3BlockPoller>(
            args.table_id, args.getContext(), args.columns, std::move(web3block_settings), args.attach);
    };

    factory.registerStorage(
        "Web3Block",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}
}
