//
// Created by user on 3/14/23.
//

#include <Storages/Web3/StorageWeb3TransactionPoller.h>
#include <Storages/StorageFactory.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/Web3/Web3Source.h>


namespace DB {

    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
        extern const int BAD_ARGUMENTS;
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int QUERY_NOT_ALLOWED;
    }

    TransactionSink::TransactionSink(
        StorageWeb3TransactionPoller & storage_,
        StorageMetadataPtr metadata_snapshot_,
        [[maybe_unused]]size_t max_parts_per_block_,
        [[maybe_unused]]ContextPtr context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
    {

    }
    /*
     * TODO: enumerate all rows if it gets more than 1 row
     * */
    void TransactionSink::consume(Chunk chunk)
    {
        auto block = getHeader().cloneWithColumns(chunk.detachColumns());
        auto hashes_column = block.getByName("transactions");
//        size_t rows = block.rows();

        const auto &array_column = static_cast<const ColumnArray &>(*hashes_column.column);
        const auto &string_column = static_cast<const ColumnString &>(*array_column.getDataPtr());

        for(size_t i =0; i < string_column.size(); i++)
        {
            auto g = string_column[i];
            String hash = g.get<String>();
            storage.pushTransactionHash(hash);
        }
    }

    void TransactionSink::onStart()
    {

    }

    void TransactionSink::onFinish()
    {

    }

    TransactionSink::~TransactionSink() {}

    StorageWeb3TransactionPoller::StorageWeb3TransactionPoller(
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
            &Poco::Logger::get("StorageWeb3Transaction (" + table_id_.table_name + ")"))
        , hashes(HashQueue(500))
        , w3(std::make_shared<Web3Client>(web3engine_settings_->node_host_port, log))
    {

        w3_context = Context::createCopy(getContext());
        w3_context->makeQueryContext();

        StorageInMemoryMetadata storage_metadata;

        NamesAndTypesList input_types;
        NamesAndTypesList output_types;

        for(auto& col : columns_.getAll())
        {
            auto name = col.getNameInStorage();
            if(name.starts_with("input_"))
            {
                String new_name = name.substr(6);
                input_types.push_back({new_name, col.getTypeInStorage()});
            }
            else
            {
                output_types.push_back({name, col.getTypeInStorage()});
            }
        }

        if(input_types.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No any `input_` types");

        if(output_types.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No any output types");

        output_columns = ColumnsDescription(std::move(output_types));

        storage_metadata.setColumns(ColumnsDescription(std::move(input_types)));
        setInMemoryMetadata(storage_metadata);
    }

    void StorageWeb3TransactionPoller::startup()
    {

    }

    void StorageWeb3TransactionPoller::shutdown()
    {

    }

    void StorageWeb3TransactionPoller::pushTransactionHash(const String& hash_)
    {
        if(!hashes.push(hash_))
        {
            LOG_ERROR(log, "Can't push Transaction's hash {}", hash_);
        }
    }

    void StorageWeb3TransactionPoller::retrieveTransaction()
    {
        if(hashes.empty())
            return;

        auto insert = std::make_shared<ASTInsertQuery>();
        auto table_id = getStorageID();
        insert->table_id = table_id;
        InterpreterInsertQuery interpreter(insert, w3_context, false, true, true);
        auto block_io = interpreter.execute();
        auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
        auto block_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

        Pipes pipes;
        std::vector<std::shared_ptr<Web3Source>> sources;

        // Customize this object
        auto column_names = block_io.pipeline.getHeader().getNames();

        while(!hashes.empty())
        {
            String hash;
            if(!hashes.pop(hash))
                break;

            w3->getTransaction(std::move(hash));

//            auto ws = std::make_shared<Web3Source>(
//                    *this,
//                    getStorageSnapshot(getInMemoryMetadataPtr(), getContext()),
//                    w3_context, column_names, max_block_size, *w3
//                );
//            sources.emplace_back(ws);
//            pipes.emplace_back(ws);
        }

        block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));
        {
            CompletedPipelineExecutor executor(block_io.pipeline);
            executor.execute();
        }
    }

    void StorageWeb3TransactionPoller::read(
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

    SinkToStoragePtr StorageWeb3TransactionPoller::write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context_)
    {
        const auto & settings = context_->getSettingsRef();
        return std::make_shared<TransactionSink>(
            *this, metadata_snapshot, settings.max_partitions_per_insert_block, context_);
    }

    void registerStorageWeb3TransactionPoller(StorageFactory & factory)
    {
        auto creator_fn = [](const StorageFactory::Arguments & args)
        {
            auto web3block_settings = std::make_unique<StorageWeb3BlockPollerSettings>();
            bool with_named_collection = getExternalDataSourceConfiguration(args.engine_args, *web3block_settings, args.getLocalContext());
            if (!with_named_collection && !args.storage_def->settings)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Web3Transaction engine must have settings");

            if (args.storage_def->settings)
                web3block_settings->loadFromQuery(*args.storage_def);

            //						if (!web3block_settings->rabbitmq_host_port.changed
            //							 && !web3block_settings->rabbitmq_address.changed)
            //										throw Exception("You must specify either `rabbitmq_host_port` or `rabbitmq_address` settings",
            //												ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!web3block_settings->message_format.changed)
                throw Exception( ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `message_format` setting");

            return std::make_shared<StorageWeb3TransactionPoller>(args.table_id, args.getContext(), args.columns, std::move(web3block_settings), args.attach);
        };

        factory.registerStorage("Web3Transaction", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
    }
}
