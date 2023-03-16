//
// Created by user on 3/14/23.
//

#include <Storages/Web3/StorageWeb3TransactionPoller.h>
#include <Storages/StorageFactory.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace DB {

    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
        extern const int BAD_ARGUMENTS;
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int QUERY_NOT_ALLOWED;
    }

    StorageWeb3TransactionPoller::StorageWeb3TransactionPoller(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
        bool is_attach_)
        :
            IStorage(table_id_),
            WithContext(context_->getGlobalContext()),
            web3_transaction_settings(std::move(web3engine_settings_)),
            format_name(web3_transaction_settings->message_format),
            is_attach(is_attach_),
            log(&Poco::Logger::get("StorageWeb3Transaction (" + table_id_.table_name + ")"))
    {

        web3transaction_context = Context::createCopy(getContext());
        web3transaction_context->makeQueryContext();

        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(columns_);
        setInMemoryMetadata(storage_metadata);
    }

    void StorageWeb3TransactionPoller::startup()
    {

    }

    void StorageWeb3TransactionPoller::shutdown()
    {

    }

    void StorageWeb3TransactionPoller::read(
        [[maybe_unused]]QueryPlan & query_plan,
        [[maybe_unused]]const Names & column_names,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        [[maybe_unused]]SelectQueryInfo & query_info,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/)
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed");
    }

    SinkToStoragePtr StorageWeb3TransactionPoller::write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*context*/)
    {
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed");
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

            return std::make_shared<StorageWeb3BlockPoller>(args.table_id, args.getContext(), args.columns, std::move(web3block_settings), args.attach);
        };

        factory.registerStorage("Web3Transaction", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
    }
}
