#include "StorageWeb3BlockPoller.h"
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/Web3/Web3Source.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <ctime>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Core/Field.h>


namespace DB {

		namespace ErrorCodes
		{
				extern const int LOGICAL_ERROR;
				extern const int BAD_ARGUMENTS;
				extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
				extern const int QUERY_NOT_ALLOWED;
		}

        void Web3NumerableType::setCustomization_(DataTypeCustomDescPtr custom_desc_) const
        {
            setCustomization(std::move(custom_desc_));
        }

        template <typename T>
        void Web3Serializer<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool /*whole*/) const
        {
            T x;
            static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
            static constexpr bool is_int8 = std::is_same_v<T, Int8>;
            if (settings.json.read_bools_as_numbers || is_uint8 || is_int8)
            {
                if (!istr.eof() && *istr.position() == '"')        /// We understand the number both in quotes and without.
                {
                    ++istr.position();
                }
                readUintHexTextUnsafe(x, istr);
                ++istr.position();
            }
            else
                readText(x, istr);
            assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
        }

        template <typename T>
        void Web3Serializer<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
        {
            deserializeText(column, istr, settings, true);
        }



		static const auto RESCHEDULE_MS = 500;

		StorageWeb3BlockPoller::StorageWeb3BlockPoller(
						const StorageID & table_id_,
						ContextPtr context_,
						const ColumnsDescription & columns_,
						std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
						bool is_attach_)
				: IStorage(table_id_),
					WithContext(context_->getGlobalContext()),
					web3block_settings(std::move(web3engine_settings_)),
                    format_name(web3block_settings->message_format),
					// format_name(getContext()->getMacros()->expand(web3engine_settings_->message_format)),
					log(&Poco::Logger::get("StorageWeb3Block (" + table_id_.table_name + ")")),
					milliseconds_to_wait(RESCHEDULE_MS),
					is_attach(is_attach_),
                    w3_check_new_block(std::make_shared<Web3Client>(web3block_settings->node_host_port)),
                    w3_block_retrieve(std::make_shared<Web3Client>(web3block_settings->node_host_port)),
                    last_check_timestamp(std::time(nullptr)),
                    polling_delay(web3block_settings->polling_delay)
		{
                auto cols = columns_.getAll();
                for(auto& col : cols)
                {
                    auto type = col.getTypeInStorage();
                    if(type->isValueRepresentedByNumber())
                    {
                        // TODO: Remove this raw ptr, Provide type for Serializer
                        auto mut = reinterpret_cast<const Web3NumerableType*>(type.get());
                        auto custom = std::make_unique<DataTypeCustomDesc>(
                            std::make_unique<DataTypeCustomFixedName>(type->getName()),
                                std::make_unique<Web3Serializer<uint64_t>>());
                        mut->setCustomization_(std::move(custom));
                    }
                }
                ColumnsDescription updated_col_description(columns_);
				StorageInMemoryMetadata storage_metadata;
				storage_metadata.setColumns(updated_col_description);
				setInMemoryMetadata(storage_metadata);

                web3block_context = Context::createCopy(getContext());
                web3block_context->makeQueryContext();

				streaming_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockStreamingTask", [this]{ streamingToViewsFunc(); });
				streaming_task->deactivate();

                polling_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockPollingTask", [this]{ fetchingNewBlock(); });
                retrieveing_task = getContext()->getMessageBrokerSchedulePool().createTask("Web3BlockREtrievingTask", [this]{ retrieveNewBlock(); });
		}

		void StorageWeb3BlockPoller::read(
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

		SinkToStoragePtr StorageWeb3BlockPoller::write(
            const ASTPtr & /*query*/,
            const StorageMetadataPtr & /*metadata_snapshot*/,
            ContextPtr /*context*/)
		{
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed");
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

            // Check if at least one direct dependency is attached
            size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
            if(num_views)
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
            InterpreterInsertQuery interpreter(insert, web3block_context, false, true, true);
            auto block_io = interpreter.execute();
            auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
            auto block_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

            auto column_names = block_io.pipeline.getHeader().getNames();
            auto block_size = 10;
            auto ws = std::make_shared<Web3Source>(*this, getStorageSnapshot(getInMemoryMetadataPtr(), getContext()),
                                                   web3block_context, column_names, block_size, *w3_block_retrieve);
            Pipes pipes;

            std::vector<std::shared_ptr<Web3Source>> sources;

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
            if(current_timestamp > last_check_timestamp + polling_delay)
            {
                w3_check_new_block->getLastBlockNumber();
                last_check_timestamp = static_cast<uint64_t>(std::time(nullptr));
                if(w3_check_new_block->responseMessageSize())
                {
                    retrieveing_task->activateAndSchedule();
                }
            }
            polling_task->activateAndSchedule();
        }

        void StorageWeb3BlockPoller::retrieveNewBlock()
        {
            // TODO: add loop to get all blocks from the queue
            auto block = w3_check_new_block->popRawData();
            if(!block.empty())
            {
                auto block_number = static_cast<uint64_t>(std::stoll(block, nullptr, 16));
                if(block_number > last_block)
                {
                    retrieveing_task->activateAndSchedule();
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

//						if (!web3block_settings->rabbitmq_host_port.changed
//							 && !web3block_settings->rabbitmq_address.changed)
//										throw Exception("You must specify either `rabbitmq_host_port` or `rabbitmq_address` settings",
//												ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

						if (!web3block_settings->message_format.changed)
								throw Exception( ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `message_format` setting");

						return std::make_shared<StorageWeb3BlockPoller>(args.table_id, args.getContext(), args.columns, std::move(web3block_settings), args.attach);
				};

				factory.registerStorage("Web3Block", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
		}
}
