#include <Storages/Web3/Web3Source.h>
#include <IO/EmptyReadBuffer.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/logger_useful.h>

namespace DB {

		Web3Source::Web3Source(
						StorageWeb3BlockPoller & storage_,
						const StorageSnapshotPtr & storage_snapshot_,
						ContextPtr context_,
						const Names & columns,
						size_t max_block_size_,
						Web3Client& client_
						)
				: ISource(storage_snapshot_->metadata->getSampleBlockNonMaterialized())
				, storage(storage_)
				,	storage_snapshot(storage_snapshot_)
				,	context(context_)
				,	column_names(columns)
				,	max_block_size(max_block_size_)
				, client(client_)
				, non_virtual_header(storage_snapshot_->metadata->getSampleBlockNonMaterialized())
				, log(&Poco::Logger::get("Web3Source"))
		{

		}

		Chunk Web3Source::generate()
		{
				EmptyReadBuffer empty_buf;
				auto input_format = FormatFactory::instance().getInputFormat(
								storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size);

				StreamingFormatExecutor executor(non_virtual_header, input_format);

				size_t total_rows = 0;

				while(true)
				{
                    size_t new_rows = 0;
                    if(auto buf = client.consume())
                    {
                        new_rows = executor.execute(*buf);
                        total_rows += new_rows;
                        if (total_rows >= max_block_size)
                            break;
                    }
                    else
                    {
                        break;
                    }
				}

				if (total_rows == 0)
						return {};

				auto result_columns  = executor.getResultColumns();

				return Chunk(std::move(result_columns), total_rows);
		}

}
