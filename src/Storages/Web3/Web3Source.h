#ifndef WEB3SOURCE_H
#define WEB3SOURCE_H
#include <Processors/ISource.h>
#include <Storages/Web3/StorageWeb3BlockPoller.h>
#include <Core/Names.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/Web3/Web3Client.h>

namespace DB {

		class Web3Source : public ISource
		{
		public:
				/*
						@todo pass IWebStorageClass instead StorageWeb3BlockPoller class.
				*/
				Web3Source(
								StorageWeb3BlockPoller & storage_,
								const StorageSnapshotPtr & storage_snapshot_,
								ContextPtr context_,
								const Names & columns,
								size_t max_block_size_,
								Web3Client& client_
								);

				String getName() const override { return storage.getName(); }
				Chunk generate() override;


		private:
				StorageWeb3BlockPoller & storage;
				StorageSnapshotPtr storage_snapshot;
				ContextPtr context;
				Names column_names;
				const size_t max_block_size;

				Web3Client& client;

				Block non_virtual_header;
				Poco::Logger * log;


		};
}

#endif // WEB3SOURCE_H
