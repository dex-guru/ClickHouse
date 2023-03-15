#pragma once

#include <Parsers/ASTCreateQuery.h>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>



namespace DB {

//		class ASTStorage;

#define WEB3BLOCK_RELATED_SETTINGS(M) \
		M(String, node_host_port, "", "A host-port to connect to Blockchain Node.", 0) \
		M(String, message_format, "", "The message format.", 0) \
		M(String, rpc_method, "", "JRPC method.", 0) \
		M(UInt64, polling_delay, 2, "Polling Delay(seconds).", 0) \

#define LIST_OF_WEB3BLOCK_SETTINGS(M) \
		WEB3BLOCK_RELATED_SETTINGS(M) \
		FORMAT_FACTORY_SETTINGS(M)

		DECLARE_SETTINGS_TRAITS(StorageWeb3BlockPollerSettingsTraits, LIST_OF_WEB3BLOCK_SETTINGS)

		class StorageWeb3BlockPollerSettings : public BaseSettings<StorageWeb3BlockPollerSettingsTraits>
		{
		public:
				void loadFromQuery(ASTStorage & storage_def);
		};



}
