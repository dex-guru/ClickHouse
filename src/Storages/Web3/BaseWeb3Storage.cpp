//
// Created by user on 4/6/23.
//

#include "BaseWeb3Storage.h"


namespace DB
{
    BaseWeb3Storage::BaseWeb3Storage(
        const StorageID & table_id_,
        ContextPtr context_,
        std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
        bool is_attach_,
        Poco::Logger* log_
    )
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , w3_settings(std::move(web3engine_settings_))
    , format_name(web3engine_settings_->message_format)
    , log(log_)
    , is_attach(is_attach_)
    {

    }

}
