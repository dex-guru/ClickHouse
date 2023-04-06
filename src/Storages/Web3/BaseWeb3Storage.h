//
// Created by user on 4/6/23.
//

#ifndef CLICKHOUSE_BASEWEB3STORAGE_H
#define CLICKHOUSE_BASEWEB3STORAGE_H

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/Web3/StorageWeb3BlockPollerSettings.h>

namespace DB
{

class Web3NumerableType : public DB::IDataType
{
public:
    void updateSerializer() const;
};

template <typename T>
class Web3Serializer : public SerializationNumber<T>
{
public:
    Web3Serializer() = default;

    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
};

class BaseWeb3Storage : public IStorage, public WithContext
{
public:
    BaseWeb3Storage(
        const StorageID & table_id_,
        ContextPtr context_,
        std::unique_ptr<StorageWeb3BlockPollerSettings> web3engine_settings_,
        bool is_attach_,
        Poco::Logger* log_
    );

    const String & getFormatName() const { return format_name; }

    bool noPushingToViews() const override { return false; }

    void checkTableCanBeDropped() const override { drop_table = true; }

    bool prefersLargeBlocks() const override { return false; }

protected:
    ContextMutablePtr w3_context;
    std::unique_ptr<StorageWeb3BlockPollerSettings> w3_settings;
    const String format_name;
    Poco::Logger * log;
    bool is_attach;

    mutable bool drop_table = false;
    size_t max_block_size = 8192;
};

}


#endif //CLICKHOUSE_BASEWEB3STORAGE_H
