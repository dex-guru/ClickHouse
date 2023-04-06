//
// Created by user on 4/6/23.
//

#include "BaseWeb3Storage.h"
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Core/Field.h>
#include <Core/Types.h>


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


    void Web3NumerableType::updateSerializer() const
    {
        SerializationPtr serializer;
        auto type_name = std::make_unique<DataTypeCustomFixedName>(getName());
        auto type_id = getTypeId();
        switch (type_id)
        {
            case TypeIndex::UInt8:
                serializer = std::make_unique<Web3Serializer<UInt8>>();
                break;
            case TypeIndex::UInt16:
                serializer = std::make_unique<Web3Serializer<UInt16>>();
                break;
            case TypeIndex::UInt32:
                serializer = std::make_unique<Web3Serializer<UInt32>>();
                break;
            case TypeIndex::UInt64:
                serializer = std::make_unique<Web3Serializer<UInt64>>();
                break;
            case TypeIndex::UInt128:
                serializer = std::make_unique<Web3Serializer<UInt128>>();
                break;
            case TypeIndex::UInt256:
                serializer = std::make_unique<Web3Serializer<UInt256>>();
                break;
            case TypeIndex::Int8:
                serializer = std::make_unique<Web3Serializer<Int8>>();
                break;
            case TypeIndex::Int16:
                serializer = std::make_unique<Web3Serializer<Int16>>();
                break;
            case TypeIndex::Int32:
                serializer = std::make_unique<Web3Serializer<Int32>>();
                break;
            case TypeIndex::Int64:
                serializer = std::make_unique<Web3Serializer<Int64>>();
                break;
            case TypeIndex::Int128:
                serializer = std::make_unique<Web3Serializer<Int128>>();
                break;
            case TypeIndex::Int256:
                serializer = std::make_unique<Web3Serializer<Int256>>();
                break;
            default:
                throw std::logic_error("Wrong type for field " + type_name->getName());
        }
        auto customizer = std::make_unique<DataTypeCustomDesc>(std::move(type_name), serializer);
        setCustomization(std::move(customizer));
    }

    template <typename T>
    void Web3Serializer<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool /*whole*/) const
    {
        T x;
        static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
        static constexpr bool is_int8 = std::is_same_v<T, Int8>;
        if (settings.json.read_bools_as_numbers || is_uint8 || is_int8)
        {
            if (!istr.eof() && *istr.position() == '"') /// We understand the number both in quotes and without.
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

}
